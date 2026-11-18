from collections.abc import Callable
from typing import Any

from lagrange.client.client import Client

from euleronebot import config as _config
from euleronebot.hyperogger import Logger

_push_hex_dump_patched = False
_dump_keyword_patched = False
_sign_blacklist_patched = False
_current_sso: Any = None  # 最近一次推送的 SSOPacket,供取证转储读取
logger = Logger.fetch("euler").name_custom("euler.patches")


def _hex_dump(data: bytes, width: int = 16) -> str:
    lines = []
    for off in range(0, len(data), width):
        chunk = data[off : off + width]
        hex_part = " ".join(f"{b:02x}" for b in chunk)
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        lines.append(f"{off:08x}  {hex_part:<{width * 3 - 1}}  |{ascii_part}|")
    return "\n".join(lines)


def patch_push_hex_dump() -> None:
    """
    包装 push_handler:
    1. 记录最近一次推送的 SSOPacket,供取证口令(JUST_FOR____DUMP____)读取;
    2. 处理抛异常时,若 log_level 为 DEBUG/TRACE,输出解密包体的 HEX 转储。
    取证口令不依赖 log_level,随时可用。
    """
    global _push_hex_dump_patched
    if _push_hex_dump_patched:
        return

    # 动态读取模块属性:apply_patches() 在 setup() 之后才运行,
    # 不能用 from-import(它绑定的是导入时的 None)
    cfg = _config.loaded_config
    dump_on_error = bool(cfg and cfg.log_level in ("DEBUG", "TRACE"))

    original_push_handler = Client.push_handler

    async def push_handler_with_hex_dump(self: Client, sso: Any) -> None:
        global _current_sso
        _current_sso = sso
        try:
            await original_push_handler(self, sso)
        except Exception:
            if dump_on_error:
                try:
                    data = bytes(sso.data)
                    logger.error(
                        f"推送包处理失败,cmd={sso.cmd!r} seq={sso.seq!r} "
                        f"data({len(data)} bytes) HEX:\n{_hex_dump(data)}"
                    )
                except Exception as dump_exc:
                    logger.error(f"推送包 HEX 转储失败: {dump_exc!r}")
            raise

    Client.push_handler = push_handler_with_hex_dump
    logger.info(
        "已启用推送包捕获"
        + ("(含异常 HEX 转储)" if dump_on_error else "(仅取证口令,异常转储需 DEBUG/TRACE)")
    )
    _push_hex_dump_patched = True


def patch_dump_keyword() -> None:
    """
    调试取证:消息含 JUST_FOR____DUMP____ 文本段时,HEX 转储该推送的
    解密包体。parse_msg_new 拿不到 SSOPacket;由 push_handler 层包装器
    (patch_push_hex_dump)把最近一次推送的 sso 写入 _current_sso
    (push 循环为单协程顺序处理,无竞争)。不依赖 log_level。
    """
    global _dump_keyword_patched
    if _dump_keyword_patched:
        return

    from lagrange.client.message import decoder as msg_decoder

    original_parse_msg_new = msg_decoder.parse_msg_new

    async def parse_msg_new_with_dump(
        client: Any, pkg: Any, fri_id: str | None = None, grp_id: int | None = None
    ) -> Any:
        result = await original_parse_msg_new(client, pkg, fri_id=fri_id, grp_id=grp_id)
        try:
            text = "".join(
                (el.text.string or "") for el in pkg.message.body.content if getattr(el, "text", None) is not None
            )
        except Exception:
            text = ""
        if text.startswith("JUST_FOR____DUMP____") and _current_sso is not None:
            try:
                data = bytes(_current_sso.data)
                logger.error(f"取证转储: data({len(data)} bytes) HEX:\n{_hex_dump(data)}")
            except Exception as dump_exc:
                logger.error(f"取证转储失败: {dump_exc!r}")
        return result

    msg_decoder.parse_msg_new = parse_msg_new_with_dump
    logger.info("已启用取证口令(JUST_FOR____DUMP____)")
    _dump_keyword_patched = True


def patch_sign_blacklist() -> None:
    """
    修复上游兼容安卓协议时连带搞烂 linux 协议签名的问题。

    该改动给 SIGN_PKG_LIST 新增了 OidbSvcTrpcTcp.0xfe1_2 / 0xfe1_8 /
    0xe37_1700 三个命令(安卓端签名需要),但常见签名服务器并不支持
    它们——服务器返回 403 {"code":-1,"message":"Unsupported sign
    command"},而 get_user_info 是高频调用,于是每条消息都触发 3 轮
    签名重试后抛 ConnectionError,事件处理跟着失败,签名报错刷屏。
    Linux 协议下这些命令此前一直无需签名,剔除即可恢复原状。

    若签名服务器已支持这些命令,本补丁无副作用(get_sign 对不在
    名单的命令直接返回空签名)。
    """
    global _sign_blacklist_patched
    if _sign_blacklist_patched:
        return

    from lagrange.utils import sign as sign_mod

    _UNSUPPORTED = {
        "OidbSvcTrpcTcp.0xfe1_2",
        "OidbSvcTrpcTcp.0xfe1_8",
        "OidbSvcTrpcTcp.0xe37_1700",
    }
    removed = [cmd for cmd in sign_mod.SIGN_PKG_LIST if cmd in _UNSUPPORTED]
    if not removed:
        logger.info("签名白名单已不含目标命令,跳过补丁")
        return
    sign_mod.SIGN_PKG_LIST[:] = [cmd for cmd in sign_mod.SIGN_PKG_LIST if cmd not in _UNSUPPORTED]
    logger.info(f"已从签名白名单剔除 {removed}")
    _sign_blacklist_patched = True


def apply_patches() -> None:
    patches: list[Callable[[], None]] = [
        patch_sign_blacklist,
        patch_push_hex_dump,
        patch_dump_keyword,
    ]
    for pt in patches:
        logger.trace(f"Applying patch for {pt.__name__}")
        pt()
