import traceback
from collections.abc import Callable
from typing import Any

from lagrange.client.client import Client

from euleronebot import config as _config
from euleronebot.hyperogger import Logger

_push_hex_dump_patched = False
_qface_decode_patched = False
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


def patch_qface_decode() -> None:
    """
    修复新版 QQ 表情(如 [哭][偷感][复兴号] 等小黄脸新表情)接收时被静默丢弃的问题。

    根因:新版表情在消息中以 common_elem 承载(service_type=33 普通新表情 /
    37 动画大表情, business_type=1),而 hiro-qq 的 decoder.parse_msg_new
    的 common_elem 分支未处理 33/37 两个 service_type,元素被静默跳过,
    导致整条消息事件缺失该表情(与旧表情混发时旧表情正常、新表情消失)。

    补丁:包装 parse_msg_new,在调用前预处理消息包——把 33/37 的
    common_elem 转写为 face 元素(faceId 取自 QSmallFaceExtra{1} /
    QBigFaceExtra{3},与 NapCat/QQNT 行为一致),使原函数走已有 face
    分支产出 Emoji,表情不再丢失。
    """
    global _qface_decode_patched
    if _qface_decode_patched:
        return

    from lagrange.client.message import decoder as msg_decoder
    from lagrange.pb.message.rich_text import elems as elems_pb

    original_parse_msg_new = msg_decoder.parse_msg_new

    def _rewrite_qface(content: Any) -> None:
        for el in content:
            common = getattr(el, "common_elem", None)
            if common is None:
                continue
            service_type = getattr(common, "service_type", None)
            if service_type not in (33, 37):
                # 未覆盖的 common_elem:输出明细以便发现新形态(低频,不会刷屏)
                logger.warning(
                    f"未处理的 common_elem: service_type={service_type!r} "
                    f"bus_type={getattr(common, 'bus_type', None)!r} "
                    f"pb_elem={getattr(common, 'pb_elem', None)!r}"
                )
                continue
            # pb_elem 在 hiro-qq 里声明为 dict:ProtoStruct 解码时已经把字节
            # 解成了 {1: faceId, 2: preview, ...},faceId 在字段 1
            # (实测接收推送里 33/37 的 faceId 均在 f1;NapCat 发送结构的
            #  QBigFaceExtra 才把 faceId 放 f3)。
            # (切勿 bytes(pb_elem):bytes(dict) 只取键值域 0-255 的键,会得到
            #  b"\x01\x02\x03" 之类的垃圾——曾导致表情解析全部失败的 bug)
            pb_elem = getattr(common, "pb_elem", None)
            if not isinstance(pb_elem, dict) or not pb_elem:
                logger.warning(f"common_elem(33/37) pb_elem 非法: {pb_elem!r}")
                continue
            face_id = pb_elem.get(1)
            if isinstance(pb_elem.get(3), int) and face_id is None:
                face_id = pb_elem.get(3)  # 兜底:若某实现沿用发送结构(faceId=f3)
            if not isinstance(face_id, int) or face_id <= 0:
                logger.warning(f"faceId 异常: {face_id!r} (pb_elem={pb_elem!r})")
                continue
            logger.info(f"新版表情命中: service_type={common.service_type} faceId={face_id}")
            el.face = elems_pb.Face(index=face_id)
            el.common_elem = None

    async def parse_msg_new_with_qface(
        client: Any, pkg: Any, fri_id: str | None = None, grp_id: int | None = None
    ) -> Any:
        try:
            content = pkg.message.body.content
            _rewrite_qface(content)
        except AttributeError:
            pass
        result = await original_parse_msg_new(client, pkg, fri_id=fri_id, grp_id=grp_id)
        # 调试魔法口令: 消息含 JUST_FOR____DUMP____ 文本段时,HEX 转储当前推送包体。
        # parse_msg_new 拿不到 SSOPacket;由 push_handler 层包装器把最近一次推送
        # 的 sso 写入 _current_sso(push 循环为单协程顺序处理,无竞争)。
        try:
            text = "".join(
                (el.text.string or "") for el in pkg.message.body.content if getattr(el, "text", None) is not None
            )
        except Exception:
            text = ""
        if text.startswith("JUST_FOR____DUMP____") and _current_sso is not None:
            try:
                data = bytes(_current_sso.data)
                logger.error(
                    f"取证转储: data({len(data)} bytes) HEX:\n{_hex_dump(data)}"
                )
            except Exception as dump_exc:
                logger.error(f"取证转储失败: {dump_exc!r}")
        return result

    msg_decoder.parse_msg_new = parse_msg_new_with_qface
    logger.info("已启用新版 QQ 表情解析补丁")
    _qface_decode_patched = True


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
        patch_qface_decode,
    ]
    for pt in patches:
        logger.trace(f"Applying patch for {pt.__name__}")
        pt()
