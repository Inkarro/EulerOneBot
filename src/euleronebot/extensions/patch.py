from collections.abc import Callable
from typing import Any

from lagrange.client.client import Client

from euleronebot import config as _config
from euleronebot.hyperogger import Logger

_push_hex_dump_patched = False
_qface_decode_patched = False
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
    服务器推送包处理抛异常时,把解密后的完整包体以 HEX 转储,
    便于定位协议解码错误。
    仅在 log_level 为 DEBUG/TRACE 时安装,避免正常运行的额外开销。
    """
    global _push_hex_dump_patched
    if _push_hex_dump_patched:
        return

    # 动态读取模块属性:apply_patches() 在 setup() 之后才运行,
    # 不能用 from-import(它绑定的是导入时的 None)
    cfg = _config.loaded_config
    if cfg is None or cfg.log_level not in ("DEBUG", "TRACE"):
        return

    original_push_handler = Client.push_handler

    async def push_handler_with_hex_dump(self: Client, sso: Any) -> None:
        try:
            await original_push_handler(self, sso)
        except Exception:
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
    logger.info("已启用推送包 HEX 转储(log_level=DEBUG/TRACE)")
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
    from lagrange.utils.binary.protobuf import proto_decode

    original_parse_msg_new = msg_decoder.parse_msg_new

    def _rewrite_qface(content: Any) -> None:
        for el in content:
            common = getattr(el, "common_elem", None)
            if common is None or getattr(common, "service_type", None) not in (33, 37):
                continue
            pb_elem = getattr(common, "pb_elem", None)
            if not pb_elem:
                continue
            try:
                if common.service_type == 33:
                    face_id = proto_decode(bytes(pb_elem)).get(1)
                else:
                    face_id = proto_decode(bytes(pb_elem)).get(3)
            except Exception:
                logger.trace(traceback.format_exc())
                continue
            if not isinstance(face_id, int) or face_id <= 0:
                continue
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
        return await original_parse_msg_new(client, pkg, fri_id=fri_id, grp_id=grp_id)

    msg_decoder.parse_msg_new = parse_msg_new_with_qface
    logger.info("已启用新版 QQ 表情解析补丁")
    _qface_decode_patched = True


def apply_patches() -> None:
    patches: list[Callable[[], None]] = [
        patch_push_hex_dump,
        patch_qface_decode,
    ]
    for pt in patches:
        logger.trace(f"Applying patch for {pt.__name__}")
        pt()
