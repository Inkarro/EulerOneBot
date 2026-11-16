import asyncio
import traceback
from collections.abc import Callable
from typing import Any, Never

from lagrange.client.base import BaseClient
from lagrange.client.client import Client

from euleronebot import config as _config
from euleronebot.hyperogger import Logger

_HEARTBEAT_TIMEOUT = 10
_heartbeat_timeout_patched = False
_push_hex_dump_patched = False
_src_msg_decode_patched = False
logger = Logger.fetch("euler").name_custom("euler.patches")


def patch_lagrange_heartbeat_timeout() -> None:
    """
    This patch is here due to an unsure bug that lagrange event emitting might be dead unexpectedly without any logs.
    """
    global _heartbeat_timeout_patched
    if _heartbeat_timeout_patched:
        return

    original_sso_heartbeat = BaseClient.sso_heartbeat

    async def sso_heartbeat_with_total_timeout(self: BaseClient, *args: Any, **kwargs: Any) -> float:
        try:
            return await asyncio.wait_for(
                original_sso_heartbeat(self, *args, **kwargs),
                timeout=_HEARTBEAT_TIMEOUT,
            )
        except TimeoutError:
            raise
        except asyncio.CancelledError as exc:
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise
            raise TimeoutError("Lagrange heartbeat response cancelled") from exc
        except Exception as exc:
            logger.error(f"Lagrange heartbeat failed: {exc!r}")
            logger.trace(traceback.format_exc())
            raise TimeoutError("Lagrange heartbeat unexpected failure") from exc

    BaseClient.sso_heartbeat = sso_heartbeat_with_total_timeout
    _heartbeat_timeout_patched = True


def patch_lagrange_heartbeat_recovery() -> None:
    """
    Lagrange only recovers from asyncio.TimeoutError here. Any other exception kills
    the heartbeat task silently while the TCP connection may remain half-open.
    """

    async def resilient_heartbeat_task(self: BaseClient) -> Never:
        error_count = 0
        while True:
            await self.online.wait()
            if not error_count:
                await asyncio.sleep(self._heartbeat_interval)
            try:
                latency = await self.sso_heartbeat(True, 5)
                logger.trace(f"Lagrange heartbeat {latency * 1000:.2f}ms")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error_count += 1
                if error_count < 3:
                    logger.warning(f"Lagrange heartbeat failed ({error_count}/3): {exc!r}")
                    continue
                logger.error(f"Lagrange heartbeat failed {error_count} times, reconnecting: {exc!r}")
                logger.trace(traceback.format_exc())
                self.destroy_network()
                await asyncio.sleep(_HEARTBEAT_TIMEOUT)
                continue
            error_count = 0

    BaseClient._heartbeat_task = resilient_heartbeat_task


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
    便于定位协议解码错误(如 src_msg 快照含非 UTF-8 字节)。
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


def patch_src_msg_typed() -> None:
    """
    从根源修复"回复(引用)"消息预览文本损坏/协议层崩溃的问题。

    根因:hiro-qq 把 SrcMsg.elems(引用快照的原文元素)声明为 list[dict],
    dict 字段经 proto_decode 无限层递归试探解析。纯文本(如 b"@Sand Stone")
    的字节恰好能构成合法 protobuf 时,bytes 被静默替换成 dict;decoder.py
    随后 proto_encode(...).decode() 重组出的字节流破坏 UTF-8 边界,
    轻则 Quote 预览乱码/丢段,重则 UnicodeDecodeError 使整条消息事件丢失。

    补丁:把 SrcMsg.elems 替换为 typed 定义(list[SrcElem],
    SrcElem.arg: SrcElemArg, SrcElemArg.text: bytes)。ProtoStruct.decode
    走 proto_decode(data, 0)——不递归试探,text 字段原样保留字节,
    @Sand Stone 之类的文本一字不差。

    兼容性:decoder.py 对元素按 dict 方式访问(v.get(1, {1: b""})[1]),
    故 SrcElem/SrcElemArg 提供 get/__getitem__ 模拟 dict 接口,使原解析
    代码零改动即拿到原始文本字节。
    """
    global _src_msg_decode_patched
    if _src_msg_decode_patched:
        return

    from lagrange.pb.message import rich_text as rt
    from lagrange.pb.message.rich_text import elems as elems_pb
    from lagrange.utils.binary.protobuf import ProtoStruct, proto_field

    class SrcElemArg(ProtoStruct):
        text: bytes = proto_field(1)

        def __getitem__(self, key: int) -> bytes:
            if key == 1:
                return self.text
            raise KeyError(key)

        def get(self, key: int, default: Any = None) -> Any:
            try:
                return self[key]
            except KeyError:
                return default

    class SrcElem(ProtoStruct):
        arg: SrcElemArg | None = proto_field(1, default=None)

        def get(self, key: int, default: Any = None) -> Any:
            if key == 1:
                return self.arg if self.arg is not None else default
            return default

    class SrcMsgTyped(ProtoStruct):
        seq: int = proto_field(1)
        uin: int = proto_field(2, default=0)
        timestamp: int = proto_field(3)
        elems: list[SrcElem] = proto_field(5, default_factory=list)
        pb_reserved: elems_pb.SrcMsgArgs | None = proto_field(8, default=None)
        to_uin: int = proto_field(10, default=0)

    # Elems 类定义在 rich_text 包的 __init__(不是 elems 子模块),且类定义时
    # 已把 src_msg 字段的类型绑定到旧 SrcMsg(存于 __proto_fields__),
    # 仅替换模块属性无效,必须直接改字段类型引用。
    elems_pb.SrcMsg = SrcMsgTyped
    rt.Elems.__proto_fields__["src_msg"].type = SrcMsgTyped | None

    logger.info("已启用 SrcMsg typed 解析补丁")
    _src_msg_decode_patched = True


def apply_patches() -> None:
    patches: list[Callable[[], None]] = [
        patch_lagrange_heartbeat_timeout,
        patch_lagrange_heartbeat_recovery,
        patch_push_hex_dump,
        patch_src_msg_typed,
    ]
    for pt in patches:
        logger.trace(f"Applying patch for {pt.__name__}")
        pt()
