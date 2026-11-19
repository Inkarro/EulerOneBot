import asyncio
import base64
import io
import os
import time
import traceback
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Literal,
    NoReturn,
    Protocol,
    cast,
    runtime_checkable,
)
from urllib.parse import unquote, urlparse

import httpx
from lagrange import Lagrange
from lagrange.client.message.elems import ForwardNode, MulitMsg
from lagrange.client.message.types import Element
from lagrange.pb.service.group import GetGrpMemberInfoRsp, PBGetGrpMemberInfoReq
from lagrange.utils.binary.protobuf import proto_encode

from ..hyperogger import Logger
from ..onebot import Adapter as OneBotAdapter
from ..onebot import events as onebot_events
from ..onebot.api import *
from ..onebot.api_data import *
from ..onebot.models import TargetInfo
from ..onebot.segments import Node
from ..utils import with_retry
from ..utils.infomgr import MsgInfo, info_mgr
from .info_cache import GROUP_LIST_KEY, InfoCache
from ..utils.transformer import to_lagrange_msg, to_onebot_msg

if TYPE_CHECKING:
    from . import LagrangeProtocol
else:
    LagrangeProtocol = Any

logger = Logger.fetch("euler").name_custom("euler.protocol.impl")
APICallHandler = Callable[..., Coroutine[Any, Any, Any]]


def _process_memory_bytes() -> int:
    """返回当前进程常驻内存字节数;非 Linux 平台无法读取时返回 0。"""
    try:
        with open("/proc/self/statm", encoding="utf-8") as f:
            resident_pages = int(f.read().split()[1])
        return resident_pages * os.sysconf("SC_PAGE_SIZE")
    except (FileNotFoundError, IndexError, OSError, ValueError):
        return 0


@runtime_checkable
class RegisteredHandler(Protocol):
    call_type: type[BaseAPICall]


def on(call_type: type[BaseAPICall]) -> Callable[[APICallHandler], APICallHandler]:
    def dec(func: APICallHandler) -> APICallHandler:
        cast(RegisteredHandler, cast(object, func)).call_type = call_type
        return func

    return dec


# 群打卡请求里带的客户端版本号（取自 NapCat 的实现）
_GROUP_SIGN_VERSION = "9.0.90"
# 不排队、并发处理的接口（其余接口仍然一个接一个处理）
# 缓存的成员表里找不到某个成员时，成员表至少要缓存了这么多秒才会去刷新（防止反复重拉整个成员表）
MEMBER_REFRESH_MIN_AGE = 30.0
# 缓存的群列表里找不到某个群时，群列表至少缓存了这么多秒才会刷新（群列表只要一次请求，间隔可以短一些）
GROUP_REFRESH_MIN_AGE = 5.0
# 一页成员请求被服务器拒绝（限流）时，最多尝试几次；每次失败后先等 BACKOFF * 2^n 秒
MEMBER_PAGE_ATTEMPTS = 4
MEMBER_PAGE_BACKOFF = 0.5
# 给消息事件取发送者信息最多等几秒，超时就降级，不能让消息被卡住
SENDER_INFO_TIMEOUT = 3.0
# 启动预热时，两个群之间再多等的秒数（在限速器的间隔之外）
PREWARM_GROUP_GAP = 0.2
# 第一轮预热里失败的群（多半是被限流了），等这么多秒之后再补拉一轮
PREWARM_RETRY_DELAY = 5.0
# 这些接口自带缓存、限速和退避，失败时不要再立即连续重试 5 次（那样会把限流越推越深）
_SINGLE_ATTEMPT_ACTIONS = frozenset(
    {
        "get_group_list",
        "get_group_info",
        "get_group_member_list",
        "get_group_member_info",
        "get_stranger_info",
        "get_login_info",
        "get_friend_list",
    }
)
_CONCURRENT_ACTIONS = frozenset({"set_group_sign", "send_group_sign"})


class LagrangeImpl:
    def __init__(self, onebot_adapter: OneBotAdapter, lag: Lagrange, protocol: LagrangeProtocol):
        self.adapter = onebot_adapter
        self.lag = lag
        self.protocol = protocol
        self.subscriptions: dict[str, APICallHandler] = {}
        self._concurrent_tasks: set[asyncio.Task] = set()
        self.cache = InfoCache()
        self._uid_added = 0  # 从成员表里新学到的 uid <-> QQ 号映射数，给启动日志用

    def subscribe(self) -> None:
        for i in dir(self):
            attr = getattr(self, i)
            func = getattr(attr, "__func__", attr)
            if isinstance(func, RegisteredHandler):
                self.subscriptions[func.call_type.model_fields["action"].default] = attr

    @staticmethod
    async def _open_upload_file(file: str, name: str | None = None) -> tuple[BinaryIO, str]:
        """解析 Lagrange.OneBot 风格的上传文件字段,返回可读流和文件名。"""
        if file.startswith("base64://"):
            data = base64.b64decode(file.removeprefix("base64://"))
            return io.BytesIO(data), name or "file"

        parsed = urlparse(file)
        if parsed.scheme in ("http", "https"):
            async with httpx.AsyncClient() as cli:
                rsp = await cli.get(parsed.geturl())
                rsp.raise_for_status()
                data = rsp.content
            filename = name or unquote(Path(parsed.path).name) or "file"
            return io.BytesIO(data), filename

        path = unquote(parsed.path) if parsed.scheme == "file" else file
        filename = name or Path(path).name
        return open(path, "rb"), filename

    async def _handle_call(self, call) -> None:
        try:
            if handler := self.subscriptions.get(call.action, None):
                rsp = await with_retry(
                    lambda c=call: handler(c.params), maximum=1 if call.action in _SINGLE_ATTEMPT_ACTIONS else 5
                )
                rsp.echo = call.echo
            else:
                rsp = ActionFailedResponse(status="failed", retcode=1404, data=None, echo=call.echo)
            await self.adapter.report(rsp)
        except Exception as e:  # noinspection PyBroadException
            logger.error(repr(e))
            logger.trace(traceback.format_exc())
            rsp = ActionFailedResponse(status="failed", retcode=1400, data=None, echo=call.echo)
            try:
                await self.adapter.report(rsp)
            except Exception:
                logger.error("error in handler, can't deliver")

    async def api_service(self) -> NoReturn:
        while True:
            call = await self.adapter.api_calls.get()
            if call.action in _CONCURRENT_ACTIONS:
                # 群打卡需要按固定间隔把请求发出去，不能排在前一个请求后面等它的回包
                task = asyncio.create_task(self._handle_call(call))
                self._concurrent_tasks.add(task)
                task.add_done_callback(self._concurrent_tasks.discard)
            else:
                await self._handle_call(call)

    @on(SendMessage)
    async def send_message(self, data: SendMsgData) -> SendMessageResponse:
        if data.group_id:
            return await self.send_group_message(SendGroupMsgData(group_id=data.group_id, message=data.message))
        else:
            return await self.send_private_message(SendPrivateMsgData(user_id=data.user_id, message=data.message))

    @on(SendPrivateMessage)
    async def send_private_message(self, data: SendPrivateMsgData) -> SendMessageResponse:
        new_msg = await to_lagrange_msg(
            msg=data.message,
            lgrc=self.lag.client,
            target=(TargetInfo(target="user", id=data.user_id)),
        )
        uid = await info_mgr.uid_mgr.from_uin(data.user_id)
        if len(new_msg) == 1 and isinstance(new_msg[0], MulitMsg):
            seq = await self.lag.client.send_friend_forward_msg(new_msg[0], uid)
        else:
            new_msg = cast(list[Element], new_msg)
            seq = await self.lag.client.send_friend_msg(uid=uid, msg_chain=new_msg)
        text = ""
        for i in new_msg:
            text += i.display
        msgid = await info_mgr.msgid_mgr.add(
            MsgInfo(
                raw_msg=new_msg,
                scene_type="user",
                scene_id=data.user_id,
                seq=seq,
                timestamp=round(time.time()),
                uid=self.lag.client.uid,
                uin=self.lag.client.uin,
                text=text,
            )
        )
        return SendMessageResponse(status="ok", retcode=0, data=SendMsgRsp(message_id=msgid))

    @on(SendGroupMessage)
    async def send_group_message(self, data: SendGroupMsgData) -> SendMessageResponse:
        new_msg = await to_lagrange_msg(
            msg=data.message,
            lgrc=self.lag.client,
            target=TargetInfo(target="group", id=data.group_id),
        )
        if len(new_msg) == 1 and isinstance(new_msg[0], MulitMsg):
            seq = await self.lag.client.send_grp_forward_msg(new_msg[0], data.group_id)  # type: ignore
        else:
            new_msg = cast(list[Element], new_msg)
            seq = await self.lag.client.send_grp_msg(grp_id=data.group_id, msg_chain=new_msg)
        try:
            rand = (
                await self.lag.client.get_grp_msg(grp_id=data.group_id, start=seq, end=seq, filter_deleted_msg=False)
            )[0].rand
        except (AttributeError, IndexError, KeyError):
            rand = 0
        text = ""
        for i in new_msg:
            text += i.display
        msgid = await info_mgr.msgid_mgr.add(
            MsgInfo(
                raw_msg=new_msg,
                scene_type="group",
                scene_id=data.group_id,
                seq=seq,
                timestamp=round(time.time()),
                uid=self.lag.client.uid,
                uin=self.lag.client.uin,
                text=text,
                rand=rand,
            )
        )
        return SendMessageResponse(status="ok", retcode=0, data=SendMsgRsp(message_id=msgid))

    async def _build_forward_message(self, messages: list[Node], target: TargetInfo) -> MulitMsg:
        converted = await to_lagrange_msg(
            msg=cast(list[Any], messages),
            lgrc=self.lag.client,
            target=target,
        )
        nodes: list[ForwardNode] = []
        for item in converted:
            if not isinstance(item, ForwardNode):
                raise ValueError("forward message payload must contain only node segments")
            nodes.append(item)
        if not nodes:
            raise ValueError("forward message payload is empty")
        return MulitMsg(messages=nodes)

    @on(SendPrivateForwardMessage)
    async def send_private_forward_message(self, data: SendPrivateForwardMsgData) -> SendMessageResponse:
        forward = await self._build_forward_message(
            data.messages,
            TargetInfo(target="user", id=data.user_id),
        )
        uid = await info_mgr.uid_mgr.from_uin(data.user_id)
        seq = await self.lag.client.send_friend_forward_msg(forward, uid)
        msgid = await info_mgr.msgid_mgr.add(
            MsgInfo(
                raw_msg=[forward],
                scene_type="user",
                scene_id=data.user_id,
                seq=seq,
                timestamp=round(time.time()),
                uid=self.lag.client.uid,
                uin=self.lag.client.uin,
                text="[聊天记录]",
            )
        )
        return SendMessageResponse(status="ok", retcode=0, data=SendMsgRsp(message_id=msgid))

    @on(SendGroupForwardMessage)
    async def send_group_forward_message(self, data: SendGroupForwardMsgData) -> SendMessageResponse:
        forward = await self._build_forward_message(
            data.messages,
            TargetInfo(target="group", id=data.group_id),
        )
        seq = await self.lag.client.send_grp_forward_msg(forward, data.group_id)
        try:
            rand = (
                await self.lag.client.get_grp_msg(
                    grp_id=data.group_id,
                    start=seq,
                    end=seq,
                    filter_deleted_msg=False,
                )
            )[0].rand
        except (AttributeError, IndexError, KeyError):
            rand = 0
        msgid = await info_mgr.msgid_mgr.add(
            MsgInfo(
                raw_msg=[forward],
                scene_type="group",
                scene_id=data.group_id,
                seq=seq,
                timestamp=round(time.time()),
                uid=self.lag.client.uid,
                uin=self.lag.client.uin,
                text="[聊天记录]",
                rand=rand,
            )
        )
        return SendMessageResponse(status="ok", retcode=0, data=SendMsgRsp(message_id=msgid))

    @on(DeleteMessage)
    async def delete_message(self, data: DeleteMsgData) -> DeleteMessageResponse:
        msgid = data.message_id
        msg_info = await info_mgr.msgid_mgr.fetch(msgid)
        if msg_info.scene_type == "user":
            uid = await info_mgr.uid_mgr.from_uin(msg_info.scene_id)
            await self.lag.client.recall_friend_msg(uid=uid, seq=msg_info.seq)
        else:
            await self.lag.client.recall_grp_msg(grp_id=msg_info.scene_id, seq=msg_info.seq)
        return DeleteMessageResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(GetVersionInfo)
    async def get_version_info(self, _data: GetVersionInfoData) -> GetVersionInfoResponse:
        return GetVersionInfoResponse(status="ok", retcode=0, data=GetVersionInfoRsp())

    @on(GetStatus)
    async def get_status(self, _data: GetStatusData) -> GetStatusResponse:
        return GetStatusResponse(
            status="ok",
            retcode=0,
            data=GetStatusRsp(
                app_initialized=True,
                app_enabled=True,
                plugins_good=None,
                app_good=True,
                online=self.protocol.status.online,
                good=self.protocol.status.good,
                memory=_process_memory_bytes(),
            ),
        )

    @on(GetMessage)
    async def get_message(self, data: GetMsgData) -> GetMessageResponse:
        msg = await info_mgr.msgid_mgr.fetch(data.message_id)
        user_info = None
        user_cached = True  # 消息本身来自本地记录；只有用户信息是刚从服务器查的才算 False
        try:
            if msg.uid or msg.uin:
                ident = msg.uid or msg.uin
                user_info, user_cached = await self.cache.users.get_ex(
                    msg.uin or msg.uid, lambda: self.lag.client.get_user_info(ident), force=data.no_cache
                )
        except (AttributeError, ValueError, RuntimeError):
            logger.warning(f"get_user_info 失败,user_info 置空: uid={msg.uid!r} uin={msg.uin}")
        if isinstance(user_info, list):
            user_info = user_info[0] if user_info else None

        sex: Literal["male", "female", "unknown"] = "unknown"
        if user_info is not None:
            sex = user_info.sex.name if user_info.sex.name != "notset" else "unknown"  # type: ignore
        nickname = "" if not user_info else user_info.name
        age = 0 if not user_info else user_info.age
        area = "" if not user_info else f"{user_info.country} {user_info.province} {user_info.city}"

        return GetMessageResponse(
            status="ok",
            retcode=0,
            cached=user_cached,
            data=GetMsgRsp(
                message=await to_onebot_msg(msg=msg, adp=self.protocol),
                time=msg.timestamp,
                message_id=data.message_id,
                message_type="private" if msg.scene_type == "user" else "group",
                real_id=msg.seq,
                sender=onebot_events.PrivateSender(
                    user_id=msg.uin,
                    nickname=nickname,
                    sex=sex,
                    age=age,
                )
                if msg.scene_type == "user"
                else onebot_events.GroupSender(
                    user_id=msg.uin,
                    title="",
                    sex=sex,
                    role="member",
                    age=age,
                    area=area,
                    card="",
                    level="",
                    nickname=nickname,
                ),
            ),
        )

    async def _group_list_data(self, force: bool = False) -> tuple[list[GetGroupInfoRsp], bool]:
        async def load() -> list[GetGroupInfoRsp]:
            groups = await self.lag.client.get_grp_list()
            return [
                GetGroupInfoRsp(
                    group_id=i.grp_id,
                    group_name=i.info.grp_name,
                    member_count=i.info.now_members,
                    max_member_count=i.info.max_members,
                )
                for i in groups.grp_list
            ]

        return await self.cache.group_list.get_ex(GROUP_LIST_KEY, load, force=force)

    @on(GetGroupInfo)
    async def get_group_info(self, data: GetGroupInfoData) -> GetGroupInfoResponse:
        groups, cached = await self._group_list_data(data.no_cache)
        info = next((g for g in groups if g.group_id == data.group_id), None)
        if info is None and cached and (self.cache.group_list.age(GROUP_LIST_KEY) or 0) > GROUP_REFRESH_MIN_AGE:
            # 缓存的群列表里没有这个群，多半是刚加入的新群：刷新一次再找
            groups, cached = await self._group_list_data(force=True)
            info = next((g for g in groups if g.group_id == data.group_id), None)
        if info is None:
            raise ValueError(f"group {data.group_id} not found")
        return GetGroupInfoResponse(status="ok", retcode=0, data=info, cached=cached)

    @on(GetStrangerInfo)
    async def get_stranger_info(self, data: GetStrangerInfoData) -> GetStrangerInfoResponse:
        async def load():
            try:
                uid = await info_mgr.uid_mgr.from_uin(data.user_id)
            except ValueError:
                uid = None
            try:
                return await self.lag.client.get_user_info(uid or data.user_id)
            except AttributeError:
                return await self.lag.client.get_user_info(data.user_id)

        info, cached = await self.cache.users.get_ex(data.user_id, load, force=data.no_cache)
        return GetStrangerInfoResponse(
            status="ok",
            retcode=0,
            cached=cached,
            data=GetStrangerInfoRsp(
                age=info.age,
                nickname=info.name,
                sex=info.sex.name if info.sex.name != "notset" else "unknown",  # type: ignore
                user_id=data.user_id,
            ),
        )

    @on(GetForwardMessage)
    async def get_forward_msg(self, data: GetForwardMsgData) -> GetForwardMessageResponse:
        res_id = data.id
        msg, cached = await self.cache.forward.get_ex(
            res_id, lambda: self.lag.client.get_forward_msg(res_id), force=data.no_cache
        )
        return GetForwardMessageResponse(
            status="ok",
            retcode=0,
            cached=cached,
            data=GetForwardMsgRsp(
                message=cast(
                    list[Node],
                    await to_onebot_msg(
                        adp=self.protocol,
                        msg=MsgInfo(scene_type="user", scene_id=0, seq=0, raw_msg=msg.messages),  # type: ignore
                    ),
                )
            ),
        )

    @on(SetGroupKick)
    async def set_group_kick(self, data: SetGroupKickData) -> SetGroupKickResponse:
        await self.lag.client.kick_grp_member(grp_id=data.group_id, uin=data.user_id, permanent=data.reject_add_request)
        return SetGroupKickResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(SetGroupBan)
    async def set_group_ban(self, data: SetGroupBanData) -> SetGroupBanResponse:
        await self.lag.client.set_mute_member(grp_id=data.group_id, uin=data.user_id, duration=data.duration)
        return SetGroupBanResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(SetGroupWholeBan)
    async def set_group_whole_ban(self, data: SetGroupWholeBanData) -> SetGroupWholeBanResponse:
        await self.lag.client.set_mute_grp(grp_id=data.group_id, enable=data.enable)
        return SetGroupWholeBanResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(SetGroupCard)
    async def set_group_card(self, data: SetGroupCardData) -> SetGroupCardResponse:
        await self.lag.client.rename_grp_member(
            grp_id=data.group_id, target_uid=await info_mgr.uid_mgr.from_uin(data.user_id), name=data.card
        )
        return SetGroupCardResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(SetGroupName)
    async def set_group_name(self, data: SetGroupNameData) -> SetGroupNameResponse:
        await self.lag.client.rename_grp_name(grp_id=data.group_id, name=data.group_name)
        return SetGroupNameResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(SendPoke)
    async def send_poke(self, data: SendPokeData) -> SendPokeResponse:
        await self.lag.client.send_nudge(uin=data.user_id, grp_id=data.group_id)
        return SendPokeResponse(status="ok", retcode=0, data=EmptyRsp())

    async def _group_sign(self, group_id: int) -> SetGroupSignResponse:
        """群打卡：OIDB 0xEB7_1，请求体 {2: {1: 登录 QQ 号, 2: 群号, 3: 客户端版本}}（字符串字段）"""
        body = proto_encode(
            {2: {1: str(self.lag.client.uin), 2: str(group_id), 3: _GROUP_SIGN_VERSION}}
        )
        rsp = await self.lag.client.send_oidb_svc(0xEB7, 1, body)
        if rsp.ret_code:
            raise AssertionError(rsp.ret_code, rsp.err_msg)
        return SetGroupSignResponse(
            status="ok",
            retcode=0,
            data=GroupSignRsp(ret_code=rsp.ret_code, err_msg=rsp.err_msg or "", data_hex=bytes(rsp.data or b"").hex()),
        )

    async def _essence(self, message_id: int, remove: bool) -> None:
        """设置 / 取消群精华消息：OIDB 0xEAC，需要群号、消息序号和随机数"""
        info = await info_mgr.msgid_mgr.fetch(message_id)
        if info.scene_type != "group":
            raise ValueError("essence message must be a group message")
        await self.lag.client.set_essence(grp_id=info.scene_id, seq=info.seq, rand=info.rand, is_remove=remove)

    @on(SetEssenceMsg)
    async def set_essence_msg(self, data: SetEssenceMsgData) -> SetEssenceMsgResponse:
        await self._essence(data.message_id, False)
        return SetEssenceMsgResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(DeleteEssenceMsg)
    async def delete_essence_msg(self, data: SetEssenceMsgData) -> DeleteEssenceMsgResponse:
        await self._essence(data.message_id, True)
        return DeleteEssenceMsgResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(SetGroupSign)
    async def set_group_sign(self, data: SetGroupSignData) -> SetGroupSignResponse:
        return await self._group_sign(data.group_id)

    @on(SendGroupSign)
    async def send_group_sign(self, data: SetGroupSignData) -> SetGroupSignResponse:
        return await self._group_sign(data.group_id)

    @on(SetGroupLeave)
    async def set_group_leave(self, data: SetGroupLeaveData) -> SetGroupLeaveResponse:
        await self.lag.client.leave_grp(grp_id=data.group_id)
        return SetGroupLeaveResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(SetGroupAddRequest)
    async def set_group_add_request(self, data: SetGroupAddRequestData) -> SetGroupAddRequestResponse:
        info = await info_mgr.req_mgr.fetch(data.flag)
        if info.type == "group":
            await self.lag.client.set_grp_request(
                grp_id=info.id,
                grp_req_seq=info.seq,
                ev_type=info.ev_type,
                action=1 if data.approve else 2,
            )
            return SetGroupAddRequestResponse(status="ok", retcode=0, data=EmptyRsp())
        raise NotImplementedError()

    @on(SetFriendAddRequest)
    async def set_friend_add_request(self, data: SetFriendAddRequestData) -> SetFriendAddRequestResponse:
        await self.lag.client.set_friend_request(target_uid=data.flag, accept=data.approve)
        return SetFriendAddRequestResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(GetLoginInfo)
    async def get_login_info(self, data: GetLoginInfoData) -> GetLoginInfoResponse:
        info, cached = await self.cache.users.get_ex(
            self.lag.client.uin, lambda: self.lag.client.get_user_info(self.lag.client.uin), force=data.no_cache
        )
        return GetLoginInfoResponse(
            status="ok",
            retcode=0,
            cached=cached,
            data=GetLoginInfoRsp(user_id=self.lag.client.uin, nickname=info.name),
        )

    def _member_from_list_entry(self, group_id: int, uin: int, body) -> GetGroupMemberInfoRsp:
        """把成员列表里的一项转成 OneBot 的成员信息。

        列表里没有性别 / 年龄 / 地区，只有用户资料缓存里已经有这个人时才填，否则用默认值——
        不为这三项逐个成员向服务器查询（那正是被限流的原因）；需要精确值请用 get_stranger_info。
        """
        user_info = self.cache.users.peek(uin)
        role = "member"
        if body.is_owner:
            role = "owner"
        elif body.is_admin:
            role = "admin"
        sex = "unknown"
        age = 0
        area = ""
        if user_info is not None:
            sex = user_info.sex.name if user_info.sex.name != "notset" else "unknown"
            age = user_info.age
            area = f"{user_info.country} {user_info.province} {user_info.city}"
        return GetGroupMemberInfoRsp(
            group_id=group_id,
            user_id=uin,
            nickname=body.nickname,
            card="" if not body.name else body.name.string,
            sex=sex,  # type: ignore
            age=age,
            area=area,
            join_time=body.joined_time,
            last_sent_time=body.last_seen,
            level="" if not body.level else str(body.level.num),
            role=role,
            title="",
        )

    async def _member_page_raw(self, group_id: int, next_key):
        """发一页群成员请求，返回 (错误码, 错误信息, 页内容)。

        hiro-qq 自带的 get_grp_members 不检查错误码，被限流时会返回空列表而不报错，
        所以这里直接发 OIDB 请求并自己检查。
        """
        if isinstance(next_key, bytes):
            next_key = next_key.decode()  # 上一页返回的 next_key 是 bytes，请求里要用 str
        rsp = await self.lag.client.send_oidb_svc(0xFE7, 4, PBGetGrpMemberInfoReq.build(group_id, next_key=next_key).encode())
        if rsp.ret_code:
            return rsp.ret_code, rsp.err_msg, None
        return 0, "", GetGrpMemberInfoRsp.decode(rsp.data)

    async def _fetch_member_page(self, group_id: int, next_key):
        """限速地取一页群成员；被服务器拒绝（限流）就减速、退避后重试"""
        limiter = self.cache.member_page_limiter
        last = ""
        for attempt in range(MEMBER_PAGE_ATTEMPTS):
            await limiter.wait()
            try:
                code, msg, page = await self._member_page_raw(group_id, next_key)
            except Exception as e:  # noqa: BLE001
                code, msg, page = -1, repr(e), None
            if page is not None:
                limiter.success()
                return page
            limiter.failure()
            last = f"ret_code={code} {msg}"
            if attempt + 1 < MEMBER_PAGE_ATTEMPTS:
                await asyncio.sleep(MEMBER_PAGE_BACKOFF * (2**attempt))
        raise RuntimeError(f"群成员查询被服务器拒绝（群 {group_id}，{MEMBER_PAGE_ATTEMPTS} 次尝试后仍失败：{last}）")

    async def _remember_uid(self, group_id: int, uid: str, uin: int) -> None:
        """成员表里带着 uid 和 QQ 号，顺手记进映射表（以前启动时单独再拉一遍成员就是为了这个）"""
        if not uid or uin == group_id:
            return
        try:
            if not (await info_mgr.uid_mgr.is_exist(uid) and await info_mgr.uid_mgr.is_exist(uin)):
                await info_mgr.uid_mgr.add(uid, uin)
                self._uid_added += 1
        except Exception as e:  # noqa: BLE001
            logger.debug(f"记录 uid 映射失败: {e!r}")

    async def prewarm_cache(self) -> int:
        """
        启动时预热：群列表，以及每个群的成员表（顺带建立 uid 映射）。
        走限速的缓存，节奏很慢；Overflow 连上来之后请求同一个群时直接命中缓存，
        或者和正在进行的预热合并成同一次查询，不会叠加成突发。返回新学到的映射数。
        """
        before = self._uid_added
        try:
            groups, _ = await self._group_list_data()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"预热：取群列表失败: {e!r}")
            return 0
        pending = [g.group_id for g in groups]
        for round_no in (1, 2):
            failed: list[int] = []
            for gid in pending:
                try:
                    await self._group_members(gid)
                except Exception as e:  # noqa: BLE001
                    failed.append(gid)
                    logger.warning(f"预热：群 {gid} 的成员表加载失败（第 {round_no} 轮）: {e!r}")
                await asyncio.sleep(PREWARM_GROUP_GAP)
            pending = failed
            if not pending:
                break
            if round_no == 1:
                # 多半是被限流了：歇一会儿再补拉失败的群
                await asyncio.sleep(PREWARM_RETRY_DELAY)
        suffix = f"，仍有 {len(pending)} 个失败（之后有人用到时再加载）" if pending else ""
        logger.info(f"预热完成：{len(groups) - len(pending)}/{len(groups)} 个群的成员表已缓存{suffix}")
        return self._uid_added - before

    async def _group_members(self, group_id: int, force: bool = False) -> tuple[dict[int, GetGroupMemberInfoRsp], bool]:
        """整个群的成员表。每页 500 人，按 next_key 翻页拉全，一个群只需要很少的请求。"""

        async def load() -> dict[int, GetGroupMemberInfoRsp]:
            members: dict[int, GetGroupMemberInfoRsp] = {}
            next_key = None
            while True:
                page = await self._fetch_member_page(group_id, next_key)
                for body in page.body:
                    uin = body.account.uin
                    if not uin:
                        try:
                            uin = await info_mgr.uid_mgr.from_uid(body.account.uid)
                        except ValueError:
                            continue
                    else:
                        await self._remember_uid(group_id, body.account.uid, uin)
                    members[uin] = self._member_from_list_entry(group_id, uin, body)
                next_key = page.next_key
                if not next_key:
                    if not members:
                        # 群里至少有机器人自己；空表几乎一定是服务器出了问题，不能当成功缓存起来
                        raise RuntimeError(f"group {group_id} returned an empty member list")
                    return members

        return await self.cache.member_list.get_ex(group_id, load, force=force)

    async def _find_member(
        self, group_id: int, user_id: int, force: bool = False
    ) -> tuple[GetGroupMemberInfoRsp | None, bool]:
        members, cached = await self._group_members(group_id, force)
        info = members.get(user_id)
        if info is None and cached and (self.cache.member_list.age(group_id) or 0) > MEMBER_REFRESH_MIN_AGE:
            # 缓存的成员表里没有这个人，可能是新加入的：刷新一次再找。
            # 刷新有最短间隔，免得反复查询不在群里的人把整个成员表一遍遍重拉
            members, cached = await self._group_members(group_id, force=True)
            info = members.get(user_id)
        return info, cached

    async def try_member(self, group_id: int, user_id: int) -> GetGroupMemberInfoRsp | None:
        """给消息事件用：取不到（比如被限流）就返回 None，不能因此丢掉整条消息"""
        try:
            # 成员表正在后台加载时最多等几秒；超时只是这条消息降级，加载仍会继续（缓存内部用了 shield）
            return (await asyncio.wait_for(self._find_member(group_id, user_id), SENDER_INFO_TIMEOUT))[0]
        except Exception as e:  # noqa: BLE001
            logger.warning(f"取群成员信息失败，发送者信息降级: group={group_id} user={user_id}: {e!r}")
            return None

    async def try_user_info(self, uin: int, uid: str):
        """给消息事件用：用户资料走缓存，取不到就返回 None"""
        try:
            return (await self.cache.users.get_ex(uin, lambda: self.lag.client.get_user_info(uid)))[0]
        except Exception as e:  # noqa: BLE001
            logger.warning(f"取用户资料失败，发送者信息降级: user={uin}: {e!r}")
            return None

    @on(GetGroupMemberInfo)
    async def get_group_member_info(self, data: GetGroupMemberInfoData) -> GetGroupMemberInfoResponse:
        info, cached = await self._find_member(data.group_id, data.user_id, data.no_cache)
        if info is None:
            raise ValueError(f"member {data.user_id} not found in group {data.group_id}")
        return GetGroupMemberInfoResponse(status="ok", retcode=0, data=info, cached=cached)

    @on(GetCookie)
    async def get_cookie(self, data: GetCookieData) -> GetCookieResponse:
        async def load() -> str:
            return (await self.lag.client.get_cookies([data.domain]))[0]

        cookie, cached = await self.cache.cookies.get_ex(data.domain, load, force=data.no_cache)
        return GetCookieResponse(status="ok", retcode=0, data=GetCookieRsp(cookies=cookie), cached=cached)

    @on(GetCSRFToken)
    async def get_csrf_token(self, data: GetCSRFTokenData) -> GetCSRFTokenResponse:
        token, cached = await self.cache.csrf.get_ex(
            "token", lambda: self.lag.client.get_csrf_token(), force=data.no_cache
        )
        return GetCSRFTokenResponse(status="ok", retcode=0, data=GetCSRFTokenRsp(token=token), cached=cached)

    @on(SendLike)
    async def send_like(self, data: SendLikeData) -> SendLikeResponse:
        uid = await info_mgr.uid_mgr.from_uin(data.user_id)
        await self.lag.client.friend_like(uid, data.times)
        return SendLikeResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(SetGroupAdmin)
    async def set_group_admin(self, data: SetGroupAdminData) -> SetGroupAdminResponse:
        uid = await info_mgr.uid_mgr.from_uin(data.user_id)
        await self.lag.client.set_grp_admin(grp_id=data.group_id, uid=uid, is_set=data.enable)
        return SetGroupAdminResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(GetFriendList)
    async def get_friend_list(self, data: GetFriendListData) -> GetFriendListResponse:
        async def load() -> list[FriendElem]:
            friends = await self.lag.client.get_friend_list()
            return [FriendElem(user_id=i.uin, nickname=i.nickname or "", remark=i.remark or "") for i in friends]

        elems, cached = await self.cache.friends.get_ex("all", load, force=data.no_cache)
        return GetFriendListResponse(
            status="ok", retcode=0, data=GetFriendListRsp.model_validate(elems), cached=cached
        )

    @on(GetGroupList)
    async def get_group_list(self, data: GetGroupListData) -> GetGroupListResponse:
        groups, cached = await self._group_list_data(data.no_cache)
        return GetGroupListResponse(status="ok", retcode=0, data=GetGroupListRsp.model_validate(groups), cached=cached)

    @on(GetGroupMemberList)
    async def get_group_member_list(self, data: GetGroupMemberListData) -> GetGroupMemberListResponse:
        members, cached = await self._group_members(data.group_id, data.no_cache)
        return GetGroupMemberListResponse(
            status="ok", retcode=0, data=GetGroupMemberListRsp.model_validate(list(members.values())), cached=cached
        )

    async def _group_reaction(self, data: GroupReactionData) -> GroupReactionResponse:
        msg_info = await info_mgr.msgid_mgr.fetch(data.message_id or 0)
        raw = data.emoji if data.emoji else (data.code or "")
        # NapCat/NTQQ 语义: emoji_id 为 ≤3 位数字时是经典 face id(type=1);
        # >3 位数字是 unicode emoji 的十进制码点(type=2),hiro-qq 的 type=2 分支
        # 会取单字符的 ord() 作为码点,故此处传 chr(codepoint)
        if raw.isdigit():
            if len(raw) <= 3:
                content: int | str = int(raw)  # face id
            else:
                content = chr(int(raw))  # unicode emoji 码点
        elif len(raw) == 1:
            content = raw  # 已是单字符 emoji,hiro-qq 自行 ord()
        else:
            content = raw[0]
        await self.lag.client.send_grp_reaction(
            grp_id=data.group_id, msg_seq=msg_info.seq, content=content, is_cancel=not data.is_add
        )
        return GroupReactionResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(GroupReaction)
    async def group_reaction(self, data: GroupReactionData) -> GroupReactionResponse:
        return await self._group_reaction(data)

    @on(SetGroupReaction)
    async def set_group_reaction(self, data: GroupReactionData) -> GroupReactionResponse:
        return await self._group_reaction(data)

    @on(SetGroupSpecialTitle)
    async def set_group_special_title(self, data: SetGroupSpecialTitleData) -> SetGroupSpecialTitleResponse:
        uid = await info_mgr.uid_mgr.from_uin(data.user_id)
        await self.lag.client.set_grp_special_title(data.group_id, uid, data.special_title)
        return SetGroupSpecialTitleResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(UploadGroupFile)
    async def upload_group_file(self, data: UploadGroupFileData) -> UploadGroupFileResponse:
        source, filename = await self._open_upload_file(data.file, data.name)
        try:
            await self.lag.client.upload_grp_file(source, data.group_id, data.folder or "/", filename)
        finally:
            source.close()
        return UploadGroupFileResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(UploadPrivateFile)
    async def upload_private_file(self, data: UploadPrivateFileData) -> UploadPrivateFileResponse:
        uid = await info_mgr.uid_mgr.from_uin(data.user_id)
        source, filename = await self._open_upload_file(data.file, data.name)
        try:
            await self.lag.client.upload_friend_file(source, uid, filename)
        finally:
            source.close()
        return UploadPrivateFileResponse(status="ok", retcode=0, data=EmptyRsp())

    @on(GetGroupFileUrl)
    async def get_group_file_url(self, data: GetGroupFileUrlData) -> GetGroupFileUrlResponse:
        url, cached = await self.cache.file_urls.get_ex(
            ("group", data.group_id, data.file_id),
            lambda: self.lag.client.fetch_grp_file_url(data.group_id, data.file_id),
            force=data.no_cache,
        )
        return GetGroupFileUrlResponse(status="ok", retcode=0, data=GetGroupFileUrlRsp(url=url), cached=cached)

    @on(GetPrivateFileUrl)
    async def get_private_file_url(self, data: GetPrivateFileUrlData) -> GetPrivateFileUrlResponse:
        async def load() -> str:
            uid = await info_mgr.uid_mgr.from_uin(data.user_id)
            return await self.lag.client.fetch_friend_file_url(data.file_id, data.file_hash, uid)

        url, cached = await self.cache.file_urls.get_ex(
            ("private", data.user_id, data.file_id, data.file_hash), load, force=data.no_cache
        )
        return GetPrivateFileUrlResponse(status="ok", retcode=0, data=GetPrivateFileUrlRsp(url=url), cached=cached)
