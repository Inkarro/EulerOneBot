import time
import traceback
from urllib.parse import parse_qs, urlsplit
from collections.abc import Callable
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    TypeVar,
    cast,
    runtime_checkable,
)

from lagrange import Client, Lagrange
from lagrange.client.events import BaseEvent
from lagrange.client.events.friend import (
    FriendAddNotify,
    FriendMessage,
    FriendPoke,
    FriendRecall,
    FriendRequest,
)
from lagrange.client.events.group import (
    GroupAdminChange,
    GroupInvite,
    GroupMemberJoined,
    GroupNameChanged,
    GroupSelfJoined,
    GroupMemberJoinedByInvite,
    GroupMemberJoinRequest,
    GroupMemberQuit,
    GroupMessage,
    GroupMuteMember,
    GroupNudge,
    GroupReaction,
    GroupRecall,
)
from lagrange.client.events.service import ClientOffline, ClientOnline, ServerKick
from lagrange.client.message.elems import Json

from ..hyperogger import Logger
from ..onebot import Adapter as OneBotAdapter
from ..onebot import events as onebot_events
from ..utils.infomgr import MsgInfo, RequestInfo, info_mgr
from ..utils.transformer import to_onebot_msg

if TYPE_CHECKING:
    from . import LagrangeProtocol
else:
    LagrangeProtocol = Any

__all__ = ["LagrangeEventHandler"]

logger = Logger.fetch("euler").name_custom("euler.protocol.handle")
LagrangeEvent = BaseEvent
F = TypeVar("F", bound=Callable[..., object])


# fetch_grp_request 里「机器人被邀请入群」这一类请求的 event_type（实测可以用它同意邀请）
BOT_INVITE_EV_TYPE = 2


def parse_invite_card(raw: bytes | str) -> tuple[int, int, int, int] | None:
    """
    解析「邀请你加入群聊」卡片（app 消息，bizsrc 为 qun.invite），返回 (群号, 请求序号, 邀请人, 被邀请人)。
    不是这种卡片或缺字段就返回 None。卡片里的 msgseq 就是同意邀请要用的请求序号。
    """
    import json

    try:
        data = json.loads(raw)
        if data.get("bizsrc") != "qun.invite":
            return None
        url = data["meta"]["news"]["jumpUrl"]
        query = parse_qs(urlsplit(url).query)
        return (
            int(query["groupcode"][0]),
            int(query["msgseq"][0]),
            int(query.get("senderuin", ["0"])[0]),
            int(query.get("receiveruin", ["0"])[0]),
        )
    except (ValueError, KeyError, IndexError, TypeError, AttributeError):
        return None


class HandlerHook:
    def __init__(self, raw_func: "RegisteredHandler | None" = None):
        self.handler: LagrangeEventHandler | None = None
        self.raw_func = raw_func

    def set_handler(self, handler: "LagrangeEventHandler") -> None:
        self.handler = handler
        if self.raw_func:
            self.raw_func.handler_hook.set_handler(handler)

    def update(self):
        if self.handler:
            self.handler.last_handled = time.time()


@runtime_checkable
class RegisteredHandler(Protocol):
    ev_type: type[LagrangeEvent]
    handler_hook: HandlerHook


def on(ev_type: type[LagrangeEvent]) -> Callable[[F], F]:
    def dec(func: F) -> F:
        cast(RegisteredHandler, func).ev_type = ev_type
        cast(RegisteredHandler, func).handler_hook = HandlerHook()

        async def wrapper(*args, **kwargs) -> Any:
            cast(RegisteredHandler, func).handler_hook.update()
            return await func(*args, **kwargs)  # type: ignore

        cast(RegisteredHandler, cast(object, wrapper)).ev_type = ev_type
        cast(RegisteredHandler, cast(object, wrapper)).handler_hook = HandlerHook(
            raw_func=cast(RegisteredHandler, func)
        )
        return cast(F, wrapper)

    return dec


class LagrangeEventHandler:
    def __init__(self, onebot_adapter: OneBotAdapter, lag: Lagrange, protocol: LagrangeProtocol):
        self.adapter = onebot_adapter
        self.lag = lag
        self.info_updated = False
        self.protocol = protocol

        self.last_handled = time.time()

    def _update_cache(self, fn: Callable[..., None], *args) -> None:
        """服务器推送了变更时顺手更新信息缓存；缓存出问题绝不能影响事件本身的处理"""
        try:
            fn(*args)
        except Exception:  # noqa: BLE001
            logger.error("update info cache failed")
            logger.trace(traceback.format_exc())

    @staticmethod
    def _make_safe(handler):
        async def wrapper(client, event):
            try:
                await handler(client, event)
            except Exception:
                logger.error(f"error handling event: {type(event).__name__}")
                logger.trace(traceback.format_exc())

        return wrapper

    def subscribe(self) -> None:
        for i in dir(self):
            attr = getattr(self, i)  # it seems like py3.14 has a **** change
            func = getattr(attr, "__func__", attr)
            if isinstance(func, RegisteredHandler):
                func.handler_hook.set_handler(self)
                self.lag.subscribe(func.ev_type, self._make_safe(attr))  # type: ignore

    @on(ClientOnline)
    async def online_handler(self, client: Client, _event: ClientOnline) -> None:
        self.adapter.connector.self_id = client.uin
        await self.protocol.emit_lifecycle("connect")
        if not self.info_updated:
            await info_mgr.uid_mgr.load_all(client, self.protocol.impl.prewarm_cache)
            self.info_updated = True

    @on(ClientOffline)
    async def offline_handler(self, _client: Client, event: ClientOffline) -> None:
        logger.warning(f"client offline, recoverable = {event.recoverable}")
        self.protocol.set_online(False)
        if not event.recoverable:
            await self.protocol.emit_lifecycle("disable")

    @on(ServerKick)
    async def kick_handler(self, _client: Client, event: ServerKick) -> None:
        logger.error(f"Kicked by server: {event.title} {event.tips}")
        self.protocol.set_online(False)
        await self.protocol.emit_lifecycle("disable")

    @on(GroupMessage)
    async def grp_msg_handler(self, client: Client, event: GroupMessage) -> None:
        if event.uin == self.lag.client.uin or len(event.msg_chain) == 0:
            return
        logger.info(f"[Group] {event.grp_name}({event.grp_id}): @{event.nickname}({event.uin}): {event.msg}")
        if not await info_mgr.uid_mgr.is_exist(event.uid):
            await info_mgr.uid_mgr.add(event.uid, event.uin)
        # 发送者资料走缓存（群成员信息来自按群缓存的成员表）；取不到就降级，消息照常上报
        impl = self.protocol.impl
        member = await impl.try_member(event.grp_id, event.uin)
        user_info = await impl.try_user_info(event.uin, event.uid)
        role = member.role if member else "member"
        ev = onebot_events.GroupMessageEvent(
            time=event.time,
            self_id=self.lag.client.uin,
            message_id=await info_mgr.msgid_mgr.add(
                MsgInfo(
                    raw_msg=event.msg_chain,
                    scene_type="group",
                    scene_id=event.grp_id,
                    seq=event.seq,
                    timestamp=event.time,
                    uin=event.uin,
                    uid=event.uid,
                    text=event.msg,
                    rand=event.rand,
                )
            ),
            user_id=event.uin,
            message=await to_onebot_msg(event=event, adp=self.protocol),
            group_id=event.grp_id,
            raw_message=event.msg,
            sender=onebot_events.GroupSender(
                age=user_info.age if user_info else 0,
                area=f"{user_info.country} {user_info.province} {user_info.city}" if user_info else "",
                card=member.card if member else "",
                level=member.level if member else "",
                nickname=user_info.name if user_info else event.nickname,
                role=role,
                sex=(user_info.sex.name if user_info.sex.name != "notset" else "unknown") if user_info else "unknown",  # type: ignore
                title="",
                user_id=event.uin,
            ),
        )
        await self.adapter.trigger(ev)

    @on(FriendMessage)
    async def pri_msg_handler(self, _client: Client, event: FriendMessage) -> None:
        if event.from_uin == self.lag.client.uin or len(event.msg_chain) == 0:
            return
        logger.info(f"[Friend] {event.from_uin} -> {event.to_uin}: {event.msg}")
        try:
            await self._check_invite_card(event)
        except Exception:  # noqa: BLE001
            logger.error("handle invite card failed")
            logger.trace(traceback.format_exc())
        if not await info_mgr.uid_mgr.is_exist(event.from_uid):
            await info_mgr.uid_mgr.add(event.from_uid, event.from_uin)
        # 发送者资料走缓存；取不到就降级，消息照常上报
        user_info = await self.protocol.impl.try_user_info(event.from_uin, event.from_uid)
        ev = onebot_events.PrivateMessageEvent(
            time=event.timestamp,
            self_id=self.lag.client.uin,
            message_id=await info_mgr.msgid_mgr.add(
                MsgInfo(
                    raw_msg=event.msg_chain,
                    scene_type="user",
                    scene_id=event.from_uin,
                    seq=event.seq,
                    timestamp=event.timestamp,
                    uin=event.from_uin,
                    uid=event.from_uid,
                    rand=event.rand,
                    text=event.msg,
                )
            ),
            user_id=event.from_uin,
            message=await to_onebot_msg(event=event, adp=self.protocol),
            raw_message=event.msg,
            sender=onebot_events.PrivateSender(
                age=user_info.age if user_info else 0,
                nickname=user_info.name if user_info else "",
                sex=(user_info.sex.name if user_info.sex.name != "notset" else "unknown") if user_info else "unknown",  # type: ignore
                user_id=event.from_uin,
            ),
        )
        await self.adapter.trigger(ev)

    @on(GroupRecall)
    async def grp_recall_handler(self, _client: Client, event: GroupRecall) -> None:
        msgid = await info_mgr.msgid_mgr.search(MsgInfo(scene_id=event.grp_id, scene_type="group", seq=event.seq))
        if not msgid:
            return
        try:
            opt_uin = await info_mgr.uid_mgr.from_uid(event.operator_id)
        except ValueError:
            opt_uin = 0
        logger.info(f"[Group] {event.grp_id}: message {event.seq} had been deleted by {opt_uin}")
        real_info = await info_mgr.msgid_mgr.fetch(msgid)
        ev = onebot_events.GroupRecallEvent(
            group_id=event.grp_id,
            message_id=msgid,
            operator_id=opt_uin,
            self_id=self.lag.client.uin,
            time=event.time,
            user_id=real_info.uin,
        )
        await self.adapter.trigger(ev)

    @on(FriendRecall)
    async def pri_recall_handler(self, _client: Client, event: FriendRecall) -> None:
        if event.from_uin == self.lag.client.uin:
            return
        logger.info(f"[Friend] {event.from_uin} -> {event.to_uin}: message {event.seq} had been deleted")
        msgid = await info_mgr.msgid_mgr.search(MsgInfo(scene_id=event.from_uin, scene_type="user", seq=event.seq))
        if not msgid:
            return
        ev = onebot_events.FriendRecallEvent(
            message_id=msgid,
            self_id=self.lag.client.uin,
            time=event.timestamp,
            user_id=event.from_uin,
        )
        await self.adapter.trigger(ev)

    @on(GroupMuteMember)
    async def grp_mute_handler(self, _client: Client, event: GroupMuteMember) -> None:
        logger.info(
            f"[Group] {event.grp_id}: member {event.target_uid} had been muted "
            f"by {event.operator_uid} for {event.duration}s"
        )
        try:
            opt_uin = await info_mgr.uid_mgr.from_uid(event.operator_uid)
            uin = 0 if not event.target_uid else await info_mgr.uid_mgr.from_uid(event.target_uid)
        except ValueError:
            return
        ev = onebot_events.GroupMuteEvent(
            duration=event.duration,
            group_id=event.grp_id,
            operator_id=opt_uin,
            self_id=self.lag.client.uin,
            sub_type="lift_ban" if event.duration == 0 else "ban",
            time=round(time.time()),
            user_id=uin,
        )
        await self.adapter.trigger(ev)

    @on(GroupMemberJoined)
    async def grp_join_handler(self, _client: Client, event: GroupMemberJoined) -> None:
        if event.join_type:
            return
        logger.info(f"[Group] {event.grp_id}: member {event.uid} has joined")
        self._update_cache(self.protocol.impl.cache.on_member_joined, event.grp_id)
        try:
            uin = await info_mgr.uid_mgr.from_uid(event.uid)
        except ValueError:
            rs = await self.lag.client.get_grp_member_info(grp_id=event.grp_id, uid=event.uid)
            uin = rs.body[0].account.uin or 0
            if uin:
                await info_mgr.uid_mgr.add(event.uid, uin)
        ev = onebot_events.GroupIncreaseEvent(
            group_id=event.grp_id,
            operator_id=0,
            self_id=self.lag.client.uin,
            sub_type="approve",
            time=round(time.time()),
            user_id=uin,
        )
        await self.adapter.trigger(ev)

    @on(GroupMemberJoinedByInvite)
    async def grp_invite_join_handler(self, _client: Client, event: GroupMemberJoinedByInvite) -> None:
        logger.info(f"[Group] {event.grp_id}: member {event.uin} has joined, invited by {event.invitor_uin}")
        self._update_cache(self.protocol.impl.cache.on_member_joined, event.grp_id)
        ev = onebot_events.GroupIncreaseEvent(
            group_id=event.grp_id,
            operator_id=event.invitor_uin,
            self_id=self.lag.client.uin,
            sub_type="invite",
            time=round(time.time()),
            user_id=event.uin,
        )
        await self.adapter.trigger(ev)

    @on(GroupMemberQuit)
    async def grp_quit_handler(self, _client: Client, event: GroupMemberQuit) -> None:
        logger.info(
            f"[Group] {event.grp_id}: member {event.uin} has left"
            f"{', kicked by ' + event.operator_uid if event.is_kicked else ''}"
        )
        opt_uin = 0
        if event.is_kicked or event.is_kicked_self:
            try:
                opt_uin = await info_mgr.uid_mgr.from_uid(event.operator_uid)
            except ValueError:
                opt_uin = 0

        if event.is_kicked_self:
            tp = "kick_me"
        elif event.is_kicked:
            tp = "kick"
        else:
            tp = "leave"
        if not await info_mgr.uid_mgr.is_exist(event.uin) and event.uin != event.grp_id:
            await info_mgr.uid_mgr.add(event.uid, event.uin)
        try:
            quit_uin = await info_mgr.uid_mgr.from_uid(event.uid)
        except ValueError:
            quit_uin = 0
        if event.is_kicked_self or quit_uin == self.lag.client.uin:
            self._update_cache(self.protocol.impl.cache.on_self_group_changed)
        else:
            self._update_cache(self.protocol.impl.cache.on_member_left, event.grp_id, quit_uin)
        ev = onebot_events.GroupDecreaseEvent(
            group_id=event.uin,
            operator_id=opt_uin,
            self_id=self.lag.client.uin,
            sub_type=tp,
            time=round(time.time()),
            user_id=await info_mgr.uid_mgr.from_uid(event.uid),
        )
        await self.adapter.trigger(ev)

    @on(GroupNudge)
    async def poke_handler(self, _client: Client, event: GroupNudge) -> None:
        ev = onebot_events.GroupPokeEvent(
            time=round(time.time()),
            self_id=self.lag.client.uin,
            group_id=event.grp_id,
            user_id=event.sender_uin,
            target_id=event.target_uin,
        )
        await self.adapter.trigger(ev)

    @on(FriendPoke)
    async def friend_poke_handler(self, _client: Client, event: FriendPoke) -> None:
        if event.from_uin == self.lag.client.uin:
            return
        target_id = event.target_uin
        if not target_id and event.target_uid:
            try:
                target_id = await info_mgr.uid_mgr.from_uid(event.target_uid)
            except ValueError:
                target_id = 0
        ev = onebot_events.FriendPokeEvent(
            time=event.timestamp or round(time.time()),
            self_id=self.lag.client.uin,
            user_id=event.from_uin or event.sender_uin,
            target_id=target_id,
        )
        await self.adapter.trigger(ev)

    @on(GroupReaction)
    async def reaction_handler(self, _client: Client, event: GroupReaction) -> None:
        try:
            if event.uid:
                uid = event.uid
                uin = await info_mgr.uid_mgr.from_uid(event.uid)
            else:
                uid = ""
                uin = 0
        except ValueError:
            if event.uid:
                uid = event.uid
                uin = 0
            else:
                uid = ""
                uin = 0
        msgid = await info_mgr.msgid_mgr.search(MsgInfo(scene_type="group", scene_id=event.grp_id, seq=event.seq))
        if not msgid:
            msgid = await info_mgr.msgid_mgr.add(
                MsgInfo(
                    scene_type="group",
                    scene_id=event.grp_id,
                    seq=event.seq,
                    uid=uid,
                    uin=uin,
                )
            )
        ev = onebot_events.ReactionEvent(
            time=round(time.time()),
            self_id=self.lag.client.uin,
            notice_type=self.protocol.cfg.event_compatibility.reaction_event_type,
            message_id=msgid,
            operator_id=uin,
            group_id=event.grp_id,
            user_id=uin,
            sub_type="add" if event.is_increase else "remove",
            code=event.emoji_id,
            count=event.emoji_count,
        )
        await self.adapter.trigger(ev)

    async def _emit_bot_invite(self, grp_id: int, seq: int, invitor_uin: int, comment: str = "") -> bool:
        """上报一次「机器人被邀请入群」。同一次邀请（群号 + 序号）只上报一次——卡片和服务器推送都可能带来。"""
        info = RequestInfo(type="group", id=grp_id, seq=seq, ev_type=BOT_INVITE_EV_TYPE)
        if await info_mgr.req_mgr.has(info):
            return False
        flag = await info_mgr.req_mgr.set_group(grp_id=grp_id, seq=seq, ev_type=BOT_INVITE_EV_TYPE)
        logger.info(f"[Group] bot was invited to {grp_id} by {invitor_uin} (seq={seq}, flag={flag})")
        await self.adapter.trigger(
            onebot_events.GroupRequestEvent(
                time=round(time.time()),
                self_id=self.lag.client.uin,
                sub_type="invite",
                group_id=grp_id,
                user_id=invitor_uin,
                comment=comment,
                flag=flag,
            )
        )
        return True

    async def _check_invite_card(self, event: FriendMessage) -> None:
        """私聊里收到「邀请你加入群聊」卡片 -> 上报成入群邀请事件"""
        for elem in event.msg_chain:
            if not isinstance(elem, Json):
                continue
            card = parse_invite_card(elem.raw)
            if card is None:
                continue
            grp_id, seq, sender_uin, receiver_uin = card
            if receiver_uin and receiver_uin != self.lag.client.uin:
                logger.info(f"[Friend] 邀请卡片的接收人({receiver_uin})不是本账号，忽略")
                continue
            # 邀请人用实际发消息的人，而不是卡片里自己声称的 senderuin（卡片内容可以是转发来的）
            if sender_uin and sender_uin != event.from_uin:
                logger.info(f"[Friend] 邀请卡片里的邀请人({sender_uin})和发送者({event.from_uin})不同，按发送者算")
            await self._emit_bot_invite(grp_id, seq, event.from_uin)

    @on(GroupInvite)
    async def bot_invited_handler(self, _client: Client, event: GroupInvite) -> None:
        """服务器推送：机器人被邀请入群。到待处理请求列表里找出对应的那一项，上报成入群邀请事件"""
        logger.info(f"[Group] invitation push: group {event.grp_id}, invitor_uid {event.invitor_uid}")
        rev = await self.lag.client.fetch_grp_request()
        found = False
        for i in rev.requests:
            if i.group.grp_id != event.grp_id:
                continue
            logger.info(f"[Group] pending request: group={i.group.grp_id} type={i.event_type} state={i.state} seq={i.seq}")
            if i.event_type != BOT_INVITE_EV_TYPE:
                continue
            found = True
            invitor_uid = (i.invitor.uid if i.invitor else "") or event.invitor_uid
            try:
                invitor_uin = await info_mgr.uid_mgr.from_uid(invitor_uid)
            except ValueError:
                invitor_uin = 0
            await self._emit_bot_invite(i.group.grp_id, i.seq, invitor_uin, i.comment)
        if not found:
            # 实测：推送先于邀请卡片到达，此时待处理请求列表里还没有这次邀请，邀请由随后到达的邀请卡片上报
            logger.info(f"[Group] 群 {event.grp_id} 的邀请推送：待处理请求里还没有对应的邀请项，等邀请卡片")

    @on(GroupMemberJoinRequest)
    async def join_req_handler(self, _client: Client, event: GroupMemberJoinRequest) -> None:
        rev = await self.lag.client.fetch_grp_request()
        req = None
        for i in rev.requests:
            if i.event_type == 1 and i.group.grp_id == event.grp_id and i.target.uid == event.uid:
                req = i
                break
        assert req
        try:
            uin = await info_mgr.uid_mgr.from_uid(event.uid)
        except ValueError:
            uin = 0
        flag = await info_mgr.req_mgr.set_group(grp_id=req.group.grp_id, seq=req.seq, ev_type=req.event_type)
        ev = onebot_events.GroupRequestEvent(
            time=round(time.time()),
            self_id=self.lag.client.uin,
            sub_type="add" if not event.invitor_uid else "invite",
            group_id=req.group.grp_id,
            user_id=uin,
            comment=req.comment,
            flag=flag,
        )
        await self.adapter.trigger(ev)

    @on(GroupNameChanged)
    async def grp_name_handler(self, _client: Client, event: GroupNameChanged) -> None:
        self._update_cache(self.protocol.impl.cache.on_group_name_changed, event.grp_id, event.name_new)

    @on(GroupSelfJoined)
    async def grp_self_joined_handler(self, _client: Client, _event: GroupSelfJoined) -> None:
        self._update_cache(self.protocol.impl.cache.on_self_group_changed)

    @on(GroupAdminChange)
    async def grp_admin_handler(self, _client: Client, event: GroupAdminChange) -> None:
        try:
            uin = await info_mgr.uid_mgr.from_uid(event.uid)
        except ValueError:
            uin = 0
        self._update_cache(self.protocol.impl.cache.on_admin_changed, event.grp_id, uin, event.is_set)

        ev = onebot_events.GroupAdminEvent(
            time=round(time.time()),
            self_id=self.lag.client.uin,
            sub_type="set" if event.is_set else "unset",
            group_id=event.grp_id,
            user_id=uin,
        )
        await self.adapter.trigger(ev)

    @on(FriendRequest)
    async def friend_req_handler(self, _client: Client, event: FriendRequest) -> None:
        if event.from_uid == self.lag.client.uid:
            return
        flag = event.from_uid
        ev = onebot_events.FriendRequestEvent(
            time=round(time.time()),
            self_id=self.lag.client.uin,
            user_id=event.from_uin,
            comment=event.message,
            flag=flag,
        )
        await self.adapter.trigger(ev)

    @on(FriendAddNotify)
    async def friend_add_handler(self, _client: Client, event: FriendAddNotify) -> None:
        if event.status == 3:
            ev = onebot_events.FriendAddEvent(
                time=event.timestamp,
                self_id=self.lag.client.uin,
                user_id=event.from_uin if event.from_uin != self.lag.client.uin else event.to_uin,
            )
            await self.adapter.trigger(ev)
