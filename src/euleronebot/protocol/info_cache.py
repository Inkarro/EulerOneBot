from ..onebot.api_data import GetGroupInfoRsp, GetGroupMemberInfoRsp
from ..utils.cache import AdaptiveLimiter, TTLCache

__all__ = ["InfoCache"]

# 各类信息的缓存时间（秒）
GROUP_LIST_TTL = 10 * 60
USER_TTL = 30 * 60
MEMBER_TTL = 15 * 60
FRIEND_LIST_TTL = 10 * 60
FORWARD_TTL = 30 * 60  # 合并转发的内容按 id 固定，不会变
COOKIE_TTL = 5 * 60
FILE_URL_TTL = 5 * 60  # 文件下载链接带有时效，缓存时间要短

GROUP_LIST_KEY = "all"


class InfoCache:
    """
    群列表 / 群信息 / 用户信息 / 群成员信息 / 群成员列表 / 好友列表的缓存。

    键：
      group_list    "all" -> list[GetGroupInfoRsp]（群信息从这里取）
      users         QQ 号 -> UserInfo
      member_list   群号 -> dict[QQ 号, GetGroupMemberInfoRsp]（整个群的成员表，翻页拉全；
                    查单个成员也从这里按 QQ 号取，和 Lagrange 一样不再逐个成员向服务器查询）
      friends       "all" -> list[FriendElem]
    """

    def __init__(self) -> None:
        self.group_list = TTLCache("群列表", GROUP_LIST_TTL, max_concurrent=1)
        self.users = TTLCache("用户信息", USER_TTL, max_concurrent=4, max_entries=20000)
        # 成员表加载失败说明服务器已经连续拒绝了好几次（加载内部有限速和退避重试），熔断 20 秒就够了
        self.member_list = TTLCache("群成员列表", MEMBER_TTL, max_concurrent=2, breaker_seconds=20.0)
        # 群成员翻页请求的节奏：平时每秒约 10 个，被限流就自动减速
        self.member_page_limiter = AdaptiveLimiter(min_interval=0.1, max_interval=3.0)
        self.friends = TTLCache("好友列表", FRIEND_LIST_TTL, max_concurrent=1)
        self.forward = TTLCache("合并转发", FORWARD_TTL, max_concurrent=2, max_entries=200)
        self.cookies = TTLCache("Cookies", COOKIE_TTL, max_concurrent=1, max_entries=50)
        self.csrf = TTLCache("CSRF Token", COOKIE_TTL, max_concurrent=1)
        self.file_urls = TTLCache("文件链接", FILE_URL_TTL, max_concurrent=2, max_entries=500)

    # ---- 服务器推送：就地更新缓存并重置缓存时间 ----
    def _patch_group(self, group_id: int, **changes) -> None:
        def fn(groups: list[GetGroupInfoRsp]) -> list[GetGroupInfoRsp]:
            return [g.model_copy(update=changes) if g.group_id == group_id else g for g in groups]

        self.group_list.update(GROUP_LIST_KEY, fn)

    def _shift_member_count(self, group_id: int, delta: int) -> None:
        def fn(groups: list[GetGroupInfoRsp]) -> list[GetGroupInfoRsp]:
            return [
                g.model_copy(update={"member_count": max(0, g.member_count + delta)}) if g.group_id == group_id else g
                for g in groups
            ]

        self.group_list.update(GROUP_LIST_KEY, fn)

    def on_group_name_changed(self, group_id: int, name: str) -> None:
        self._patch_group(group_id, group_name=name)

    def on_member_joined(self, group_id: int) -> None:
        groups = self.group_list.peek(GROUP_LIST_KEY)
        known = groups is not None and any(g.group_id == group_id for g in groups)
        self._shift_member_count(group_id, 1)  # 注意：update() 会清掉脏标记，所以标记脏必须放在它后面
        if groups is not None and not known:
            # 缓存的群列表里没有这个群：机器人自己刚加入，整个群列表都该刷新
            self.group_list.mark_dirty(GROUP_LIST_KEY)
        # 新成员的资料推送里没有，没法就地补进成员列表，标记为脏，下次查询时刷新
        self.member_list.mark_dirty(group_id)

    def on_member_left(self, group_id: int, user_id: int) -> None:
        self._shift_member_count(group_id, -1)
        self.member_list.update(group_id, lambda ms: {k: v for k, v in ms.items() if k != user_id})

    def on_admin_changed(self, group_id: int, user_id: int, is_set: bool) -> None:
        role = "admin" if is_set else "member"

        def patch(ms: dict[int, GetGroupMemberInfoRsp]) -> dict[int, GetGroupMemberInfoRsp]:
            m = ms.get(user_id)
            # 群主不会被设置 / 取消管理员，保持原样
            if m is None or m.role == "owner":
                return ms
            return {**ms, user_id: m.model_copy(update={"role": role})}

        self.member_list.update(group_id, patch)

    def on_self_group_changed(self) -> None:
        """机器人自己进群 / 被踢：整个群列表都不可信了"""
        self.group_list.mark_dirty(GROUP_LIST_KEY)
