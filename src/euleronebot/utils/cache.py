import asyncio
import time
from collections.abc import Awaitable, Callable, Hashable
from typing import Any

from ..hyperogger import Logger

__all__ = ["AdaptiveLimiter", "TTLCache"]

logger = Logger.fetch("euler").name_custom("euler.cache")


class AdaptiveLimiter:
    """
    控制向服务器发请求的节奏：两次请求之间至少隔 ``interval`` 秒。

    请求被服务器拒绝（限流）时调用 :meth:`failure`，间隔翻倍（最多 ``max_interval``）；
    请求成功时调用 :meth:`success`，间隔逐步缩回 ``min_interval``。
    """

    def __init__(self, min_interval: float = 0.1, max_interval: float = 3.0):
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.interval = min_interval
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            delay = self._next - now
            if delay > 0:
                await asyncio.sleep(delay)
                now = time.monotonic()
            self._next = now + self.interval

    def success(self) -> None:
        self.interval = max(self.min_interval, self.interval * 0.9)

    def failure(self) -> None:
        self.interval = min(self.max_interval, self.interval * 2)


class _Entry:
    __slots__ = ("value", "ts", "dirty")

    def __init__(self, value: Any, ts: float):
        self.value = value
        self.ts = ts
        self.dirty = False


class TTLCache:
    """
    带过期时间的异步缓存，用来挡住对服务器的重复查询，避免被限流。

    - 没过期：直接返回缓存。
    - 过期或被标记为脏：重新查询；同一个键的并发请求只会发出一次查询。
    - 查询失败（例如服务器限流）：有旧缓存就返回旧缓存，没有才抛出异常。
    - 连续失败达到 ``breaker_failures`` 次后，``breaker_seconds`` 秒内不再向服务器发查询，
      有旧缓存就返回旧缓存，没有就直接失败，免得限流期间不断刷错误。
    - 服务器推送了更新时，用 :meth:`update` 就地修改缓存并重置缓存时间，
      无法就地修改时用 :meth:`mark_dirty` 标记为脏，下次查询时刷新（刷新失败仍然返回旧缓存）。
    """

    def __init__(
        self,
        name: str,
        ttl: float,
        *,
        max_concurrent: int = 4,
        max_stale: float = 24 * 3600,
        max_entries: int | None = None,
        breaker_failures: int = 3,
        breaker_seconds: float = 60.0,
    ):
        self.name = name
        self.ttl = ttl
        self.max_stale = max_stale
        self.max_entries = max_entries
        self.breaker_failures = breaker_failures
        self.breaker_seconds = breaker_seconds
        self._entries: dict[Hashable, _Entry] = {}
        self._inflight: dict[Hashable, asyncio.Task] = {}
        self._sem = asyncio.Semaphore(max_concurrent)
        self._failures = 0
        self._breaker_until = 0.0

    # ---- 查询 ----
    def _warn(self, msg: str) -> None:
        # 日志输出失败（例如控制台编码问题）绝不能影响缓存的回退逻辑
        try:
            logger.warning(msg)
        except Exception:  # noqa: BLE001
            pass

    async def get(self, key: Hashable, loader: Callable[[], Awaitable[Any]], *, force: bool = False) -> Any:
        return (await self.get_ex(key, loader, force=force))[0]

    async def get_ex(
        self, key: Hashable, loader: Callable[[], Awaitable[Any]], *, force: bool = False
    ) -> tuple[Any, bool]:
        """
        返回 (数据, 是否来自缓存)。

        ``force=True`` 绕过缓存（也绕过熔断）：一定向服务器查询并刷新缓存，查询失败就直接抛出异常，
        不会悄悄返回旧缓存——调用方明确要最新的数据。
        """
        now = time.monotonic()
        entry = self._entries.get(key)
        if not force and entry is not None and not entry.dirty and now - entry.ts < self.ttl:
            return entry.value, True

        task = self._inflight.get(key)
        if task is None:
            if not force and now < self._breaker_until:
                # 熔断期间不向服务器发查询
                if entry is not None and now - entry.ts < self.max_stale:
                    return entry.value, True
                raise RuntimeError(f"[{self.name}] 查询暂时被熔断（服务器可能在限流），请稍后再试")
            task = asyncio.ensure_future(self._load(key, loader))
            self._inflight[key] = task
            task.add_done_callback(lambda _t, k=key: self._inflight.pop(k, None))
        try:
            return await asyncio.shield(task), False
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            entry = self._entries.get(key)
            if not force and entry is not None and time.monotonic() - entry.ts < self.max_stale:
                self._warn(f"[{self.name}] 查询失败，返回旧缓存: {e!r}")
                return entry.value, True
            raise

    async def _load(self, key: Hashable, loader: Callable[[], Awaitable[Any]]) -> Any:
        async with self._sem:
            try:
                value = await loader()
            except Exception:
                self._failures += 1
                if self._failures >= self.breaker_failures:
                    self._breaker_until = time.monotonic() + self.breaker_seconds
                    self._failures = 0
                    self._warn(f"[{self.name}] 连续查询失败，{self.breaker_seconds:.0f} 秒内不再向服务器查询")
                raise
        self._failures = 0
        self._entries[key] = _Entry(value, time.monotonic())
        self._evict()
        return value

    def _evict(self) -> None:
        """超过条目数上限时，一次淘汰最旧的 10%，避免每次写入都排序"""
        if self.max_entries is None or len(self._entries) <= self.max_entries:
            return
        drop = max(1, self.max_entries // 10)
        for k in sorted(self._entries, key=lambda x: self._entries[x].ts)[:drop]:
            del self._entries[k]

    # ---- 服务器推送 / 手动维护 ----
    def peek(self, key: Hashable) -> Any | None:
        entry = self._entries.get(key)
        return None if entry is None else entry.value

    def age(self, key: Hashable) -> float | None:
        """这个键的缓存已经多少秒了；没有缓存返回 None"""
        entry = self._entries.get(key)
        return None if entry is None else time.monotonic() - entry.ts

    def keys(self) -> list[Hashable]:
        return list(self._entries)

    def update(self, key: Hashable, fn: Callable[[Any], Any]) -> bool:
        """就地更新缓存并重置缓存时间；缓存里没有这个键就什么也不做，返回是否更新了"""
        entry = self._entries.get(key)
        if entry is None:
            return False
        entry.value = fn(entry.value)
        entry.ts = time.monotonic()
        entry.dirty = False
        return True

    def mark_dirty(self, key: Hashable) -> None:
        entry = self._entries.get(key)
        if entry is not None:
            entry.dirty = True

    def pop(self, key: Hashable) -> None:
        self._entries.pop(key, None)
