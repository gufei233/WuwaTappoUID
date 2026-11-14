"""跨插件热重载保存实例归属，等待在途操作收尾后交接。"""

import asyncio
import traceback
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from functools import wraps
from typing import TYPE_CHECKING, ParamSpec, Protocol, TypeVar, runtime_checkable

from gsuid_core.aps import scheduler
from gsuid_core.logger import logger
from gsuid_core.webconsole.app_app import app
from pydantic import JsonValue

from .diagnostics import current_diagnostics
from .schema import WuwaError

if TYPE_CHECKING:
    from .api import WuwaAPI
    from .login import LoginManager
    from .resources import Resources
    from .service import Service

P = ParamSpec("P")
R = TypeVar("R")
_current: ContextVar["RuntimeOwner | None"] = ContextVar("wt_runtime_owner", default=None)


class RuntimeHandle(Protocol):
    generation: int
    accepting: bool
    started: bool
    _start_requested: bool
    initialize: Callable[[], Awaitable[None]]
    start_jobs: Callable[[], Awaitable[None]]
    login: "LoginManager"
    service: "Service"

    def stop(self) -> None: ...
    async def drain(self) -> dict[str, JsonValue]: ...
    async def _run_registered(
        self, operation: Callable[P, Awaitable[R]], *args: P.args, cancellable: bool = False, **kwargs: P.kwargs
    ) -> R: ...


@runtime_checkable
class SlotHandle(Protocol):
    generation: int
    current: RuntimeHandle | None
    latest: RuntimeHandle | None
    activation: asyncio.Task[None] | None
    lock: asyncio.Lock
    transfer: dict[str, JsonValue]


class RuntimeBound(Protocol):
    runtime: "RuntimeOwner"


class RuntimeSlot:
    def __init__(self) -> None:
        self.generation = 0
        self.current: RuntimeHandle | None = None
        self.latest: RuntimeOwner | None = None
        self.activation: asyncio.Task[None] | None = None
        self.lock = asyncio.Lock()
        self.transfer: dict[str, JsonValue] = {}


if not hasattr(app.state, "wuwatappouid_runtime"):
    # 接管 0.1.51 已在同一进程中创建的运行实例。
    if hasattr(app, "extra") and "wuwatappouid_runtime" in app.extra:
        app.state.wuwatappouid_runtime = app.extra["wuwatappouid_runtime"]
    else:
        app.state.wuwatappouid_runtime = RuntimeSlot()
slot = app.state.wuwatappouid_runtime
if not isinstance(slot, SlotHandle):
    raise RuntimeError("WT 生命周期槽格式不正确，请完整重启 GsCore。")


class RuntimeOwner:
    def __init__(self, service: "Service", api: "WuwaAPI", resources: "Resources", login: "LoginManager") -> None:
        self.service, self.api, self.resources, self.login = service, api, resources, login
        slot.generation += 1
        self.generation = slot.generation
        self.accepting = False
        self.started = False
        self._start_requested = False
        self.tasks: set[asyncio.Task[object]] = set()
        self.read_tasks: set[asyncio.Task[object]] = set()
        self._draining: asyncio.Task[dict[str, JsonValue]] | None = None
        self.initialize: Callable[[], Awaitable[None]]
        self.start_jobs: Callable[[], Awaitable[None]]
        if slot.current is not None:
            slot.current.stop()
        slot.latest = self
        service.runtime = self

    @property
    def active(self) -> bool:
        return self.accepting and slot.latest is self

    def stop(self) -> None:
        self.accepting = False
        self.service.closing = True
        self.api.stopping = True
        self.login.stop_workers()
        if slot.current is self:
            for job in scheduler.get_jobs():
                if job.id.startswith("WuwaTappoUID:"):
                    job.remove()
        for task in tuple(self.read_tasks):
            task.cancel()

    async def run(
        self, operation: Callable[P, Awaitable[R]], *args: P.args, cancellable: bool = False, **kwargs: P.kwargs
    ) -> R:
        if _current.get() is self:
            return await operation(*args, **kwargs)
        if not self.active:
            raise WuwaError("插件正在重新加载，请稍后重试。")

        return await self._run_registered(operation, *args, cancellable=cancellable, **kwargs)

    async def _run_registered(
        self, operation: Callable[P, Awaitable[R]], *args: P.args, cancellable: bool = False, **kwargs: P.kwargs
    ) -> R:
        task = self._create_task(operation, *args, cancellable=cancellable, **kwargs)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await asyncio.gather(task, return_exceptions=True)
            raise

    def _create_task(
        self, operation: Callable[P, Awaitable[R]], *args: P.args, cancellable: bool = False, **kwargs: P.kwargs
    ) -> asyncio.Task[R]:
        async def invoke() -> R:
            token = _current.set(self)
            diagnostic_token = current_diagnostics.set(self.service.diagnostics)
            try:
                return await operation(*args, **kwargs)
            finally:
                current_diagnostics.reset(diagnostic_token)
                _current.reset(token)

        task = asyncio.create_task(invoke())
        self.tasks.add(task)
        if cancellable:
            self.read_tasks.add(task)

        def finished(done: asyncio.Task[R]) -> None:
            self.tasks.discard(done)
            self.read_tasks.discard(done)

        task.add_done_callback(finished)
        return task

    def start_background(
        self, operation: Callable[P, Awaitable[R]], *args: P.args, **kwargs: P.kwargs
    ) -> asyncio.Task[R]:
        if not self.active:
            raise WuwaError("插件正在重新加载，请稍后重试。")
        # 后台创建必须显式登记，不能依赖继承的 _current 上下文。
        task = self._create_task(operation, *args, **kwargs)

        def report(done: asyncio.Task[R]) -> None:
            if done.cancelled():
                return
            error = done.exception()
            if error is not None:
                # 只输出源码位置，不格式化异常输入或捕获的局部凭据。
                frames = "".join(traceback.format_tb(error.__traceback__))
                logger.error(f"[WuwaTappoUID] 后台任务异常：{type(error).__name__}\n{frames}")

        task.add_done_callback(report)
        return task

    def entry(self, operation: Callable[P, Awaitable[R]], *, cancellable: bool = False) -> Callable[P, Awaitable[R]]:
        @wraps(operation)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            return await self.run(operation, *args, cancellable=cancellable, **kwargs)

        return wrapped

    async def drain(self) -> dict[str, JsonValue]:
        if self._draining is None:
            self._draining = asyncio.create_task(self._drain())
        return await asyncio.shield(self._draining)

    async def _drain(self) -> dict[str, JsonValue]:
        self.stop()
        if self.tasks:
            pending = asyncio.gather(*tuple(self.tasks), return_exceptions=True)
            try:
                async with asyncio.timeout(60):
                    await asyncio.shield(pending)
            except TimeoutError:
                await self.service.failures.fail("reload", "热重载交接", "旧操作仍未结束，新实例继续等待。", limit=1)
                await pending
        await self.resources.close()
        checkpoints = self.login.export_checkpoints()
        failures = {key: value.model_dump(mode="json") for key, value in self.service.failures._failures.items()}
        await self.api.close()
        self.service.cards._avatars.clear()
        self.service._notice_images.clear()
        from .render_cache import images

        images.items.clear()
        images.bytes = 0
        return {"checkpoints": checkpoints, "failures": failures}

    async def activate(self, *, start: bool = False) -> None:
        self._start_requested = self._start_requested or start
        if slot.current is self and (self.started or not start):
            return
        if slot.activation is None or slot.activation.done():
            slot.activation = asyncio.create_task(_activate_latest())
        await asyncio.shield(slot.activation)
        if start and slot.current is self and not self.started:
            await self.activate(start=True)


async def _activate_latest() -> None:
    async with slot.lock:
        transfer = slot.transfer
        current = slot.current
        latest = slot.latest
        if current is not None and current is latest:
            if latest is not None and latest._start_requested and not latest.started:
                latest.accepting = True
                await latest._run_registered(latest.start_jobs)
                latest.started = True
            if slot.latest is latest:
                return
            current = latest
        if current is not None:
            transfer = await current.drain()
            slot.transfer = transfer
            slot.current = None
        while slot.latest is not None:
            owner = slot.latest
            owner.login.import_checkpoints(transfer.get("checkpoints", []))
            await owner._run_registered(owner.initialize)
            from .failures import Failure

            failure_data = transfer.get("failures", {})
            if isinstance(failure_data, dict):
                owner.service.failures._failures = {
                    **{key: Failure.model_validate(value) for key, value in failure_data.items()},
                    **owner.service.failures._failures,
                }
            if slot.latest is not owner:
                transfer = await owner.drain()
                slot.transfer = transfer
                continue
            slot.current = owner
            if owner._start_requested:
                owner.accepting = True
                await owner._run_registered(owner.start_jobs)
                owner.started = True
            if slot.latest is not owner:
                transfer = await owner.drain()
                slot.transfer = transfer
                slot.current = None
                continue
            slot.transfer = {}
            logger.info(f"[WuwaTappoUID] 运行实例 {owner.generation} 已完成初始化和任务交接。")
            return


def tracked(owner: Callable[[], RuntimeOwner]) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    def decorate(operation: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @wraps(operation)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            return await owner().run(operation, *args, **kwargs)

        return wrapped

    return decorate


def tracked_method(operation: Callable[..., Awaitable[R]]) -> Callable[..., Awaitable[R]]:
    @wraps(operation)
    async def wrapped(self: RuntimeBound, *args: object, **kwargs: object) -> R:
        return await self.runtime.run(operation, self, *args, **kwargs)

    return wrapped
