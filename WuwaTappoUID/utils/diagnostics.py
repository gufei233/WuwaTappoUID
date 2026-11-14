"""有限的阶段耗时记录；只记录调用方提供的非敏感标签和异常类型。"""

import asyncio
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from typing import ParamSpec, TypeVar

from gsuid_core.logger import logger

P = ParamSpec("P")
R = TypeVar("R")


@dataclass
class StageResult:
    finished_at: float
    seconds: float
    error: str


class Diagnostics:
    def __init__(self) -> None:
        self.stages: dict[str, StageResult] = {}
        self.last_error: tuple[str, str] | None = None


current_diagnostics: ContextVar[Diagnostics | None] = ContextVar("wt_diagnostics", default=None)


@contextmanager
def measure(stage: str, *, label: str = "", slow: float = 5, debug: bool = False) -> Iterator[None]:
    started = time.monotonic()
    error = ""
    try:
        yield
    except BaseException as caught:
        error = type(caught).__name__
        raise
    finally:
        seconds = time.monotonic() - started
        diagnostics = current_diagnostics.get()
        if diagnostics is not None and error != "CancelledError":
            diagnostics.stages[stage] = StageResult(time.time(), seconds, error)
            if error:
                diagnostics.last_error = stage, error
        summary = f"[WuwaTappoUID] 阶段 {stage}{' · ' + label if label else ''} 耗时 {seconds:.2f}s"
        if error:
            summary += f"，异常 {error}"
        if seconds >= slow:
            logger.warning(summary)
        elif debug:
            logger.debug(summary)


def timed(stage: str, *, slow: float = 5) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    def decorate(operation: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @wraps(operation)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            with measure(stage, slow=slow):
                return await operation(*args, **kwargs)

        return wrapped

    return decorate


class MeasuredLock(asyncio.Lock):
    def __init__(self, stage: str) -> None:
        super().__init__()
        self.stage = stage

    async def acquire(self) -> bool:
        with measure(self.stage, slow=2):
            return await super().acquire()
