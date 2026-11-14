"""绘图共用的串行入口、配置快照和按字节计费的图片缓存。"""

import asyncio
from collections import OrderedDict
from collections.abc import Callable, Hashable
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar

from PIL import Image

from .command_text import command_prefix, current_prefix
from .diagnostics import MeasuredLock, measure
from .settings import bot_name, chest_task_hours, max_click_increment, settings

T = TypeVar("T")


@dataclass(frozen=True)
class LocalImage:
    path: Path
    revision: int


@dataclass(frozen=True)
class RenderConfig:
    bot_name: str
    click_limit: int
    task_hours: int
    prefix: str = "wt"

    @classmethod
    def capture(cls) -> "RenderConfig":
        return cls(
            bot_name(),
            settings.get_config("max_click_increment").data,
            settings.get_config("chest_task_hours").data,
            command_prefix(),
        )


current_config: ContextVar[RenderConfig | None] = ContextVar("wt_render_config", default=None)


class ImageCache:
    def __init__(self, limit: int = 64 * 1024 * 1024) -> None:
        self.limit = limit
        self.bytes = 0
        self.items: OrderedDict[Hashable, Image.Image] = OrderedDict()

    def get(self, key: Hashable) -> Image.Image | None:
        image = self.items.get(key)
        if image is not None:
            self.items.move_to_end(key)
        return image

    def put(self, key: Hashable, image: Image.Image) -> Image.Image:
        # Pillow 的 RGB/RGBA 像素缓冲均按四字节计费。
        size = image.width * image.height * (1 if image.mode == "L" else 4)
        old = self.items.pop(key, None)
        if old is not None:
            self.bytes -= old.width * old.height * (1 if old.mode == "L" else 4)
        if size > self.limit:
            return image
        while self.items and self.bytes + size > self.limit:
            _, old = self.items.popitem(last=False)
            self.bytes -= old.width * old.height * (1 if old.mode == "L" else 4)
        self.items[key] = image
        self.bytes += size
        return image


images = ImageCache()
render_lock = MeasuredLock("render-queue")


async def render_in_thread(operation: Callable[..., T], *args, config: RenderConfig | None = None) -> T:
    snapshot = config or RenderConfig.capture()
    async with render_lock:
        token = current_config.set(snapshot)
        prefix_token = current_prefix.set(snapshot.prefix)
        try:
            # 取消调用方时仍等工作线程结束，避免提前释放锁后并行绘制大图。
            with measure("render"):
                worker = asyncio.create_task(asyncio.to_thread(operation, *args))
                try:
                    return await asyncio.shield(worker)
                except asyncio.CancelledError:
                    try:
                        await worker
                    finally:
                        raise
        finally:
            current_prefix.reset(prefix_token)
            current_config.reset(token)


class HelpImageCache:
    def __init__(self) -> None:
        self.key: tuple[str, RenderConfig] | None = None
        self.image: bytes | None = None
        self.lock = asyncio.Lock()

    async def get(self, prefix: str, renderer: Callable[[str], bytes]) -> bytes:
        config = RenderConfig(bot_name(), max_click_increment(), chest_task_hours(), prefix)
        key = (prefix, config)
        async with self.lock:
            if key != self.key:
                image = await render_in_thread(renderer, prefix, config=config)
                self.key, self.image = key, image
            assert self.image is not None
            return self.image
