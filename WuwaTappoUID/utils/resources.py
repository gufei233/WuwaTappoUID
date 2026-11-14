"""名称和完整物品图库：普通更新同步差异，强制更新逐文件覆盖。"""

import asyncio
import hashlib
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import ParamSpec, TypeVar

from gsuid_core.logger import logger
from pydantic import BaseModel, ValidationError

from .api import JSON, Download, WuwaAPI, parse
from .command_text import command_hint
from .item_image import render_tile
from .render_cache import RenderConfig, render_in_thread
from .schema import (
    AssetEntry,
    AssetManifest,
    DataError,
    DesktopCharacter,
    Dictionary,
    Item,
    MaintenanceError,
    WuwaError,
)
from .settings import retry_limit

DEFAULT_HOST = "https://mc-wuwatappo.kurogames-global.com"
CacheModel = TypeVar("CacheModel", bound=BaseModel)
DiskResult = TypeVar("DiskResult")
DiskArgs = ParamSpec("DiskArgs")


class Activity(BaseModel):
    assets_host: str = DEFAULT_HOST


class DictHost(BaseModel):
    url: dict[str, str]


class AssetHost(BaseModel):
    url: str


class CacheHeaders(BaseModel):
    etag: str = ""
    last_modified: str = ""


class ResourceState(BaseModel):
    assets_host: str = DEFAULT_HOST
    headers: dict[str, CacheHeaders] = {}
    sources: dict[str, str] = {}


class Catalog(BaseModel):
    items: list[Item]


class Resources:
    def __init__(self, api: WuwaAPI, root: Path) -> None:
        self.api = api
        self.root = root
        self.catalog_path = root / "catalog_live.json"
        self.catalog: Catalog | None = None
        self.path = root / "dict_zh.json"
        self.manifest_path = root / "appearance_asset.json"
        self.manifest_host_path = root / "appearance_asset_host.json"
        self.state_path = root / "resource_state.json"
        self.raw_dir = root / "item_originals"
        self.image_dir = root / "item_images"
        self.dictionary: Dictionary | None = None
        self.manifest: AssetManifest | None = None
        self.state = ResourceState()
        self.revision = 0
        self._update: asyncio.Task[str] | None = None
        self._update_force = False
        self._needs_retry = True
        self.raw_cache_lock = asyncio.Lock()
        self.image_cache_lock = asyncio.Lock()

    @property
    def assets_host(self) -> str:
        return self.state.assets_host

    async def load(self) -> None:
        state = await self._load_cache(self.state_path, ResourceState)
        if state is not None:
            self.state = state
        self.manifest = await self._load_cache(self.manifest_path, AssetManifest)
        self.dictionary = await self._load_cache(self.path, Dictionary)
        self.catalog = await self._load_cache(self.catalog_path, Catalog)

    @staticmethod
    def _read_cache(path: Path, model: type[CacheModel]) -> CacheModel:
        return model.model_validate_json(path.read_bytes())

    async def _load_cache(self, path: Path, model: type[CacheModel]) -> CacheModel | None:
        if not path.exists():
            return None
        try:
            return await self.disk(self._read_cache, path, model)
        except (OSError, ValidationError):
            logger.warning(f"[WuwaTappoUID] 缓存 {path.name} 无法读取，将在更新资源时重建。")
            return None

    @property
    def incomplete(self) -> bool:
        return self.dictionary is None or self.manifest is None or self.catalog is None or self._needs_retry

    async def update(self, force: bool = False) -> str:
        if self._update is not None and not self._update.done():
            running_force = self._update_force
            result = await asyncio.shield(self._update)
            # 强制请求不能被正在进行的普通更新吞掉，等它结束后再完整执行。
            if force and not running_force:
                return await self.update(force=True)
            return result
        self._update_force = force
        self._update = asyncio.create_task(self._download(force))
        return await asyncio.shield(self._update)

    async def close(self) -> None:
        if self._update is not None and not self._update.done():
            self._update.cancel()
            await asyncio.gather(self._update, return_exceptions=True)

    @staticmethod
    async def disk(
        operation: Callable[DiskArgs, DiskResult], *args: DiskArgs.args, **kwargs: DiskArgs.kwargs
    ) -> DiskResult:
        worker = asyncio.create_task(asyncio.to_thread(operation, *args, **kwargs))
        try:
            return await asyncio.shield(worker)
        except asyncio.CancelledError:
            try:
                await worker
            finally:
                raise

    @staticmethod
    def prune_files(directory: Path, *, keep: set[str] | None = None, limit: int = 128 * 1024 * 1024) -> None:
        if not directory.exists():
            return
        files = []
        cutoff = time.time() - 7 * 86400
        for path in directory.iterdir():
            if not path.is_file() or re.fullmatch(r"[0-9a-f]{64}\.png", path.name) is None:
                continue
            stat = path.stat()
            if keep is not None:
                if path.name not in keep:
                    path.unlink()
            elif stat.st_mtime < cutoff:
                path.unlink()
            else:
                files.append((stat.st_mtime, stat.st_size, path))
        if keep is None:
            size = sum(entry[1] for entry in files)
            for _, length, path in sorted(files):
                if size <= limit:
                    break
                path.unlink()
                size -= length

    async def prune_generated(self) -> None:
        async with self.image_cache_lock:
            await self.disk(self.prune_files, self.image_dir)

    async def _json_file(self, url: str, path: Path, force: bool) -> Download:
        headers = self.state.headers.get(url, CacheHeaders())
        use_cache = not force and path.exists() and self.state.sources.get(path.name) == url
        response = await self.api.download(
            url,
            etag=headers.etag if use_cache else "",
            last_modified=headers.last_modified if use_cache else "",
        )
        if response.not_modified:
            response.body = await self.disk(path.read_bytes)
        return response

    async def _commit_json(self, url: str, path: Path, response: Download) -> None:
        if not response.not_modified:
            await self.disk(self._write_file, path, response.body)
        self.state.headers[url] = CacheHeaders(etag=response.etag, last_modified=response.last_modified)
        self.state.sources[path.name] = url

    async def _download(self, force: bool) -> str:
        self._needs_retry = True
        problems = []
        messages = []
        host = self.assets_host
        try:
            activity = parse(Activity, await self.api.request("/api/common/activity-config", "GET"))
            host = activity.assets_host.rstrip("/") or DEFAULT_HOST
        except MaintenanceError:
            raise
        except WuwaError as error:
            problems.append(f"活动配置未更新：{error} 继续使用已有资源站。")
        try:
            config_url = host + "/app/json-host/dict_host.json"
            config_path = self.root / "dict_host.json"
            response = await self._json_file(config_url, config_path, force)
            config = parse(DictHost, JSON.validate_json(response.body))
            if "zh" not in config.url:
                raise DataError("字典地址配置缺少中文地址。")
            await self._commit_json(config_url, config_path, response)
            url = config.url["zh"]
            response = await self._json_file(url, self.path, force or self.dictionary is None)
            fresh = parse(Dictionary, JSON.validate_json(response.body))
            if not fresh.character:
                raise DataError("远端中文角色字典为空。")
            await self._commit_json(url, self.path, response)
            self.dictionary = fresh
            messages.append("中文名称未变化。" if response.not_modified else "中文名称资源已更新。")
        except (WuwaError, OSError, ValidationError) as error:
            problems.append(self._error("中文名称更新", error))
        try:
            config_url = host + "/app/json-host/appearance_asset_host.json"
            response = await self._json_file(config_url, self.manifest_host_path, force)
            config = parse(AssetHost, JSON.validate_json(response.body))
            await self._commit_json(config_url, self.manifest_host_path, response)
            url = config.url
            response = await self._json_file(url, self.manifest_path, force or self.manifest is None)
            fresh_manifest = parse(AssetManifest, JSON.validate_json(response.body))
            if not fresh_manifest.images():
                raise DataError("远端物品图清单为空。")
            await self._commit_json(url, self.manifest_path, response)
            self.manifest = fresh_manifest
            self.state.assets_host = host
            messages.append(
                f"物品图清单{'未变化' if response.not_modified else '已更新'}（{len(fresh_manifest.images())} 项）。"
            )
        except (WuwaError, OSError, ValidationError) as error:
            problems.append(self._error("物品图清单更新", error))
        if self.manifest is not None:
            try:
                atlas_groups = {
                    "character_skin": (self.manifest.character_skin_atlas, self.manifest.character_skin),
                    "accessory": (self.manifest.accessory_atlas, self.manifest.accessory),
                    "emote": (self.manifest.emote_atlas, self.manifest.emote),
                }
                items = []
                for entry in await self.api.atlas(fresh=True):
                    kind = entry.asset_type.removesuffix("_atlas")
                    if kind not in atlas_groups:
                        raise DataError("图鉴包含未识别的品类，保留原图鉴目录。")
                    mapping, images = atlas_groups[kind]
                    mapped = mapping.get(str(entry.asset_id))
                    if mapped is None or mapped.height_light_id is None:
                        raise DataError("图鉴条目缺少实际物品 ID 映射，保留原图鉴目录。")
                    asset_id = mapped.height_light_id
                    if str(asset_id) not in images:
                        raise DataError("图鉴物品未出现在图片清单中，保留原图鉴目录。")
                    items.append(entry.model_copy(update={"asset_id": asset_id, "asset_type": kind}))
                if not items:
                    raise DataError("远端图鉴为空，保留原图鉴目录。")
                catalog = Catalog(items=items)
                await self.disk(self._write_file, self.catalog_path, catalog.model_dump_json().encode())
                self.catalog = catalog
                messages.append(f"图鉴目录已同步（{len(items)} 项）。")
            except MaintenanceError:
                raise
            except (WuwaError, OSError) as error:
                problems.append(self._error("图鉴目录更新", error))
            updated, skipped, failed, deferred, repaired = await self._sync_images(force)
            messages.append(
                f"物品图：下载 {updated}（损坏修复 {repaired}），跳过 {skipped}，失败 {failed}，待补齐 {deferred}。"
            )
            if failed or deferred:
                problems.append("部分图片下载失败，后续普通更新会继续补齐。")
        if not problems:
            active_urls = set(self.state.sources.values())
            self.state.headers = {url: headers for url, headers in self.state.headers.items() if url in active_urls}
        try:
            await self.disk(self._write_file, self.state_path, self.state.model_dump_json().encode())
        except OSError:
            problems.append("资源索引写入失败，下次更新将重新核对。")
        if not problems and self.manifest is not None:
            urls = (*self.manifest.images().values(), *self.manifest.head_images().values())
            keep = {self._raw_path(self._image_url(relative)).name for relative in urls}
            async with self.raw_cache_lock:
                await self.disk(self.prune_files, self.raw_dir, keep=keep)
        self._needs_retry = bool(problems)
        self.revision += 1
        mode = "强制资源更新" if force else "资源增量更新"
        return mode + ("未全部完成。" if problems else "完成。") + "\n" + "\n".join(messages + problems)

    @staticmethod
    def _error(action: str, error: Exception) -> str:
        if isinstance(error, WuwaError):
            reason = str(error)
        elif isinstance(error, OSError):
            reason = "缓存文件无法读写"
        else:
            reason = "资源不是有效 JSON"
        return f"{action}失败：{reason}。保留已有缓存。"

    def _image_url(self, relative: str) -> str:
        return f"{self.assets_host.rstrip('/')}/{relative.lstrip('/')}"

    def _raw_path(self, url: str) -> Path:
        # 清单 URL 带版本参数；地址或版本变化就对应新文件，未变化的图片直接复用。
        return self.raw_dir / f"{hashlib.sha256(url.encode()).hexdigest()}.png"

    async def _sync_images(self, force: bool) -> tuple[int, int, int, int, int]:
        assert self.manifest is not None
        images = (*self.manifest.images().values(), *self.manifest.head_images().values())
        urls = sorted({self._image_url(relative) for relative in images})
        semaphore = asyncio.Semaphore(4)
        consecutive_failures = 0

        async def sync(url: str) -> str:
            nonlocal consecutive_failures
            async with semaphore:
                path = self._raw_path(url)
                try:
                    damaged = False
                    if not force and path.exists() and path.stat().st_size:
                        try:
                            await self.disk(self._verify_file, path)
                        except DataError:
                            damaged = True
                        else:
                            return "skipped"
                    # 连续失败后不再发起本轮剩余下载；在途请求正常收尾。
                    if consecutive_failures >= retry_limit():
                        return "deferred"
                    raw = await self.api.fetch_bytes(url)
                    await self.disk(self._verify_image, raw)
                    async with self.raw_cache_lock:
                        await self.disk(self._write_file, path, raw)
                    consecutive_failures = 0
                    return "repaired" if damaged else "updated"
                except (WuwaError, OSError, ValueError) as error:
                    consecutive_failures += 1
                    if consecutive_failures == 1:
                        logger.warning(f"[WuwaTappoUID] {self._error('物品图下载', error)}")
                    elif consecutive_failures == retry_limit():
                        logger.warning("[WuwaTappoUID] 资源站连续失败达到上限，本轮剩余图片等待后续补齐。")
                    return "failed"

        results = await asyncio.gather(*(sync(url) for url in urls))
        return (
            results.count("updated") + results.count("repaired"),
            results.count("skipped"),
            results.count("failed"),
            results.count("deferred"),
            results.count("repaired"),
        )

    @staticmethod
    def _verify_file(path: Path) -> None:
        # 文件读写错误向外传播，只有图片内容错误才进入局部修复。
        Resources._verify_image(path.read_bytes())

    @staticmethod
    def _verify_image(raw: bytes) -> None:
        from io import BytesIO

        from PIL import Image

        try:
            with Image.open(BytesIO(raw)) as image:
                image.verify()
        except (OSError, ValueError, SyntaxError):
            raise DataError("远端物品图无法解析。") from None

    @staticmethod
    def _write_file(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(".tmp")
        temp.write_bytes(data)
        temp.replace(path)

    def _character_entry(self, character: DesktopCharacter) -> AssetEntry:
        manifest = self.manifest
        if manifest is None:
            raise DataError(command_hint("桌宠头像资源清单尚未加载，请发送 wt更新资源 后重试。"))
        key = str(character.id)
        if key not in manifest.character_skin and key in manifest.character_skin_atlas:
            key = str(manifest.character_skin_atlas[key].height_light_id)
        entry = manifest.character_skin.get(key)
        if entry is None and self.catalog is not None and character.name:
            name = character.name.removesuffix(".png")
            item = next(
                (
                    item
                    for item in self.catalog.items
                    if item.asset_type == "character_skin"
                    and item.asset_name is not None
                    and item.asset_name.removesuffix(".png") == name
                ),
                None,
            )
            if item is not None:
                entry = manifest.character_skin.get(str(item.asset_id))
        if entry is None:
            raise DataError(command_hint("当前桌宠角色未出现在资源清单中，请发送 wt更新资源 后重试。"))
        return entry

    async def character_head(self, character: DesktopCharacter) -> bytes:
        async with self.raw_cache_lock:
            return await self._character_head(character)

    async def _character_head(self, character: DesktopCharacter) -> bytes:
        """只读本地头像；缺少 head1 时复用按 ID 或素材文件名匹配的本地主图。"""
        entry = self._character_entry(character)
        images = ([entry.head1.url] if entry.head1 is not None else []) + [entry.url]
        for relative in images:
            path = self._raw_path(self._image_url(relative))
            if path.exists():
                raw = await self.disk(path.read_bytes)
                try:
                    await self.disk(self._verify_image, raw)
                except DataError:
                    await self.disk(path.unlink)
                    logger.warning("[WuwaTappoUID] 本地头像缓存损坏，资源更新时重新下载。")
                else:
                    return raw
        raise DataError(command_hint("本地角色图片尚未缓存，请发送 wt更新资源 补齐。"))

    async def item_image(self, asset_id: str | int, rarity: str) -> bytes | None:
        """只读本地原图，推送过程不等待网络下载。"""
        try:
            async with self.raw_cache_lock:
                if self.manifest is None:
                    return None
                relative = self.manifest.lookup(str(asset_id))
                if not relative:
                    return None
                path = self._raw_path(self._image_url(relative))
                if not path.exists():
                    return None
                raw = await self.disk(path.read_bytes)
            # 内容参与缓存键，强制覆盖同一个 URL 后也会重新绘制通知图。
            config = RenderConfig.capture()
            key = hashlib.sha256(raw + rarity.encode() + b"footer-v1" + config.bot_name.encode()).hexdigest()
            cached = self.image_dir / f"{key}.png"
            async with self.image_cache_lock:
                if cached.exists():
                    tile = await self.disk(cached.read_bytes)
                    await self.disk(cached.touch)
                    return tile
                tile = await render_in_thread(render_tile, raw, rarity, config=config)
                await self.disk(self._write_file, cached, tile)
                return tile
        except (OSError, ValueError):
            logger.warning("[WuwaTappoUID] 本地物品图无法读取或绘制，本条只发文字；可强制更新资源修复。")
            return None

    def name(self, raw: str | None, asset_id: str | int) -> str:
        if not raw:
            return f"资源 ID {asset_id}"
        dictionary = self.dictionary
        if dictionary is None:
            return raw
        filename = raw.strip()
        base = re.sub(r"\.png$", "", filename, flags=re.I)
        categories = (dictionary.character, dictionary.series, dictionary.accessory, dictionary.emote)
        for category in categories:
            # 史诗服饰和阿布表情的字典键保留 .png，不能只查询去后缀的名称。
            for key in (filename, base, base + ".png"):
                if key in category:
                    return category[key]
        role = re.fullmatch(r"role_(\d+)_(\d+)", base, re.I)
        emote = re.fullmatch(r"emote_(\d+)_([a-z0-9]+)", base, re.I)
        accessory = re.fullmatch(r"accessory_(\d+)", base, re.I)
        if role or emote:
            match = role or emote
            assert match is not None
            names = dictionary.series if role else dictionary.emote
            # 只命中一部分时保留原名，避免不同资源被展示为同一个名字。
            if match[1] not in dictionary.character or match[2] not in names:
                return raw
            return " · ".join((dictionary.character[match[1]], names[match[2]]))
        if accessory:
            return dictionary.accessory.get(accessory[1], raw)
        return raw
