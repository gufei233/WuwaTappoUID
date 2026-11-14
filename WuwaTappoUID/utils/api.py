"""异步 HTTP 与脚本使用的 Engine.IO v4 点击协议。"""

import asyncio
import hashlib
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import wraps
from itertools import count
from typing import ParamSpec, TypeVar

import aiohttp
from gsuid_core.logger import logger
from pydantic import BaseModel, JsonValue, TypeAdapter, ValidationError

from .diagnostics import measure
from .schema import (
    BASE,
    CODE_OK,
    CODE_TOKEN_INVALID,
    ORIGIN,
    UA,
    ApiError,
    DataError,
    Envelope,
    Expired,
    Item,
    LoginData,
    MaintenanceError,
    NetworkError,
    OpenResult,
    OperationStopped,
    RoleInfo,
    RoleQuery,
    SdkLogin,
    Side,
    SynthesisResult,
    UserInfo,
)

T = TypeVar("T", bound=BaseModel)
R = TypeVar("R")
P = ParamSpec("P")
JSON = TypeAdapter(JsonValue)
ITEMS = TypeAdapter(list[Item])
RETRY_DELAYS = (1, 3, 5)
RESOURCE_TIMEOUT = aiohttp.ClientTimeout(total=25, connect=8, sock_read=12)
CLICK_INTERVAL = 30
CLICK_RATE_RETRIES = 3
_READ_VERSIONS = count(1)


@dataclass
class AccountRequests:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    finished_at: float = 0


@dataclass
class ReadCache:
    active: bool = True
    users: dict[tuple[str, int], UserInfo] = field(default_factory=dict)
    bags: dict[tuple[str, int], list[Item]] = field(default_factory=dict)


_read_cache: ContextVar[ReadCache | None] = ContextVar("wt_read_cache", default=None)


def with_read_cache(operation: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    @wraps(operation)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        cache = ReadCache()
        token = _read_cache.set(cache)
        try:
            return await operation(*args, **kwargs)
        finally:
            cache.active = False
            _read_cache.reset(token)

    return wrapped


def network_details(path: str, error: Exception, status: int | None = None, request_id: str = "") -> str:
    if isinstance(error, aiohttp.ClientConnectorCertificateError):
        reason = str(error.certificate_error)
    elif isinstance(error, aiohttp.ClientConnectorError):
        reason = str(error.os_error)
    elif isinstance(error, TimeoutError):
        reason = "请求超时"
    else:
        reason = type(error).__name__
    return f"接口 {path}，HTTP {status}，异常 {type(error).__name__}，原因 {reason}，请求ID {request_id or '未提供'}"


async def retry_network(operation: Callable[[], Awaitable[R]], label: str) -> R:
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            return await operation()
        except NetworkError as error:
            if attempt == len(RETRY_DELAYS):
                logger.warning(f"[WuwaTappoUID] {label}已重试3次仍未完成：{error.details}。")
                raise NetworkError(f"{error}（已重试 3 次）", error.details) from None
            delay = RETRY_DELAYS[attempt]
            if attempt == 0:
                logger.warning(f"[WuwaTappoUID] {label}首次网络失败，将按1/3/5秒重试：{error.details}。")
            await asyncio.sleep(delay)
    raise RuntimeError("网络重试没有产生结果")


@dataclass
class Download:
    body: bytes
    etag: str = ""
    last_modified: str = ""
    not_modified: bool = False


def parse(model: type[T], data: object) -> T:
    try:
        return model.model_validate(data)
    except ValidationError as error:
        fields = "、".join(
            f"{'.'.join(str(part) for part in entry['loc']) or '响应'}:{entry['type']}"
            for entry in error.errors(include_input=False, include_context=False, include_url=False)
        )
        raise DataError(f"{model.__name__}响应字段格式已变化：{fields}。") from None


class WuwaAPI:
    def __init__(self) -> None:
        self.session: aiohttp.ClientSession | None = None
        self._atlas: list[Item] | None = None
        self._atlas_until = 0.0
        self._atlas_lock = asyncio.Lock()
        self._versions: dict[str, int] = {}
        self._requests: dict[str, AccountRequests] = {}
        self._socket_rooms: dict[str, str] = {}
        self._token_users: dict[str, int] = {}
        self._retired_tokens: set[str] = set()
        self.stopping = False
        self.maintenance = False
        self._maintenance_probe_after = 0.0
        self._maintenance_lock = asyncio.Lock()

    def _maintenance_error(self, path: str, request_id: str = "") -> MaintenanceError:
        if not self.maintenance:
            logger.warning("[WuwaTappoUID] 官方服务维护中，暂缓业务请求，每分钟探测恢复。")
        self.maintenance = True
        self._maintenance_probe_after = time.monotonic() + 60
        return MaintenanceError(f"接口 {path}，HTTP 601，请求 ID {request_id or '未提供'}")

    async def ensure_available(self) -> None:
        if not self.maintenance:
            return
        async with self._maintenance_lock:
            if not self.maintenance:
                return
            if time.monotonic() < self._maintenance_probe_after:
                raise MaintenanceError()
            self._maintenance_probe_after = time.monotonic() + 60
            try:
                await self._fetch_once(BASE + "/api/common/activity-config", "GET", None, "")
            except MaintenanceError:
                raise
            except (NetworkError, ApiError, DataError) as error:
                detail = error.details if isinstance(error, (NetworkError, ApiError)) else str(error)
                raise MaintenanceError("维护恢复探测未成功：" + detail) from None

    def account_requests(self, token: str = "", cuid: str = "") -> AccountRequests:
        key = "cuid:" + cuid if cuid else "token:" + self._read_key(token)[0]
        return self._requests.setdefault(key, AccountRequests())

    def remember_account(self, token: str, cuid: str) -> None:
        activity = self.account_requests(cuid=cuid)
        key = "token:" + self._read_key(token)[0]
        previous = self._requests.get(key)
        if previous is not None:
            activity.finished_at = max(activity.finished_at, previous.finished_at)
        self._requests[key] = activity

    @asynccontextmanager
    async def account_request(
        self, token: str = "", cuid: str = "", *, clicks: bool = False, expires_at: float | None = None
    ) -> AsyncIterator[None]:
        if not token and not cuid:
            yield
            return
        activity = self.account_requests(token, cuid)
        async with activity.lock:
            if clicks:
                delay = activity.finished_at + CLICK_INTERVAL - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                if expires_at is not None and time.time() >= expires_at:
                    raise Expired
            try:
                yield
            finally:
                activity.finished_at = time.monotonic()

    def _read_key(self, token: str) -> tuple[str, int]:
        key = hashlib.sha256(token.encode()).hexdigest()
        if key not in self._versions:
            self._versions[key] = next(_READ_VERSIONS)
        return key, self._versions[key]

    def invalidate(self, token: str) -> None:
        key = self._read_key(token)[0]
        self._versions[key] = next(_READ_VERSIONS)

    def retire_token(self, token: str) -> None:
        if token:
            key = hashlib.sha256(token.encode()).hexdigest()
            self._retired_tokens.add(key)
            self._collect_token(key)

    def _collect_token(self, key: str) -> None:
        if key in self._retired_tokens and not self._token_users.get(key, 0):
            self._requests.pop("token:" + key, None)
            self._socket_rooms.pop(key, None)
            self._versions.pop(key, None)
            self._retired_tokens.remove(key)

    @asynccontextmanager
    async def token_use(self, token: str) -> AsyncIterator[None]:
        key = hashlib.sha256(token.encode()).hexdigest()
        self._token_users[key] = self._token_users.get(key, 0) + 1
        try:
            yield
        finally:
            users = self._token_users[key] - 1
            if users:
                self._token_users[key] = users
            else:
                del self._token_users[key]
            self._collect_token(key)

    async def start(self) -> None:
        self.session = aiohttp.ClientSession(
            headers={"Accept": "application/json", "User-Agent": UA, "Origin": ORIGIN, "Referer": ORIGIN + "/"},
            timeout=aiohttp.ClientTimeout(total=25),
            cookie_jar=aiohttp.DummyCookieJar(),
        )

    async def close(self) -> None:
        if self.session is not None:
            await self.session.close()
        self._atlas = None
        self._atlas_until = 0
        self._requests.clear()
        self._socket_rooms.clear()
        self._versions.clear()
        self._retired_tokens.clear()

    def client(self) -> aiohttp.ClientSession:
        if self.session is None:
            raise RuntimeError("WuwaTappo 网络客户端尚未初始化")
        return self.session

    async def fetch(
        self, url: str, method: str = "GET", payload: object = None, token: str = "", *, retry: bool = True
    ) -> Envelope:
        async with self.token_use(token):
            return await self._fetch(url, method, payload, token, retry=retry)

    async def _fetch(self, url: str, method: str, payload: object, token: str, *, retry: bool) -> Envelope:
        async def request() -> Envelope:
            cuid = str(payload["cuid"]) if isinstance(payload, dict) and "cuid" in payload else ""
            async with self.account_request(token, cuid):
                return await self._fetch_once(url, method, payload, token, write=not retry)

        if retry:
            return await retry_network(request, "接口请求")
        try:
            return await request()
        except NetworkError as error:
            logger.warning(f"[WuwaTappoUID] 本次接口请求结果未确认：{error.details}。")
            raise

    async def _fetch_once(self, url: str, method: str, payload: object, token: str, *, write: bool = False) -> Envelope:
        if url != BASE + "/api/common/activity-config":
            await self.ensure_available()
        if write and self.stopping:
            raise OperationStopped
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        path = url.removeprefix(BASE).split("?", 1)[0]
        request_id = ""
        status = None
        with measure("http", label=path, debug=True):
            try:
                async with self.client().request(method, url, json=payload, headers=headers) as response:
                    request_id = response.headers.get("x-request-id", "")
                    status = response.status
                    if status == 601:
                        raise self._maintenance_error(path, request_id)
                    try:
                        envelope = Envelope.model_validate_json(await response.text())
                    except ValidationError:
                        if response.status in (401, 403):
                            raise ApiError(
                                None, http_status=response.status, path=path, request_id=request_id
                            ) from None
                        if response.status >= 500 or response.status in (408, 429):
                            raise NetworkError(
                                f"服务端暂不可用（HTTP {response.status}）。",
                                f"接口 {path}，HTTP {status}，请求ID {request_id or '未提供'}，无有效业务响应",
                            ) from None
                        raise DataError(f"接口 {path} 未返回有效业务响应（HTTP {response.status}）。") from None
                    request_id = envelope.request_id or request_id
                    if envelope.code not in CODE_OK:
                        raise ApiError(envelope.code, http_status=response.status, path=path, request_id=request_id)
                    if response.status in (401, 403):
                        raise ApiError(None, http_status=response.status, path=path, request_id=request_id)
                    if response.status >= 500 or response.status in (408, 429):
                        raise NetworkError(
                            f"服务端暂不可用（HTTP {response.status}），稍后再查询。",
                            f"接口 {path}，HTTP {status}，业务码 {envelope.code}，请求ID {request_id or '未提供'}",
                        )
                    if response.status >= 400:
                        raise DataError(f"请求被拒绝（HTTP {response.status}），请管理员检查接口。")
                    if url == BASE + "/api/common/activity-config" and method == "GET":
                        if self.maintenance:
                            logger.info("[WuwaTappoUID] 官方服务维护结束，恢复业务请求。")
                        self.maintenance = False
                    return envelope
            except (aiohttp.ClientError, TimeoutError) as error:
                raise NetworkError(
                    "网络请求失败或超时，暂时无法确认服务端结果。", network_details(path, error, status, request_id)
                ) from None

    async def download(self, url: str, limit: int = 4_000_000, *, etag: str = "", last_modified: str = "") -> Download:
        async def request() -> Download:
            return await self._download_once(url, limit, etag, last_modified)

        return await retry_network(request, "资源下载")

    async def _download_once(self, url: str, limit: int, etag: str, last_modified: str) -> Download:
        headers = {}
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        try:
            async with self.client().get(url, headers=headers, timeout=RESOURCE_TIMEOUT) as response:
                if response.status == 304:
                    return Download(b"", etag, last_modified, not_modified=True)
                if response.status >= 500 or response.status in (408, 429):
                    raise NetworkError(
                        f"资源服务暂不可用（HTTP {response.status}）。",
                        f"资源 {url.split('?', 1)[0]}，HTTP {response.status}，"
                        f"请求ID {response.headers.get('x-request-id', '未提供')}",
                    )
                if response.status >= 400:
                    raise DataError(f"资源下载被拒绝（HTTP {response.status}）。")
                if response.content_length is not None and response.content_length > limit:
                    raise DataError("资源文件过大，已跳过。")
                body = bytearray()
                async for chunk in response.content.iter_chunked(64 * 1024):
                    if len(body) + len(chunk) > limit:
                        raise DataError("资源文件过大，已跳过。")
                    body.extend(chunk)
                return Download(
                    bytes(body), response.headers.get("ETag", ""), response.headers.get("Last-Modified", "")
                )
        except (aiohttp.ClientError, TimeoutError) as error:
            raise NetworkError("资源下载失败或超时。", network_details(url.split("?", 1)[0], error)) from None

    async def fetch_bytes(self, url: str, limit: int = 4_000_000) -> bytes:
        return (await self.download(url, limit)).body

    async def request(
        self, path: str, method: str = "POST", payload: object = None, token: str = "", *, retry: bool = True
    ) -> JsonValue:
        try:
            response = await self.fetch(BASE + path, method, payload, token, retry=retry)
        except ApiError as error:
            if token and error.is_auth:
                self.invalidate(token)
            logger.warning(f"[WuwaTappoUID] 接口拒绝请求：{error.details}。")
            raise
        return response.payload()

    async def exchange_sdk_token(
        self, login: SdkLogin, role: RoleInfo | None, channel_id: int | str | None
    ) -> LoginData:
        """仅显式网页登录使用真实 KrSDK 凭据；认证请求不自动重发。"""
        payload = {
            "cuid": login.cuid,
            "puid": login.sdk_openid,
            "channelId": channel_id if channel_id is not None else 0,
            "userid": login.sdk_openid,
            "cname": login.username,
            "accessToken": login.token,
            "source": "sdk",
            "playerId": int(role.playerId) if role is not None else 0,
            "serverId": role.serverId if role is not None else "0",
            "is_domestic": 1,
        }
        result = parse(LoginData, await self.request("/api/auth/login", payload=payload, retry=False))
        self.remember_account(result.token, login.cuid)
        return result

    async def login_roles(self, login: SdkLogin) -> RoleQuery:
        payload = {
            "cuid": login.cuid,
            "is_domestic": 1,
            "accessToken": login.token,
            "cname": login.username,
        }
        data = await self.request("/api/auth/player/query", payload=payload)
        return parse(RoleQuery, data)

    async def renew_token(self, token: str) -> None:
        # 续期延长服务端期限，超时不盲目重复；仍保留原 token。
        result = await self.request("/api/frontend/user/renew-token", "GET", token=token, retry=False)
        if result is not True:
            raise DataError("token 续期接口未返回 true，原会话已保留。")

    async def me(self, token: str, *, fresh: bool = False) -> UserInfo:
        async with self.token_use(token):
            return await self._me(token, fresh=fresh)

    async def _me(self, token: str, *, fresh: bool) -> UserInfo:
        cache = _read_cache.get()
        key = self._read_key(token)
        if not fresh and cache is not None and cache.active and key in cache.users:
            return cache.users[key]
        info = parse(UserInfo, await self.request("/api/frontend/user/me", "GET", token=token))
        self.remember_account(token, str(info.cuid))
        self._socket_rooms[key[0]] = str(info.room_no or info.id)
        if cache is not None and cache.active and key == self._read_key(token):
            cache.users[key] = info
        return info

    async def bags(self, token: str, *, fresh: bool = False) -> list[Item]:
        async with self.token_use(token):
            return await self._bags(token, fresh=fresh)

    async def _bags(self, token: str, *, fresh: bool) -> list[Item]:
        cache = _read_cache.get()
        cache_key = self._read_key(token)
        if not fresh and cache is not None and cache.active and cache_key in cache.bags:
            return cache.bags[cache_key]
        data = await self.request("/api/frontend/bags/list", payload={}, token=token)
        if isinstance(data, dict):
            for field_name in ("list", "items", "appearances"):
                if field_name in data and isinstance(data[field_name], list):
                    data = data[field_name]
                    break
        try:
            items = ITEMS.validate_python(data)
        except ValidationError:
            raise DataError("背包响应格式已变化，无法将其当作空背包。") from None
        if cache is not None and cache.active and cache_key == self._read_key(token):
            cache.bags[cache_key] = items
        return items

    async def atlas(self, *, fresh: bool = False) -> list[Item]:
        async with self._atlas_lock:
            if not fresh and self._atlas is not None and time.monotonic() < self._atlas_until:
                return self._atlas
            data = await self.request("/api/common/atlas", "GET")
            try:
                items = ITEMS.validate_python(data)
            except ValidationError:
                raise DataError("图鉴响应格式已变化，无法确定各品类的最高合成阶级。") from None
            self._atlas = items
            self._atlas_until = time.monotonic() + 1800
            return items

    async def redeem(self, token: str, code: str) -> None:
        self.invalidate(token)
        await self.request("/api/frontend/redeems/use", payload={"code": code}, token=token, retry=False)

    async def synthesize(self, token: str, asset_ids: list[int], same_level: bool) -> SynthesisResult:
        self.invalidate(token)
        path = "/api/frontend/assets/synthesis" + ("/same-level" if same_level else "")
        # 材料会被消耗，超时或响应异常时不能盲目重发。
        data = await self.request(path, payload={"asset_ids": asset_ids}, token=token, retry=False)
        return parse(SynthesisResult, data)

    async def open(self, token: str, side: Side, total: int) -> OpenResult:
        self.invalidate(token)
        return parse(
            OpenResult,
            # 开箱由 Service 根据持久化流水确认结果后重试，不能在 HTTP 层直接重发。
            await self.request(
                "/api/frontend/chests/open", payload={"place": side, "click_num": total}, token=token, retry=False
            ),
        )

    async def set_clicks(self, token: str, total: int, expires_at: float | None = None) -> int:
        async with self.token_use(token):
            return await self._set_clicks(token, total, expires_at)

    async def _set_clicks(self, token: str, total: int, expires_at: float | None) -> int:
        """重连时仍提交同一个目标累计值；回读已达到目标就不再发送。"""
        await self.ensure_available()
        key = self._read_key(token)[0]
        if key not in self._socket_rooms:
            info = await self.me(token)
            self._socket_rooms[key] = str(info.room_no or info.id)
        attempted = False
        rate_retries = 0

        async def submit() -> int:
            nonlocal attempted, rate_retries
            if attempted and (await self.me(token, fresh=True)).total_click_count >= total:
                return 0
            if expires_at is not None and time.time() >= expires_at:
                raise Expired
            attempted = True
            while True:
                try:
                    async with self.account_request(token, clicks=True, expires_at=expires_at):
                        code = await self._set_clicks_once(token, total, expires_at)
                    if code == 80013:
                        raise ApiError(code, path="/socket.io:cr")
                    return code
                except ApiError as error:
                    if error.code != 80013 or rate_retries >= CLICK_RATE_RETRIES:
                        raise
                    rate_retries += 1
                    logger.info(
                        f"[WuwaTappoUID] 增加次数返回80013，等待30秒后重试（{rate_retries}/{CLICK_RATE_RETRIES}）。"
                    )

        return await retry_network(submit, "点击连接")

    async def _set_clicks_once(self, token: str, total: int, expires_at: float | None) -> int:
        await self.ensure_available()
        if self.stopping:
            raise OperationStopped
        self.invalidate(token)
        try:
            async with asyncio.timeout(15):
                async with self.client().ws_connect(
                    BASE.replace("https://", "wss://") + "/socket.io/",
                    params={
                        "EIO": "4",
                        "transport": "websocket",
                        "token": token,
                        "rm": self._socket_rooms[self._read_key(token)[0]],
                    },
                    origin=ORIGIN,
                ) as ws:
                    sent_connect = sent_clicks = False
                    async for frame in ws:
                        if frame.type == aiohttp.WSMsgType.ERROR:
                            raise NetworkError("点击连接中断，需重新查询实际点击数。")
                        if frame.type != aiohttp.WSMsgType.TEXT:
                            continue
                        message = frame.data
                        if message == "2":
                            await ws.send_str("3")
                        elif message.startswith("0") and not sent_connect:
                            await ws.send_str("40")
                            sent_connect = True
                        elif message.startswith("40") and not sent_clicks:
                            if expires_at is not None and time.time() >= expires_at:
                                raise Expired
                            if self.stopping:
                                raise OperationStopped
                            await ws.send_str("421" + json.dumps(["cr", {"num": total}], separators=(",", ":")))
                            sent_clicks = True
                        elif message.startswith("42"):
                            event = JSON.validate_json(message[2:])
                            if isinstance(event, list) and len(event) >= 2 and event[0] == "fd":
                                body = event[1]
                                if isinstance(body, dict) and "c" in body and isinstance(body["c"], int):
                                    raise ApiError(body["c"], path="/socket.io:fd")
                        elif message.startswith("43") and sent_clicks:
                            # 部分服务端在 43 后携带 ACK id，正文从 [ 或 { 开始。
                            starts = [i for i in (message.find("["), message.find("{")) if i >= 0]
                            if not starts:
                                raise DataError("点击确认帧缺少正文。")
                            start = min(starts)
                            if message[2:start] != "1":
                                continue
                            body = JSON.validate_json(message[start:])
                            if isinstance(body, list) and body:
                                body = body[0]
                            if not isinstance(body, dict) or "c" not in body or not isinstance(body["c"], int):
                                raise DataError("点击确认帧格式已变化。")
                            code = body["c"]
                            if code in CODE_TOKEN_INVALID:
                                raise ApiError(code, path="/socket.io:cr")
                            return code
        except aiohttp.WSServerHandshakeError as error:
            request_id = error.headers.get("x-request-id", "") if error.headers is not None else ""
            if error.status == 601:
                raise self._maintenance_error("/socket.io:connect", request_id) from None
            if error.status in (401, 403):
                raise ApiError(
                    None, http_status=error.status, path="/socket.io:connect", request_id=request_id
                ) from None
            raise NetworkError(
                f"点击连接握手失败（HTTP {error.status}），需回读实际点击数。",
                network_details("/socket.io:connect", error, error.status, request_id),
            ) from None
        except (aiohttp.ClientError, TimeoutError) as error:
            raise NetworkError(
                "点击提交失败或超时，需重新查询实际点击数。", network_details("/socket.io:cr", error)
            ) from None
        except ValidationError:
            raise DataError("点击确认帧不是有效 JSON。") from None
        raise NetworkError("点击连接已关闭，未收到服务端确认。")
