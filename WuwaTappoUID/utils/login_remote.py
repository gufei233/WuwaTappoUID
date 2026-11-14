"""插件主动轮询 ww-login；浏览器不需要访问 GsCore。"""

import aiohttp
from pydantic import BaseModel, ValidationError

from .api import WuwaAPI
from .database import LoginSession
from .diagnostics import measure
from .schema import DataError, NetworkError, SdkLogin, WuwaError


class RemoteData(BaseModel):
    state: str = ""
    poll_token: str = ""
    expires_at: int = 0
    submitted: bool = False
    login: SdkLogin | None = None


class RemoteResponse(BaseModel):
    status: int
    msg: str
    data: RemoteData


async def request(api: WuwaAPI, base: str, path: str, payload: dict) -> RemoteData:
    with measure("login-http", label=path, debug=True):
        try:
            async with api.client().post(
                base + "/wuwatappouid/" + path,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=12),
            ) as response:
                if response.status >= 500 or response.status in (408, 429):
                    raise NetworkError(f"外置登录服务暂不可用（HTTP {response.status}）。")
                if response.status != 200:
                    raise WuwaError(f"外置登录服务拒绝请求（HTTP {response.status}），请检查服务版本和配置。")
                try:
                    result = RemoteResponse.model_validate_json(await response.text())
                except ValidationError:
                    raise DataError("外置登录服务返回格式不正确，请确认部署了支持 WuwaTappoUID 的 ww-login。") from None
        except (aiohttp.ClientError, TimeoutError):
            raise NetworkError("无法连接外置登录服务，请检查服务地址和网络。") from None
        if result.status != 0:
            raise WuwaError(result.msg)
        return result.data


async def create(api: WuwaAPI, base: str, platform: str, user_id: str) -> RemoteData:
    data = await request(api, base, "token", {"bot_id": platform, "user_id": user_id})
    if not data.state or not data.poll_token or not data.expires_at:
        raise DataError("外置登录服务没有返回有效的一次性会话。")
    return data


async def poll(api: WuwaAPI, row: LoginSession) -> RemoteData:
    return await request(
        api,
        row.external_url,
        "get",
        {
            "state": row.external_state,
            "poll_token": row.external_poll_token,
        },
    )


async def confirm(api: WuwaAPI, row: LoginSession, result: dict) -> None:
    await request(
        api,
        row.external_url,
        "get",
        {
            "state": row.external_state,
            "poll_token": row.external_poll_token,
            "result": result,
        },
    )
