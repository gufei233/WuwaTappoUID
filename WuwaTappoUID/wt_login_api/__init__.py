"""与 WW 相同的会话页面和提交入口；最终结果由机器人通知。"""

import json
from html import escape
from pathlib import Path

from fastapi import Request
from gsuid_core.webconsole.app_app import app
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy.exc import SQLAlchemyError
from starlette.responses import FileResponse, HTMLResponse, JSONResponse

from ..runtime import login, owner, service
from ..utils.command_text import command_hint, command_text
from ..utils.login_config import SDK_CONFIG
from ..utils.schema import SdkLogin, WuwaError

ASSETS = Path(__file__).parent.parent / "assets"
LOGIN_TEMPLATE = (ASSETS / "login" / "login.html").read_text(encoding="utf-8")
PAGE_HEADERS = {"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"}


class LoginInput(BaseModel):
    auth: str = Field(min_length=1, max_length=128, repr=False)
    login: SdkLogin | None = None


def render_page(auth: str, result: dict) -> HTMLResponse:
    target = result.get("data", {}).get("binding_target", "")
    binding = (
        f"绑定至：{target}。请确认是本人账号，请勿使用他人的链接。"
        if target
        else command_hint("请重新发送 wt登录 获取有效链接。")
    )
    initial = json.dumps(
        {"auth": auth, "sdk_config": SDK_CONFIG, "login_command": command_text("登录"), **result}, ensure_ascii=False
    ).replace("<", "\\u003c")
    page = LOGIN_TEMPLATE
    page = page.replace("__WT_BINDING_TARGET__", escape(binding)).replace("__WT_BOOTSTRAP__", initial)
    return HTMLResponse(page, headers=PAGE_HEADERS)


@app.get("/wuwatappouid/i/{auth}", include_in_schema=False)
async def wuwatappouid_login_page(auth: str):
    try:
        result = await owner.run(login.page_state, auth)
    except WuwaError as error:
        result = {"status": 1, "msg": str(error), "data": {}}
    except SQLAlchemyError as error:
        await service.disable_database(error)
        result = {"status": 1, "msg": service.database_error, "data": {}}
    return render_page(auth, result)


@app.get("/wuwatappouid/assets/{name}", include_in_schema=False)
async def wuwatappouid_login_asset(name: str):
    files = {
        "logo.webp": ASSETS / "tappo" / "logo.webp",
        "bg.jpg": ASSETS / "tappo" / "bg.jpg",
        "tappo.woff2": ASSETS / "tappo" / "tappo.woff2",
        "misans.woff2": ASSETS / "login" / "misans.woff2",
    }
    if name not in files:
        return JSONResponse({"status": 1, "msg": "素材不存在", "data": {}}, status_code=404)
    return FileResponse(files[name], headers={"Cache-Control": "public, max-age=86400"})


@app.post("/wuwatappouid/login", include_in_schema=False)
async def wuwatappouid_login_submit(request: Request):
    try:
        payload = LoginInput.model_validate(await request.json())
    except (ValueError, ValidationError):
        # 不使用默认 422 响应，避免回显 SDK token 和原始输入。
        return JSONResponse(
            {"success": False, "msg": "登录请求参数不正确，请重新打开登录链接。"},
            headers=PAGE_HEADERS,
        )
    try:
        result = await owner.run(login.submit, payload.auth, payload.login)
    except WuwaError as error:
        result = {"success": False, "msg": str(error)}
    except SQLAlchemyError as error:
        await service.disable_database(error)
        result = {"success": False, "msg": service.database_error}
    return JSONResponse(result, headers=PAGE_HEADERS)
