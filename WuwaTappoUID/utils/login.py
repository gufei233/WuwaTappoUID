"""一次性 KrSDK 网页登录；没有后台登录和 SDK 凭据续期。"""

import asyncio
import hashlib
import json
import secrets
import time
from typing import Literal
from urllib.parse import urlsplit

from gsuid_core.aps import scheduler
from gsuid_core.config import core_config
from gsuid_core.logger import logger
from gsuid_core.models import Event
from gsuid_core.utils.database.base_models import DatabaseWriteTimeout
from gsuid_core.utils.database.write_gate import WriteGateTimeout
from pydantic import BaseModel, Field, JsonValue
from sqlalchemy.exc import SQLAlchemyError

from . import login_remote
from .database import LoginSession, Store
from .formatting import mask_cuid
from .login_card import render_login
from .login_store import LoginStore
from .render_cache import render_in_thread
from .schema import CN_SERVER_ID, ApiError, DataError, SdkLogin, UserInfo, WuwaError, token_expiry
from .service import Service, account_id, identity
from .settings import retry_limit, settings

LOGIN_TTL = 300


def session_key(state: str) -> str:
    return hashlib.sha256(state.encode()).hexdigest()


def normalize_login_base(raw: str) -> str:
    # 缺协议时按 https 处理，与 XWUID 的登录地址规则一致；明文 http 须写全 http://
    base = raw.strip().lstrip("/")
    if base and "://" not in base:
        base = "https://" + base
    return base.rstrip("/")


class PendingLoginCheckpoint(BaseModel):
    key: str
    owner_id: str
    account_id: str
    cuid: str
    phase: Literal["token", "verify", "bind", "outcome"] = "token"
    expected_digest: str = ""
    old_token: str = Field(default="", repr=False)
    token: str = Field(default="", repr=False)
    sdk: SdkLogin | None = Field(default=None, repr=False)
    sdk_expires_at: float = 0
    token_expires_at: float = 0
    expiry_error: str = ""
    info: UserInfo | None = None
    outcome_status: str = "success"
    message: str = ""
    result: str = "{}"
    notice_text: str = ""
    expire_account: bool = False
    attempts: int = 0
    retry_after: float = 0


class LoginManager:
    def __init__(self, service: Service) -> None:
        self.service = service
        self._remote_retries: dict[str, tuple[int, float]] = {}
        self._checkpoints: dict[str, PendingLoginCheckpoint] = {}
        self._processing: set[str] = set()

    async def create(self, event: Event) -> list[str | bytes]:
        self.service.require_database()
        if not event.WS_BOT_ID:
            raise WuwaError("当前会话缺少 GS WebSocket 路由，无法保存登录通知目标。")
        use_local = settings.get_config("login_use_local").data
        base = normalize_login_base(settings.get_config("login_public_url").data)
        if not base and use_local:
            host = core_config.get_config("HOST")
            if host in ("0.0.0.0", "::"):
                host = "127.0.0.1"
            if ":" in host and not host.startswith("["):
                host = f"[{host}]"
            base = f"http://{host}:{core_config.get_config('PORT')}"
        address = urlsplit(base)
        # 主机名解析成 http/https 说明协议写错了，例如 http:/host、https//host
        if (
            address.scheme not in ("http", "https")
            or not address.netloc
            or address.hostname in ("http", "https")
            or address.query
            or address.fragment
        ):
            raise WuwaError("主人尚未配置有效的 HTTP/HTTPS 登录页面地址，请暂用 wt添加token。")
        owner = identity(event.bot_id, event.user_id)
        async with self.service.user_lock(owner):
            pending = next((item for item in self._checkpoints.values() if item.owner_id == owner), None)
            if pending is not None:
                pending.retry_after = 0
                self.wake_checkpoint()
                return ["已取得登录结果，正在等待数据库补写；不会重复登录，请稍后查看机器人通知。"]
            opened = await Store.open_logins(owner)
            if any(row.status == "processing" and row.expires_at > time.time() for row in opened):
                raise WuwaError("登录正在处理，请等待结果。")
            state = secrets.token_urlsafe(32)
            remote = None
            if not use_local:
                remote = await login_remote.create(self.service.api, base, event.bot_id, event.user_id)
                state = remote.state
            key = session_key(state)
            async with self.service.subscription(event, "WuwaTappo登录", key) as subscription_id:
                row = LoginSession(
                    id=key,
                    owner_id=owner,
                    platform=event.bot_id,
                    user_id=event.user_id,
                    subscription_id=subscription_id,
                    expires_at=time.time() + LOGIN_TTL,
                )
                if remote is not None:
                    row.expires_at = min(row.expires_at, remote.expires_at / 1000)
                    row.external_url = base
                    row.external_state = state
                    row.external_poll_token = remote.poll_token
                for old in opened:
                    old.status = "revoked"
                    old.message = "该链接已被新的登录链接替代，请使用最新链接。"
                    old.submission = ""
                    old.external_synced = True
                    old.external_poll_token = ""
                    old.external_result_pending = False
                    self._remote_retries.pop(old.id, None)
                await Store.save(*opened, row)
        url = base + "/wuwatappouid/i/" + state
        text = "请在5分钟内打开链接登录本人国服账号，请勿转发。\n" + url
        if not settings.get_config("login_send_qrcode").data:
            return [text]
        try:
            card = await render_in_thread(render_login, url)
        except Exception:
            logger.exception("[WuwaTappoUID] 登录二维码绘制失败，发送文字链接")
            return [text]
        return [card]

    async def get(self, state: str) -> LoginSession:
        return await self.get_key(session_key(state))

    async def get_key(self, key: str) -> LoginSession:
        self.service.require_database()
        row = await Store.login_session(key)
        if row is None:
            raise WuwaError("登录链接无效，请重新发送 wt登录。")
        return row

    async def finish_due(self, row: LoginSession) -> LoginSession:
        if (
            row.id not in self._processing
            and row.id not in self._checkpoints
            and row.expires_at <= time.time()
            and row.status in ("pending", "processing", "verifying")
        ):
            if await Store.finish_login(row.id, time.time()):
                self.wake_notices()
                saved = await Store.login_session(row.id)
                if saved is None:
                    raise WuwaError("登录链接无效，请重新发送 wt登录。")
                return saved
        return row

    @staticmethod
    def response(row: LoginSession) -> dict:
        return {
            "status": 0 if row.status in ("pending", "processing", "verifying", "success") else 1,
            "msg": row.message,
            "data": {
                "state": row.status,
                "expires_at": int(row.expires_at * 1000),
                "binding_target": f"{row.platform} · {mask_cuid(row.user_id)}",
                **json.loads(row.result),
            },
        }

    async def page_state(self, state: str) -> dict:
        row = await self.get(state)
        async with self.service.user_lock(row.owner_id):
            row = await self.finish_due(await self.get(state))
        if row.external_url:
            raise WuwaError("请使用机器人提供的外置登录链接。")
        result = self.response(row)
        if row.submission and row.status in ("pending", "verifying"):
            result["data"]["state"] = "processing"
            result["msg"] = "登录资料已提交，请返回机器人查看结果。"
        return result

    async def submit(self, state: str, sdk: SdkLogin | None) -> dict:
        row = await self.get(state)
        async with self.service.user_lock(row.owner_id):
            row = await self.finish_due(await self.get(state))
            if row.external_url:
                raise WuwaError("请在外置登录页面完成登录。")
            if row.status not in ("pending", "verifying", "processing", "success"):
                raise WuwaError(row.message or "登录链接已失效，请重新发送 wt登录。")
            if row.submission or row.status in ("processing", "success"):
                return {"success": True}
            if row.status == "pending" and sdk is None:
                raise WuwaError("请先登录库洛账号。")
            row.submission = sdk.model_dump_json() if row.status == "pending" else "null"
            await Store.save(row)
        return {"success": True}

    async def complete_key(self, key: str, sdk: SdkLogin | None) -> dict:
        row = await self.get_key(key)
        async with self.service.user_lock(row.owner_id):
            row = await self.finish_due(await self.get_key(key))
            if key in self._checkpoints:
                return self.response(row)
            if row.status not in ("pending", "verifying"):
                return self.response(row)
            self._processing.add(key)
            try:
                if row.status == "pending":
                    if row.submission:
                        sdk = SdkLogin.model_validate_json(row.submission)
                    if sdk is None:
                        raise WuwaError("请先填写手机号和验证码后登录。")
                    row.status = "processing"
                    row.submission = ""
                    await Store.save(row)
                    roles = await self.service.api.login_roles(sdk)
                    role = next(
                        (
                            item
                            for item in roles.items
                            if str(item.playerId).isdigit() and int(item.playerId) > 0 and item.serverId == CN_SERVER_ID
                        ),
                        None,
                    )
                    target = account_id(sdk.cuid, 1)
                    async with self.service.account_lock(target):
                        if row.expires_at <= time.time() or self.service.closing:
                            raise WuwaError("登录链接已到期或插件正在重载，请重新发送 wt登录。")
                        account = await Store.account(target)
                        expected = session_key(account.token) if account is not None and account.token else ""
                        result = await self.service.api.exchange_sdk_token(sdk, role, roles.channelId)
                        checkpoint = PendingLoginCheckpoint(
                            key=key,
                            owner_id=row.owner_id,
                            account_id=target,
                            cuid=sdk.cuid,
                            expected_digest=expected,
                            old_token=account.token if account is not None else "",
                            token=result.token,
                            sdk=sdk,
                            sdk_expires_at=time.time() + sdk.expires_in if sdk.expires_in > 0 else 0,
                        )
                        self._checkpoints[key] = checkpoint
                        try:
                            checkpoint.token_expires_at = token_expiry(result.expire_time)
                        except DataError as error:
                            checkpoint.expiry_error = str(error)
                        await self._continue_checkpoint(checkpoint)
                else:
                    async with self.service.account_lock(row.account_id):
                        account = await Store.account(row.account_id)
                        if account is None or session_key(account.token) != row.token_digest:
                            raise WuwaError("本次登录凭据已被更新，请重新发送 wt登录。")
                        checkpoint = PendingLoginCheckpoint(
                            key=key,
                            owner_id=row.owner_id,
                            account_id=account.id,
                            cuid=account.cuid,
                            phase="verify",
                            token=account.token,
                            token_expires_at=account.token_expires_at,
                        )
                        self._checkpoints[key] = checkpoint
                        await self._continue_checkpoint(checkpoint)
            except (DatabaseWriteTimeout, WriteGateTimeout):
                raise
            except SQLAlchemyError as error:
                await self.service.disable_database(error)
                raise WuwaError(self.service.database_error) from None
            except WuwaError as error:
                existing = self._checkpoints.get(key)
                if existing is not None:
                    await self.service.failures.fail(
                        "login-write:" + key,
                        "登录结果补写",
                        self.service.error_detail(error),
                        existing.account_id,
                        limit=1,
                    )
                    existing.retry_after = time.time() + 900
                    return self.response(row)
                checkpoint = PendingLoginCheckpoint(
                    key=key,
                    owner_id=row.owner_id,
                    account_id=row.account_id,
                    cuid="",
                    phase="outcome",
                    outcome_status="failed",
                    message=str(error),
                    notice_text=str(error) + "\n请重新发送 wt登录。",
                )
                self._checkpoints[key] = checkpoint
                await self._continue_checkpoint(checkpoint)
            except Exception:
                logger.exception("[WuwaTappoUID] 网页登录处理异常，已保留当前结果")
                checkpoint = self._checkpoints.get(key)
                if checkpoint is None:
                    checkpoint = PendingLoginCheckpoint(
                        key=key,
                        owner_id=row.owner_id,
                        account_id=row.account_id,
                        cuid="",
                        phase="outcome",
                        outcome_status="failed",
                        message="登录处理出现异常，请重新发送 wt登录。",
                        notice_text="登录处理出现异常，已记录；请重新发送 wt登录。",
                    )
                    self._checkpoints[key] = checkpoint
                    await self._continue_checkpoint(checkpoint)
                else:
                    checkpoint.retry_after = time.time() + 900
                    await self.service.failures.fail(
                        "login-write:" + key,
                        "登录结果补写",
                        "登录处理程序异常，已保留结果，仅继续补写。",
                        checkpoint.account_id,
                        limit=1,
                    )
            finally:
                self._processing.discard(key)
        saved = await Store.login_session(key)
        return self.response(saved) if saved is not None else self.response(row)

    async def _continue_checkpoint(self, checkpoint: PendingLoginCheckpoint) -> None:
        key = checkpoint.key
        try:
            row = await Store.login_session(key)
            if row is None or row.status in ("revoked", "expired", "failed", "success"):
                self._checkpoints.pop(key, None)
                return
            token_digest = session_key(checkpoint.token) if checkpoint.token else ""
            if checkpoint.phase == "token":
                saved = await LoginStore.token(
                    key,
                    checkpoint.account_id,
                    checkpoint.cuid,
                    checkpoint.expected_digest,
                    checkpoint.token,
                    checkpoint.sdk,
                    checkpoint.sdk_expires_at,
                    checkpoint.token_expires_at,
                )
                if not saved:
                    await self._superseded(checkpoint)
                    return
                self.service.api.retire_token(checkpoint.old_token)
                checkpoint.phase = "verify"
            if checkpoint.phase == "verify":
                account = await Store.account(checkpoint.account_id)
                if account is None or session_key(account.token) != token_digest:
                    await self._superseded(checkpoint)
                    return
                try:
                    info = await self.service.api.me(checkpoint.token, fresh=True)
                    if (
                        str(info.cuid) != checkpoint.cuid
                        or info.is_domestic is not None
                        and info.is_domestic != 1
                        or str(info.server_id) not in ("0", CN_SERVER_ID)
                    ):
                        raise DataError("登录结果的账号或区服不一致，本次未建立绑定。")
                except WuwaError as error:
                    checkpoint.outcome_status = (
                        "failed" if isinstance(error, ApiError) and error.is_auth else "verifying"
                    )
                    if isinstance(error, DataError):
                        checkpoint.outcome_status = "failed"
                    checkpoint.expire_account = isinstance(error, ApiError) and error.is_auth
                    checkpoint.message = str(error)
                    checkpoint.notice_text = str(error) + (
                        "\n请重新打开原链接，点击继续登录。"
                        if checkpoint.outcome_status == "verifying"
                        else "\n请重新发送 wt登录。"
                    )
                    checkpoint.phase = "outcome"
                else:
                    checkpoint.info = info
                    checkpoint.phase = "bind"
            if checkpoint.phase == "bind":
                assert checkpoint.info is not None
                if not await LoginStore.bind(key, token_digest, checkpoint.info, checkpoint.token_expires_at):
                    await self._superseded(checkpoint)
                    return
                account = await Store.account(checkpoint.account_id)
                assert account is not None
                # 绑定已提交后先保留最终结果，后续保存失败不重复恢复任务。
                checkpoint.message = "登录成功，已绑定为当前账号。"
                checkpoint.notice_text = checkpoint.message + "\n任务恢复尚未完成，可发送 wt恢复。"
                checkpoint.result = json.dumps(
                    {
                        "cuid": mask_cuid(account.cuid),
                        "region": "国服",
                        "player_name": checkpoint.info.display_name,
                        "task_note": "任务恢复尚未完成，可发送 wt恢复。",
                    },
                    ensure_ascii=False,
                )
                checkpoint.phase = "outcome"
                await self.service.failures.success("renew:" + account.id)
                try:
                    note = await self.service.resume_after_token(account, checkpoint.info)
                except (DatabaseWriteTimeout, WriteGateTimeout, SQLAlchemyError, WuwaError) as error:
                    note = "登录成功，但任务尚未恢复：" + self.service.error_detail(error) + "；可稍后用 wt恢复。"
                if checkpoint.expiry_error:
                    note += "\n登录态有效期格式异常，已保留新 token：" + checkpoint.expiry_error
                note += (
                    "\n已保存本人的SDK恢复凭据，可在有效期内发送 wt恢复 重新登录。"
                    if account.can_recover(row.owner_id)
                    else "\n本次登录未启用免验证码恢复；使用外置登录时请更新ww-login。"
                )
                checkpoint.message = "登录成功，已绑定为当前账号。"
                checkpoint.result = json.dumps(
                    {
                        "cuid": mask_cuid(account.cuid),
                        "region": "国服",
                        "player_name": checkpoint.info.display_name,
                        "task_note": note,
                    },
                    ensure_ascii=False,
                )
                checkpoint.notice_text = checkpoint.message + "\n" + note
                checkpoint.phase = "outcome"
            if checkpoint.phase == "outcome":
                saved = await LoginStore.outcome(
                    key,
                    token_digest,
                    checkpoint.outcome_status,
                    checkpoint.message,
                    checkpoint.result,
                    checkpoint.notice_text,
                    checkpoint.expire_account,
                )
                if not saved:
                    await self._superseded(checkpoint)
                    return
                self._checkpoints.pop(key, None)
                await self.service.failures.success("login-write:" + key)
                self.wake_notices()
        except (DatabaseWriteTimeout, WriteGateTimeout) as error:
            checkpoint.attempts += 1
            delay = 900 if checkpoint.attempts >= retry_limit() else (60, 300, 900)[min(checkpoint.attempts - 1, 2)]
            checkpoint.retry_after = time.time() + delay
            await self.service.failures.fail(
                "login-write:" + key,
                "登录结果补写",
                f"阶段 {checkpoint.phase}：{type(error).__name__}；已保留结果，仅补写数据库。",
                checkpoint.account_id,
                limit=1,
            )

    async def _superseded(self, checkpoint: PendingLoginCheckpoint) -> None:
        checkpoint.token = ""
        checkpoint.sdk = None
        checkpoint.phase = "outcome"
        checkpoint.outcome_status = "failed"
        checkpoint.message = "本次登录凭据已被更新，本次未覆盖新登录态，请重新发送 wt登录。"
        checkpoint.result = "{}"
        checkpoint.notice_text = checkpoint.message
        checkpoint.expire_account = False
        await LoginStore.outcome(checkpoint.key, "", "failed", checkpoint.message, "{}", checkpoint.message)
        self._checkpoints.pop(checkpoint.key, None)
        self.wake_notices()

    def wake_checkpoint(self) -> None:
        if self.service.closing:
            return
        job = scheduler.get_job("WuwaTappoUID:login-local")
        if job is not None:
            from datetime import datetime, timezone

            job.modify(next_run_time=datetime.now(timezone.utc))

    def wake_notices(self) -> None:
        self.service.wake_notices()

    def export_checkpoints(self) -> list[JsonValue]:
        return [item.model_dump(mode="json") for item in self._checkpoints.values()]

    def import_checkpoints(self, data: JsonValue) -> None:
        if isinstance(data, list):
            self._checkpoints = {
                item.key: item for item in (PendingLoginCheckpoint.model_validate(value) for value in data)
            }

    async def maintain_sessions(self, *, startup: bool = False) -> None:
        protected = tuple(self._processing | self._checkpoints.keys())
        rows = await Store.login_candidates(time.time(), startup=startup, protected_ids=protected)
        for snapshot in rows:
            if self.service.user_lock(snapshot.owner_id).locked():
                continue
            async with self.service.user_lock(snapshot.owner_id):
                if snapshot.id in self._processing or snapshot.id in self._checkpoints:
                    continue
                if await Store.finish_login(snapshot.id, time.time(), startup=startup):
                    self.wake_notices()
                await Store.cleanup_login(snapshot.id, time.time())
        for snapshot in await Store.login_cleanup_candidates(time.time()):
            async with self.service.user_lock(snapshot.owner_id):
                if snapshot.id not in self._checkpoints:
                    await Store.cleanup_login(snapshot.id, time.time())

    async def poll_checkpoints(self) -> None:
        for checkpoint in tuple(self._checkpoints.values()):
            if self.service.closing:
                return
            if checkpoint.retry_after > time.time() or self.service.user_lock(checkpoint.owner_id).locked():
                continue
            if self.service.account_lock(checkpoint.account_id).locked():
                continue
            async with self.service.user_lock(checkpoint.owner_id):
                async with self.service.account_lock(checkpoint.account_id):
                    try:
                        await self._continue_checkpoint(checkpoint)
                    except SQLAlchemyError:
                        raise
                    except Exception as error:
                        if self.service.failures.attempts("login-write:" + checkpoint.key) == 0:
                            logger.exception("[WuwaTappoUID] 登录补写出现程序异常，已保留结果")
                        checkpoint.retry_after = time.time() + 900
                        await self.service.failures.fail(
                            "login-write:" + checkpoint.key,
                            "登录结果补写",
                            self.service.error_detail(error),
                            checkpoint.account_id,
                            limit=1,
                        )

    async def poll_local(self) -> None:
        if not self.service.database_ready or self.service.closing:
            return
        try:
            await self.poll_checkpoints()
            rows = await Store.local_logins()
            results = await asyncio.gather(*(self.poll_local_one(row) for row in rows), return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
        except SQLAlchemyError as error:
            await self.service.disable_database(error)

    async def poll_local_one(self, snapshot: LoginSession) -> None:
        failure_key = "login-local:" + snapshot.id
        if not self.service.failures.allows(failure_key) or self.service.user_lock(snapshot.owner_id).locked():
            return
        try:
            await self.complete_key(snapshot.id, None)
            await self.service.failures.success(failure_key)
            self.wake_notices()
        except SQLAlchemyError:
            raise
        except Exception as error:
            write_timeout = isinstance(error, (DatabaseWriteTimeout, WriteGateTimeout))
            if (
                not write_timeout
                and not isinstance(error, WuwaError)
                and self.service.failures.attempts(failure_key) == 0
            ):
                logger.exception("[WuwaTappoUID] 本地登录会话处理异常")
            phase = "本地登录写入超时" if write_timeout else "本地登录会话处理"
            detail = f"{snapshot.platform} 用户 {mask_cuid(snapshot.user_id)}：{self.service.error_detail(error)}"
            await self.service.failures.fail(failure_key, phase, detail, snapshot.account_id)

    async def poll_external(self) -> None:
        if not self.service.database_ready or self.service.closing:
            return
        try:
            rows = await Store.external_logins()
            results = await asyncio.gather(*(self.poll_one(row) for row in rows), return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
        except SQLAlchemyError as error:
            await self.service.disable_database(error)

    async def poll_one(self, snapshot: LoginSession) -> None:
        # 本地回调或另一轮认证持有用户锁时，不启动第二次认证。
        if self.service.user_lock(snapshot.owner_id).locked():
            return
        attempts, retry_after = self._remote_retries.get(snapshot.id, (0, 0))
        if snapshot.expires_at > time.time() and retry_after > time.time():
            return
        try:
            row = await self.get_key(snapshot.id)
            if row.external_synced:
                self._remote_retries.pop(row.id, None)
                return
            if row.id in self._checkpoints:
                return
            if row.expires_at <= time.time():
                async with self.service.user_lock(row.owner_id):
                    row = await self.finish_due(await self.get_key(row.id))
                    if row.id in self._checkpoints:
                        return
                    # 链接过期停止通信；已经成功的绑定和保存的 token 保留。
                    await LoginStore.expire_external(row.id, time.time())
                self._remote_retries.pop(row.id, None)
                return
            if row.status == "processing":
                return
            if row.status in ("pending", "verifying") and not row.external_result_pending:
                data = await login_remote.poll(self.service.api, row)
                if not data.submitted:
                    self._remote_retries.pop(row.id, None)
                    return
                result = await self.complete_key(row.id, data.login)
                row = await Store.login_session(row.id)
                if row is None or row.id in self._checkpoints:
                    return
                self.wake_notices()
            else:
                result = self.response(row)
            await login_remote.confirm(self.service.api, row, result)
            async with self.service.user_lock(row.owner_id):
                if row.id not in self._checkpoints:
                    await LoginStore.confirm_external(row)
            self._remote_retries.pop(row.id, None)
        except SQLAlchemyError:
            raise
        except Exception as error:
            async with self.service.user_lock(snapshot.owner_id):
                row = await Store.login_session(snapshot.id)
                if row is None or row.id in self._checkpoints:
                    return
                if row.external_synced:
                    self._remote_retries.pop(row.id, None)
                    return
                if row.expires_at <= time.time():
                    # 正常到期不触发后台故障告警，也不撤销已完成的账号绑定。
                    await self.finish_due(row)
                    await LoginStore.expire_external(row.id, time.time())
                    self._remote_retries.pop(row.id, None)
                    return
            detail = self.service.error_detail(error)
            if attempts == 0:
                if isinstance(error, WuwaError):
                    logger.warning(f"[WuwaTappoUID] 外置登录通信失败，将在链接有效期内退避重试：{detail}")
                else:
                    logger.exception("[WuwaTappoUID] 外置登录轮询出现程序异常，将在链接有效期内退避重试")
            delay = (3, 6, 12, 24, 30)[min(attempts, 4)]
            self._remote_retries[row.id] = (attempts + 1, time.time() + delay)
