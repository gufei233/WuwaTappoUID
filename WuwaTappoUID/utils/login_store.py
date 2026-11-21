"""登录结果补写只更新当前阶段字段，不覆盖陈旧账号快照。"""

import hashlib
import json
import time

from gsuid_core.utils.database.base_models import with_session
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .command_text import command_hint
from .database import Account, AccountLink, Binding, LoginSession, Notice
from .diagnostics import timed
from .formatting import mask_cuid
from .schema import SdkLogin, UserInfo


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class PendingCredential(BaseModel):
    cuid: str
    expected_digest: str = ""
    token: str = Field(default="", repr=False)
    token_expires_at: float = 0
    sdk: SdkLogin | None = Field(default=None, repr=False)
    sdk_expires_at: float = 0


class LoginStore:
    @classmethod
    @timed("login-write", slow=2)
    @with_session
    async def confirm_external(cls, session: AsyncSession, expected: LoginSession) -> bool:
        row = await session.get(LoginSession, expected.id)
        if row is None or row.external_synced or row.expires_at <= time.time():
            return False
        # 确认期间可产生新提交或新结果，旧响应只能确认自己发送的那份结果。
        if (
            row.status != expected.status
            or row.account_id != expected.account_id
            or row.token_digest != expected.token_digest
            or row.message != expected.message
            or row.result != expected.result
            or row.submission != expected.submission
            or row.bound != expected.bound
            or row.external_result_pending != expected.external_result_pending
            or row.external_poll_token != expected.external_poll_token
        ):
            return False
        row.external_result_pending = False
        if row.status not in ("pending", "verifying"):
            row.external_synced = True
            row.external_poll_token = ""
        session.add(row)
        return True

    @classmethod
    @with_session
    async def expire_external(cls, session: AsyncSession, key: str, now: float) -> bool:
        row = await session.get(LoginSession, key)
        if row is None or row.expires_at > now:
            return False
        row.external_synced = True
        row.external_poll_token = ""
        row.external_result_pending = False
        session.add(row)
        return True

    @classmethod
    @timed("login-write", slow=2)
    @with_session
    async def token(
        cls,
        session: AsyncSession,
        key: str,
        target: str,
        cuid: str,
        expected: str,
        token: str,
        sdk: SdkLogin | None,
        sdk_expires_at: float,
        token_expires_at: float,
    ) -> bool:
        row = await session.get(LoginSession, key)
        if row is None or row.status not in ("processing", "verifying"):
            return False
        account = await session.get(Account, target)
        if account is None:
            if expected:
                return False
        else:
            current = digest(account.token) if account.token else ""
            if current != expected:
                return False
        new_digest = digest(token)
        row.account_id = target
        row.token_digest = new_digest
        row.status = "verifying"
        row.submission = ""
        row.pending_credential = PendingCredential(
            cuid=cuid,
            expected_digest=expected,
            token=token,
            token_expires_at=token_expires_at,
            sdk=sdk,
            sdk_expires_at=sdk_expires_at,
        ).model_dump_json()
        session.add(row)
        return True

    @classmethod
    @timed("login-write", slow=2)
    @with_session
    async def bind(
        cls, session: AsyncSession, key: str, token_digest: str, info: UserInfo, expires_at: float
    ) -> tuple[bool, str]:
        row = await session.get(LoginSession, key)
        if row is None or row.status != "verifying" or row.token_digest != token_digest:
            return False, ""
        if row.bound:
            # 本会话的绑定事务已提交，重试时仅幂等确认，不再重建绑定避免覆盖后续切换或解绑。
            return True, ""
        if not row.pending_credential:
            # 兼容 0.1.62 升级前已将候选凭据存于 Account 的未绑定待验证会话（原绑定用户刷新可能已将其标为 ready）。
            account = await session.get(Account, row.account_id)
            if (
                account is None
                or digest(account.token) != token_digest
                or account.token_state not in ("pending", "ready")
            ):
                return False, ""
            account.info = info.merge_saved(account.info).model_dump_json()
            account.verified_at = time.time()
            account.token_state = "ready"
            account.token_expires_at = expires_at
            session.add(account)
            await session.merge(
                Binding(id=row.owner_id, platform=row.platform, user_id=row.user_id, account_id=account.id)
            )
            await session.merge(AccountLink(owner_id=row.owner_id, account_id=account.id))
            row.bound = True
            row.message = "登录成功，已绑定为当前账号。"
            row.result = json.dumps(
                {
                    "cuid": mask_cuid(account.cuid),
                    "region": "国服",
                    "player_name": info.display_name,
                    "task_note": command_hint("任务恢复尚未完成，可发送 wt恢复。"),
                },
                ensure_ascii=False,
            )
            session.add(row)
            return True, ""
        cred = PendingCredential.model_validate_json(row.pending_credential)
        if digest(cred.token) != token_digest:
            return False, ""
        account = await session.get(Account, row.account_id)
        old_token = ""
        if account is None:
            if cred.expected_digest:
                return False, ""
            account = Account(id=row.account_id, cuid=cred.cuid, domestic=1)
        else:
            current = digest(account.token) if account.token else ""
            if current not in (cred.expected_digest, token_digest):
                return False, ""
            if current != token_digest:
                old_token = account.token
        account.token = cred.token
        account.token_state = "ready"
        account.token_expires_at = expires_at
        account.token_renew_after = 0
        if cred.sdk is not None and cred.sdk_expires_at:
            account.sdk_token = cred.sdk.token
            account.sdk_openid = cred.sdk.sdk_openid
            account.sdk_username = cred.sdk.username
            account.sdk_expires_at = cred.sdk_expires_at
            account.sdk_owner_id = row.owner_id
        else:
            account.clear_sdk()
        account.info = info.merge_saved(account.info).model_dump_json()
        account.verified_at = time.time()
        session.add(account)
        await session.merge(Binding(id=row.owner_id, platform=row.platform, user_id=row.user_id, account_id=account.id))
        await session.merge(AccountLink(owner_id=row.owner_id, account_id=account.id))
        row.bound = True
        row.pending_credential = ""
        row.message = "登录成功，已绑定为当前账号。"
        row.result = json.dumps(
            {
                "cuid": mask_cuid(account.cuid),
                "region": "国服",
                "player_name": info.display_name,
                "task_note": command_hint("任务恢复尚未完成，可发送 wt恢复。"),
            },
            ensure_ascii=False,
        )
        session.add(row)
        return True, old_token

    @classmethod
    @timed("login-write", slow=2)
    @with_session
    async def outcome(
        cls,
        session: AsyncSession,
        key: str,
        token_digest: str,
        status: str,
        message: str,
        result: str,
        notice_text: str,
    ) -> bool:
        row = await session.get(LoginSession, key)
        if row is None or row.status == "revoked":
            return False
        if row.status in ("success", "failed", "expired"):
            return row.status == status
        if token_digest and row.token_digest != token_digest:
            return False
        row.status = status
        row.message = message
        row.result = result
        row.submission = ""
        if status != "verifying":
            row.pending_credential = ""
            if status == "failed" and token_digest and row.account_id:
                account = await session.get(Account, row.account_id)
                if account is not None and account.token_state == "pending" and digest(account.token) == token_digest:
                    account.token_state = "expired"
                    session.add(account)
        row.external_result_pending = bool(row.external_url and row.expires_at > time.time())
        session.add(row)
        notice_id = digest(key + ":" + status + ":" + message)[:32]
        if notice_text and await session.get(Notice, notice_id) is None:
            session.add(
                Notice(id=notice_id, subscription_id=row.subscription_id, account_id=row.account_id, text=notice_text)
            )
        return True
