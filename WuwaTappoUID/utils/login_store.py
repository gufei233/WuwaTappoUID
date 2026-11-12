"""登录结果补写只更新当前阶段字段，不覆盖陈旧账号快照。"""

import hashlib
import time

from gsuid_core.utils.database.base_models import with_session
from sqlalchemy.ext.asyncio import AsyncSession

from .database import Account, AccountLink, Binding, LoginSession, Notice
from .schema import SdkLogin, UserInfo


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class LoginStore:
    @classmethod
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
            account = Account(id=target, cuid=cuid, domestic=1)
        current = digest(account.token) if account.token else ""
        new_digest = digest(token)
        if current not in (expected, new_digest):
            return False
        if current != new_digest:
            account.token = token
            account.token_state = "pending"
            account.token_expires_at = token_expires_at
            account.token_renew_after = 0
            if sdk is not None and sdk_expires_at:
                account.sdk_token = sdk.token
                account.sdk_openid = sdk.sdk_openid
                account.sdk_username = sdk.username
                account.sdk_expires_at = sdk_expires_at
                account.sdk_owner_id = row.owner_id
            else:
                account.clear_sdk()
            session.add(account)
        row.account_id = target
        row.token_digest = new_digest
        row.status = "verifying"
        row.submission = ""
        session.add(row)
        return True

    @classmethod
    @with_session
    async def bind(cls, session: AsyncSession, key: str, token_digest: str, info: UserInfo, expires_at: float) -> bool:
        row = await session.get(LoginSession, key)
        if row is None or row.status != "verifying":
            return False
        account = await session.get(Account, row.account_id)
        if account is None or digest(account.token) != token_digest or row.token_digest != token_digest:
            return False
        account.info = info.model_dump_json()
        account.verified_at = time.time()
        account.token_state = "ready"
        account.token_expires_at = expires_at
        session.add(account)
        await session.merge(Binding(id=row.owner_id, platform=row.platform, user_id=row.user_id, account_id=account.id))
        await session.merge(AccountLink(owner_id=row.owner_id, account_id=account.id))
        return True

    @classmethod
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
        expire_account: bool = False,
    ) -> bool:
        row = await session.get(LoginSession, key)
        if row is None or row.status == "revoked":
            return False
        if row.status in ("success", "failed", "expired"):
            return row.status == status
        if token_digest:
            account = await session.get(Account, row.account_id)
            if account is None or digest(account.token) != token_digest:
                return False
            if expire_account:
                account.token_state = "expired"
                session.add(account)
        row.status = status
        row.message = message
        row.result = result
        row.submission = ""
        row.external_result_pending = bool(row.external_url and row.expires_at > time.time())
        session.add(row)
        notice_id = digest(key + ":" + status + ":" + message)[:32]
        if notice_text and await session.get(Notice, notice_id) is None:
            session.add(
                Notice(id=notice_id, subscription_id=row.subscription_id, account_id=row.account_id, text=notice_text)
            )
        return True
