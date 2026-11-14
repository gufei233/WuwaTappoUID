"""插件专属表和 GS 数据库会话封装。"""

import hashlib
import json
import time
from uuid import uuid4

from gsuid_core.logger import logger
from gsuid_core.models import Event
from gsuid_core.utils.database import base_models
from gsuid_core.utils.database.base_models import with_read_session, with_session
from gsuid_core.utils.database.models import CoreUser, Subscribe
from gsuid_core.utils.database.startup import exec_list
from sqlalchemy import (
    Column,
    Connection,
    Float,
    Index,
    String,
    Text,
    delete,
    func,
    inspect,
    literal,
    or_,
    text,
    union_all,
    update,
)
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import Field, SQLModel, col, select

from .command_text import command_hint
from .diagnostics import timed
from .schema import DatabaseSchemaError, DataError, OpenResult


def new_id() -> str:
    return uuid4().hex


def login_notice_id(key: str, phase: str) -> str:
    return hashlib.sha256(f"login:{key}:{phase}".encode()).hexdigest()[:32]


class Account(SQLModel, table=True):
    __tablename__ = "wt_account"
    __table_args__ = {"extend_existing": True}
    id: str = Field(primary_key=True, max_length=64)
    cuid: str = Field(max_length=32)
    domestic: int
    token: str = Field(default="", sa_column=Column(Text, nullable=False), repr=False)
    info: str = Field(default="", sa_column=Column(Text, nullable=False))
    verified_at: float = 0
    token_expires_at: float = 0
    token_renew_after: float = 0
    token_state: str = Field(default="ready", max_length=32)
    sdk_token: str = Field(default="", sa_column=Column(Text, nullable=False), repr=False)
    sdk_openid: str = Field(default="", max_length=128)
    sdk_username: str = Field(default="", max_length=128)
    sdk_expires_at: float = 0
    sdk_owner_id: str = Field(default="", max_length=64)
    subscribed: bool = False
    bag_snapshot: str = Field(default="", sa_column=Column(Text, nullable=False), repr=False)

    def clear_sdk(self) -> None:
        self.sdk_token = self.sdk_openid = self.sdk_username = self.sdk_owner_id = ""
        self.sdk_expires_at = 0

    def can_recover(self, owner_id: str) -> bool:
        return bool(
            self.domestic == 1
            and self.sdk_owner_id == owner_id
            and self.sdk_token
            and self.sdk_openid
            and self.sdk_username
            and self.sdk_expires_at > time.time()
        )


class Binding(SQLModel, table=True):
    __tablename__ = "wt_binding"
    __table_args__ = {"extend_existing": True}
    id: str = Field(primary_key=True, max_length=64)
    platform: str = Field(max_length=64)
    user_id: str = Field(max_length=128)
    account_id: str = Field(index=True, max_length=64)


class AccountLink(SQLModel, table=True):
    __tablename__ = "wt_account_link"
    __table_args__ = {"extend_existing": True}
    owner_id: str = Field(primary_key=True, max_length=64)
    account_id: str = Field(primary_key=True, max_length=64)


class LoginSession(SQLModel, table=True):
    __tablename__ = "wt_login_session"
    __table_args__ = {"extend_existing": True}
    id: str = Field(primary_key=True, max_length=64)
    owner_id: str = Field(index=True, max_length=64)
    platform: str = Field(max_length=64)
    user_id: str = Field(max_length=128)
    subscription_id: int = Field(index=True)
    expires_at: float
    status: str = Field(default="pending", max_length=16)
    account_id: str = Field(default="", max_length=64)
    token_digest: str = Field(default="", max_length=64, repr=False)
    message: str = Field(default="", sa_column=Column(Text, nullable=False))
    result: str = Field(default="{}", sa_column=Column(Text, nullable=False))
    submission: str = Field(default="", sa_column=Column(Text, nullable=False), repr=False)

    external_url: str = Field(default="", sa_column=Column(Text, nullable=False))
    external_state: str = Field(default="", max_length=128, repr=False)
    external_poll_token: str = Field(default="", max_length=128, repr=False)
    external_synced: bool = False
    external_result_pending: bool = False


class ChestTask(SQLModel, table=True):
    __tablename__ = "wt_chest_task"
    __table_args__ = {"extend_existing": True}
    account_id: str = Field(primary_key=True, max_length=64)
    run_id: str = Field(max_length=32)
    owner_id: str = Field(max_length=64)
    subscription_id: int = Field(index=True)
    status: str = Field(default="active", max_length=16)
    pause_reason: str = Field(default="", max_length=32)
    started_at: float
    expires_at: float
    next_check: float
    reminded: bool = False
    successes: int = 0
    failures: int = 0
    paused_at: float | None = None

    @property
    def timer_paused(self) -> bool:
        return self.status == "paused" and self.paused_at is not None

    def remaining(self, now: float | None = None) -> float:
        clock = self.paused_at if self.timer_paused else time.time() if now is None else now
        return self.expires_at - clock


class ChestRecord(SQLModel, table=True):
    __tablename__ = "wt_chest_record"
    __table_args__ = (
        Index("ix_wt_chest_record_pending", "state", "notice_id", "account_id"),
        Index("ix_wt_chest_record_recent", "account_id", "run_id", "side", "created_at"),
        Index("ix_wt_chest_record_history", "account_id", "created_at"),
        Index("ix_wt_chest_record_retention", "created_at"),
        Index("ix_wt_chest_record_notice", "notice_id"),
        Index("ix_wt_chest_record_archive", "archive_error", "created_at", "id"),
        {"extend_existing": True},
    )
    id: str = Field(default_factory=new_id, primary_key=True, max_length=32)
    account_id: str = Field(index=True, max_length=64)
    run_id: str = Field(index=True, max_length=32)
    subscription_id: int = Field(index=True)
    side: str = Field(max_length=8)
    created_at: float = Field(default_factory=time.time)
    state: str = Field(default="sending", index=True, max_length=16)
    before_info: str = Field(sa_column=Column(Text, nullable=False))
    result: str = Field(default="", sa_column=Column(Text, nullable=False))
    fresh: bool = False
    supplement: str = Field(default="", sa_column=Column(Text, nullable=False))
    notice_id: str = Field(default="", max_length=32)
    archive_error: str = Field(default="", sa_column=Column(String(160), nullable=False, server_default=""))


class Notice(SQLModel, table=True):
    __tablename__ = "wt_notice"
    __table_args__ = (
        Index("ix_wt_notice_pending", "delivery_state", "created_at", "account_id"),
        Index("ix_wt_notice_finished", "delivery_state", "finished_at"),
        {"extend_existing": True},
    )
    id: str = Field(default_factory=new_id, primary_key=True, max_length=32)
    subscription_id: int = Field(index=True)
    account_id: str = Field(index=True, max_length=64)
    created_at: float = Field(default_factory=time.time)
    text: str = Field(sa_column=Column(Text, nullable=False))
    # 发送时按这两项取物品图；为空就是纯文字通知。
    asset_id: str = Field(default="", max_length=32)
    rarity: str = Field(default="", max_length=32)
    submitted: bool = Field(default=False, index=True)
    submitted_at: float | None = None
    delivery_state: str = Field(
        default="pending", sa_column=Column(String(16), nullable=False, server_default="pending")
    )
    finished_at: float | None = Field(default=None, sa_column=Column(Float, nullable=True))
    new_checked: bool = False
    attempts: int = 0
    last_error: str = Field(default="", max_length=160)
    record_ids: str = Field(default="", sa_column=Column(Text, nullable=False))
    available_clicks: int | None = None


class SynthesisRecord(SQLModel, table=True):
    __tablename__ = "wt_synthesis_record"
    __table_args__ = (Index("ix_wt_synthesis_record_retention", "state", "created_at"), {"extend_existing": True})
    id: str = Field(default_factory=new_id, primary_key=True, max_length=32)
    run_id: str = Field(index=True, max_length=32)
    account_id: str = Field(index=True, max_length=64)
    created_at: float = Field(default_factory=time.time)
    mode: str = Field(max_length=16)
    material_ids: str = Field(sa_column=Column(Text, nullable=False))
    before_quantity: int
    after_quantity: int | None = None
    state: str = Field(default="sending", max_length=16)
    result: str = Field(default="", sa_column=Column(Text, nullable=False))
    error: str = Field(default="", max_length=160)


class ChestSummary(SQLModel, table=True):
    """day=0 为累计归档；其余为北京时间零点，仅保留最近30天的按日归档。"""

    __tablename__ = "wt_chest_summary"
    __table_args__ = (Index("ix_wt_chest_summary_day", "day"), {"extend_existing": True})
    account_id: str = Field(primary_key=True, max_length=64)
    day: int = Field(primary_key=True)
    asset_id: str = Field(primary_key=True, max_length=32)
    rarity: str = Field(primary_key=True, max_length=32)
    result: str = Field(default="", sa_column=Column(Text, nullable=False))
    quantity: int = 0
    clicks: int = 0
    first_at: float


# 按 GS 官方流程注册补列；旧数据使用默认值，不要求回填历史账号关联。
ADDED_COLUMNS = {
    "wt_login_session": {
        "submission": "TEXT NOT NULL DEFAULT ''",
        "external_url": "TEXT NOT NULL DEFAULT ''",
        "external_state": "VARCHAR(128) NOT NULL DEFAULT ''",
        "external_poll_token": "VARCHAR(128) NOT NULL DEFAULT ''",
        "external_synced": "BOOLEAN NOT NULL DEFAULT FALSE",
        "external_result_pending": "BOOLEAN NOT NULL DEFAULT FALSE",
    },
    "wt_account": {
        "sdk_token": "TEXT NOT NULL DEFAULT ''",
        "sdk_openid": "VARCHAR(128) NOT NULL DEFAULT ''",
        "sdk_username": "VARCHAR(128) NOT NULL DEFAULT ''",
        "sdk_expires_at": "FLOAT NOT NULL DEFAULT 0",
        "sdk_owner_id": "VARCHAR(64) NOT NULL DEFAULT ''",
        "subscribed": "BOOLEAN NOT NULL DEFAULT FALSE",
        "token_expires_at": "FLOAT NOT NULL DEFAULT 0",
        "token_renew_after": "FLOAT NOT NULL DEFAULT 0",
        "token_state": "VARCHAR(32) NOT NULL DEFAULT 'ready'",
        "bag_snapshot": "TEXT NOT NULL DEFAULT ''",
    },
    "wt_chest_task": {"paused_at": "FLOAT DEFAULT NULL"},
    # 已有记录不重新推送；新记录由模型显式写入空 notice_id。
    "wt_chest_record": {
        "notice_id": "VARCHAR(32) NOT NULL DEFAULT 'legacy'",
        "fresh": "BOOLEAN NOT NULL DEFAULT FALSE",
        "archive_error": "VARCHAR(160) NOT NULL DEFAULT ''",
    },
    "wt_notice": {
        "asset_id": "VARCHAR(32) NOT NULL DEFAULT ''",
        "rarity": "VARCHAR(32) NOT NULL DEFAULT ''",
        "record_ids": "TEXT NOT NULL DEFAULT ''",
        "available_clicks": "BIGINT DEFAULT NULL",
        "new_checked": "BOOLEAN NOT NULL DEFAULT FALSE",
        "delivery_state": "VARCHAR(16) NOT NULL DEFAULT 'pending'",
        "finished_at": "FLOAT DEFAULT NULL",
    },
}
exec_list.extend(
    f"ALTER TABLE {table} ADD COLUMN {name} {column_type}"
    for table, columns in ADDED_COLUMNS.items()
    for name, column_type in columns.items()
)
PLUGIN_TABLE_NAMES = (
    "wt_account",
    "wt_binding",
    "wt_account_link",
    "wt_login_session",
    "wt_chest_task",
    "wt_chest_record",
    "wt_chest_summary",
    "wt_notice",
    "wt_synthesis_record",
)


def _ensure_schema(connection: Connection) -> list[str]:
    tables = [SQLModel.metadata.tables[name] for name in PLUGIN_TABLE_NAMES]
    # 启动前钩子可能被其他插件的异常跳过，补建仅本插件的表。
    SQLModel.metadata.create_all(connection, tables=tables)
    changes = []
    for table, columns in ADDED_COLUMNS.items():
        existing = {column["name"] for column in inspect(connection).get_columns(table)}
        for name, column_type in columns.items():
            if name not in existing:
                connection.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {column_type}"))
                changes.append(f"新增 {table}.{name}")
    # 0.1.40 首装创建的旧列没有服务端默认值；删除废弃快照列，避免新账号插入触发非空约束。
    account_columns = {column["name"] for column in inspect(connection).get_columns("wt_account")}
    for name in ("role_info", "roles_updated_at"):
        if name in account_columns:
            connection.execute(text(f"ALTER TABLE wt_account DROP COLUMN {name}"))
            changes.append(f"删除 wt_account.{name}")
    missing = []
    inspector = inspect(connection)
    for table in tables:
        existing = {column["name"] for column in inspector.get_columns(table.name)}
        missing.extend(f"{table.name}.{column.name}" for column in table.columns if column.name not in existing)
    if missing:
        raise DatabaseSchemaError(f"插件数据库缺少字段：{'、'.join(missing)}；后台任务不会启动。")
    connection.execute(
        text(
            "UPDATE wt_notice SET delivery_state = 'submitted', "
            "finished_at = COALESCE(submitted_at, created_at) "
            "WHERE submitted = TRUE AND delivery_state = 'pending'"
        )
    )
    connection.execute(
        text(
            "INSERT INTO wt_account_link (owner_id, account_id) "
            "SELECT b.id, b.account_id FROM wt_binding b JOIN wt_account a ON a.id = b.account_id "
            "WHERE NOT EXISTS (SELECT 1 FROM wt_account_link l WHERE l.owner_id = b.id AND l.account_id = b.account_id)"
        )
    )
    connection.execute(
        text(
            "UPDATE wt_account SET token_state = CASE WHEN token = '' THEN 'expired' ELSE 'pending' END "
            "WHERE token_state = 'login_unconfirmed'"
        )
    )
    # create_all 跳过已有表时也跳过新增索引，升级时单独创建。
    for table in tables:
        for index in table.indexes:
            index.create(connection, checkfirst=True)
    return changes


async def ensure_database_schema() -> None:
    async with base_models.engine.begin() as connection:
        changes = await connection.run_sync(_ensure_schema)
    if changes:
        logger.info(f"[WuwaTappoUID] 数据库迁移完成：{'、'.join(changes)}")
    logger.info("[WuwaTappoUID] 数据库表结构校验通过。")


class Store:
    @classmethod
    @with_read_session
    async def login_session(cls, session: AsyncSession, key: str) -> LoginSession | None:
        return await session.get(LoginSession, key)

    @classmethod
    @with_read_session
    async def open_logins(cls, session: AsyncSession, owner_id: str | None = None) -> list[LoginSession]:
        query = select(LoginSession).where(col(LoginSession.status).in_(["pending", "processing", "verifying"]))
        if owner_id is not None:
            query = query.where(col(LoginSession.owner_id) == owner_id)
        return list((await session.execute(query)).scalars().all())

    @classmethod
    @with_read_session
    async def external_logins(cls, session: AsyncSession) -> list[LoginSession]:
        query = select(LoginSession).where(
            col(LoginSession.external_url) != "", col(LoginSession.external_synced).is_(False)
        )
        return list((await session.execute(query)).scalars().all())

    @classmethod
    @with_read_session
    async def local_logins(cls, session: AsyncSession) -> list[LoginSession]:
        query = select(LoginSession).where(col(LoginSession.external_url) == "", col(LoginSession.submission) != "")
        return list((await session.execute(query)).scalars().all())

    @classmethod
    @with_read_session
    async def login_candidates(
        cls, session: AsyncSession, now: float, *, startup: bool = False, protected_ids: tuple[str, ...] = ()
    ) -> list[LoginSession]:
        unfinished = col(LoginSession.status).in_(["pending", "verifying", "processing"])
        due = (LoginSession.expires_at <= now) & unfinished
        if startup:
            due = due | (LoginSession.status == "processing")
        query = select(LoginSession).where(due)
        if protected_ids:
            query = query.where(col(LoginSession.id).not_in(protected_ids))
        result = await session.execute(query.order_by(col(LoginSession.expires_at), col(LoginSession.id)).limit(100))
        return list(result.scalars().all())

    @classmethod
    @with_session
    async def finish_login(cls, session: AsyncSession, key: str, now: float, *, startup: bool = False) -> bool:
        row = await session.get(LoginSession, key)
        if row is None or row.status not in ("pending", "verifying", "processing"):
            return False
        if row.expires_at > now and not (startup and row.status == "processing"):
            return False
        interrupted = row.status == "processing"
        status = "failed" if interrupted else "expired"
        message = (
            command_hint("登录处理已中断，请重新发送 wt登录。")
            if interrupted
            else command_hint("登录链接已过期，请重新发送 wt登录。")
        )
        deadline = LoginSession.expires_at <= now
        if startup:
            deadline = or_(deadline, LoginSession.status == "processing")
        result = await session.execute(
            update(LoginSession)
            .where(LoginSession.id == key, LoginSession.status == row.status, deadline)
            .values(status=status, submission="", message=message, external_result_pending=False)
            .execution_options(synchronize_session=False)
        )
        if not isinstance(result, CursorResult):
            raise TypeError("登录结束更新没有返回数据库写入结果")
        if result.rowcount != 1:
            return False
        notice_id = login_notice_id(key, status)
        if await session.get(Notice, notice_id) is None:
            session.add(
                Notice(id=notice_id, subscription_id=row.subscription_id, account_id=row.account_id, text=message)
            )
        return True

    @classmethod
    @with_read_session
    async def login_cleanup_candidates(cls, session: AsyncSession, now: float) -> list[LoginSession]:
        pending = (
            select(Notice.id)
            .where(Notice.subscription_id == LoginSession.subscription_id, Notice.delivery_state == "pending")
            .correlate(LoginSession)
            .exists()
        )
        result = await session.execute(
            select(LoginSession)
            .where(
                LoginSession.expires_at < now - 24 * 3600,
                col(LoginSession.status).not_in(["pending", "verifying", "processing"]),
                ~pending,
            )
            .order_by(col(LoginSession.expires_at), col(LoginSession.id))
            .limit(100)
        )
        return list(result.scalars().all())

    @classmethod
    @with_session
    async def cleanup_login(cls, session: AsyncSession, key: str, now: float) -> bool:
        row = await session.get(LoginSession, key)
        if row is None or row.expires_at >= now - 24 * 3600 or row.status in ("pending", "verifying", "processing"):
            return False
        pending = await session.scalar(
            select(Notice.id)
            .where(Notice.subscription_id == row.subscription_id, Notice.delivery_state == "pending")
            .limit(1)
        )
        if pending is not None:
            return False
        await session.delete(row)
        return True

    @classmethod
    @with_read_session
    async def statistics(cls, session: AsyncSession) -> tuple[int, int, int]:
        bound_people = await session.scalar(select(func.count()).select_from(Binding))
        task_people = await session.scalar(
            select(func.count(col(ChestTask.owner_id).distinct())).where(
                col(ChestTask.status).in_(["active", "paused"]),
                col(ChestTask.expires_at) > func.coalesce(col(ChestTask.paused_at), time.time()),
            )
        )
        recent = select(func.count()).select_from(ChestRecord).where(col(ChestRecord.state) == "success")
        archived = select(func.coalesce(func.sum(col(ChestSummary.quantity)), 0)).where(col(ChestSummary.day) == 0)
        total_chests = await session.scalar(select(recent.scalar_subquery() + archived.scalar_subquery()))
        return bound_people, task_people, total_chests

    @classmethod
    @with_read_session
    async def operational_statistics(cls, session: AsyncSession) -> dict[str, int | float | None]:
        result: dict[str, int | float | None] = dict.fromkeys(
            ("active", "manual", "auth", "failure", "route", "pending", "unroutable"), 0
        )
        result["oldest"] = None
        tasks = await session.execute(
            select(ChestTask.status, ChestTask.pause_reason, func.count())
            .where(
                col(ChestTask.status).in_(["active", "paused"]),
                ChestTask.expires_at > func.coalesce(ChestTask.paused_at, time.time()),
            )
            .group_by(ChestTask.status, ChestTask.pause_reason)
        )
        for status, reason, count in tasks.all():
            name = "active" if status == "active" else reason
            result[name] = int(result.get(name) or 0) + count
        notices = await session.execute(
            select(Notice.delivery_state, func.count(), func.min(Notice.created_at))
            .where(col(Notice.delivery_state).in_(["pending", "unroutable"]))
            .group_by(Notice.delivery_state)
        )
        for state, count, oldest in notices.all():
            result[state] = count
            if state == "pending":
                result["oldest"] = oldest
        return result

    @classmethod
    @with_session
    async def delete_link(
        cls, session: AsyncSession, owner_id: str, account_id: str, replacement: str | None
    ) -> list[str]:
        account = await session.get(Account, account_id)
        if account is not None and account.sdk_owner_id == owner_id:
            account.clear_sdk()
        link = await session.get(AccountLink, (owner_id, account_id))
        if link is not None:
            await session.delete(link)
        binding = await session.get(Binding, owner_id)
        if binding is not None and binding.account_id == account_id:
            if replacement is None:
                await session.delete(binding)
            else:
                binding.account_id = replacement
        await session.flush()
        links = await session.scalar(
            select(func.count()).select_from(AccountLink).where(col(AccountLink.account_id) == account_id)
        )
        current_bindings = await session.scalar(
            select(func.count()).select_from(Binding).where(col(Binding.account_id) == account_id)
        )
        if not links and not current_bindings:
            account = await session.get(Account, account_id)
            if account is not None:
                account.token = ""
                account.clear_sdk()
                account.token_state = "expired"
                account.token_expires_at = account.token_renew_after = 0
            result = await session.execute(
                select(ChestRecord)
                .where(
                    col(ChestRecord.account_id) == account_id,
                    col(ChestRecord.state) == "success",
                    col(ChestRecord.notice_id) == "",
                )
                .order_by(col(ChestRecord.created_at), col(ChestRecord.id))
            )
            grouped: dict[tuple[str, int], list[ChestRecord]] = {}
            for record in result.scalars().all():
                grouped.setdefault((record.run_id, record.subscription_id), []).append(record)
            changed: list[str] = []
            for (_, subscription_id), records in grouped.items():
                notice = Notice(
                    account_id=account_id,
                    subscription_id=subscription_id,
                    text=f"本次 {len(records)} 个 · 账号已解绑，未判定物品不标 NEW",
                    record_ids=json.dumps([record.id for record in records]),
                    new_checked=True,
                )
                session.add(notice)
                for record in records:
                    record.notice_id = notice.id
                changed.append(notice.id)
            result = await session.execute(
                select(Notice).where(
                    col(Notice.account_id) == account_id,
                    col(Notice.delivery_state) == "pending",
                    col(Notice.new_checked).is_(False),
                    col(Notice.record_ids) != "",
                )
            )
            # 主动解除最后绑定后无法再读取背包；保留已判定的 fresh，不猜测剩余物品的 NEW。
            for notice in result.scalars().all():
                notice.new_checked = True
                notice.text = f"本次 {len(json.loads(notice.record_ids))} 个 · 账号已解绑，未判定物品不标 NEW"
                changed.append(notice.id)
            return changed
        return []

    @classmethod
    @with_read_session
    async def sender_profile(cls, session: AsyncSession, event: Event) -> tuple[str, str]:
        result = await session.execute(
            select(CoreUser)
            .where(col(CoreUser.user_id) == event.user_id, col(CoreUser.bot_id) == event.bot_id)
            .order_by((col(CoreUser.group_id) == event.group_id).desc(), col(CoreUser.id).desc())
            .limit(1)
        )
        row = result.scalars().first()
        if row is None:
            return "", ""
        name = row.user_name if row.user_name not in (None, "1") else ""
        avatar = row.user_icon if row.user_icon not in (None, "1") else ""
        return name, avatar

    @classmethod
    @with_read_session
    async def result_accounts(cls, session: AsyncSession) -> list[str]:
        result = await session.execute(
            select(col(ChestRecord.account_id))
            .where(col(ChestRecord.state) == "success", col(ChestRecord.notice_id) == "")
            .distinct()
        )
        return list(result.scalars().all())

    @classmethod
    @with_read_session
    async def linked_accounts(cls, session: AsyncSession, owner_id: str) -> list[Account]:
        result = await session.execute(
            select(Account)
            .join(AccountLink, col(AccountLink.account_id) == col(Account.id))
            .where(col(AccountLink.owner_id) == owner_id)
            .order_by(col(Account.id))
        )
        return list(result.scalars().all())

    @classmethod
    @with_read_session
    async def pending_results(cls, session: AsyncSession, account_id: str) -> list[ChestRecord]:
        result = await session.execute(
            select(ChestRecord)
            .where(
                col(ChestRecord.account_id) == account_id,
                col(ChestRecord.state) == "success",
                col(ChestRecord.notice_id) == "",
            )
            .order_by(col(ChestRecord.created_at), col(ChestRecord.id))
        )
        return list(result.scalars().all())

    @classmethod
    @with_read_session
    async def records(cls, session: AsyncSession, ids: list[str]) -> list[ChestRecord]:
        result = await session.execute(select(ChestRecord).where(col(ChestRecord.id).in_(ids)))
        rows = {row.id: row for row in result.scalars().all()}
        return [rows[key] for key in ids]

    @classmethod
    @with_session
    async def prepare_new_items(
        cls, session: AsyncSession, account_id: str, notice_ids: list[str], owned: set[str] | None
    ) -> None:
        if owned is None:
            return
        account = await session.get(Account, account_id)
        if account is None:
            raise DataError("开箱通知对应的账号不存在。")
        result = await session.execute(
            select(Notice).where(
                Notice.account_id == account_id,
                col(Notice.id).in_(notice_ids),
                col(Notice.new_checked).is_(False),
                Notice.delivery_state == "pending",
            )
        )
        notices = list(result.scalars().all())
        if not notices:
            return
        ids = {key for notice in notices for key in json.loads(notice.record_ids)}
        result = await session.execute(
            select(ChestRecord)
            .where(ChestRecord.account_id == account_id, col(ChestRecord.id).in_(ids))
            .order_by(col(ChestRecord.created_at), col(ChestRecord.id))
        )
        # 首次通知仅建立基线；积压的多组结果一起比较，避免更新快照后漏掉后续组的NEW。
        new_ids = owned - set(json.loads(account.bag_snapshot)) if account.bag_snapshot else set()
        for record in result.scalars().all():
            asset_id = str(OpenResult.model_validate_json(record.result).asset_id) if record.result else ""
            record.fresh = asset_id in new_ids
            new_ids.discard(asset_id)
            session.add(record)
        account.bag_snapshot = json.dumps(sorted(owned), separators=(",", ":"))
        session.add(account)
        for notice in notices:
            notice.new_checked = True
            session.add(notice)

    @classmethod
    @with_read_session
    async def chest_history(
        cls, session: AsyncSession, account_id: str, since: float, until: float
    ) -> tuple[float | None, list[tuple[str, int, int | None]]]:
        conditions = (col(ChestRecord.account_id) == account_id, col(ChestRecord.state) == "success")
        starts = union_all(
            select(func.min(col(ChestRecord.created_at)).label("first_at")).where(*conditions),
            select(func.min(col(ChestSummary.first_at))).where(
                col(ChestSummary.account_id) == account_id, col(ChestSummary.day) == 0
            ),
        ).subquery()
        first_at = await session.scalar(select(func.min(starts.c.first_at)))
        recent = (
            select(
                col(ChestRecord.result).label("result"), func.count().label("quantity"), literal(None).label("clicks")
            )
            .where(*conditions, col(ChestRecord.created_at) >= since, col(ChestRecord.created_at) <= until)
            .group_by(col(ChestRecord.result))
        )
        archive_days = (
            col(ChestSummary.day) == 0
            if since == 0
            else (col(ChestSummary.day) >= since) & (col(ChestSummary.day) <= until)
        )
        archived = select(col(ChestSummary.result), col(ChestSummary.quantity), col(ChestSummary.clicks)).where(
            col(ChestSummary.account_id) == account_id, archive_days
        )
        # 同一条SELECT同时读取原始流水和归档，归档事务提交前后都不会漏计或重复计数。
        history = union_all(recent, archived).subquery()
        result = await session.execute(
            select(history.c.result, history.c.quantity, history.c.clicks).order_by(history.c.result)
        )
        return first_at, [(payload, count, clicks) for payload, count, clicks in result.all()]

    @classmethod
    @with_read_session
    async def binding(cls, session: AsyncSession, identity: str) -> Binding | None:
        return await session.get(Binding, identity)

    @classmethod
    @with_read_session
    async def account(cls, session: AsyncSession, account_id: str) -> Account | None:
        return await session.get(Account, account_id)

    @classmethod
    @with_read_session
    async def task(cls, session: AsyncSession, account_id: str) -> ChestTask | None:
        return await session.get(ChestTask, account_id)

    @classmethod
    @with_read_session
    async def active_tasks(cls, session: AsyncSession) -> list[ChestTask]:
        result = await session.execute(select(ChestTask).where(col(ChestTask.status).in_(["active", "paused"])))
        return list(result.scalars().all())

    @classmethod
    @with_read_session
    async def unresolved_accounts(cls, session: AsyncSession) -> list[str]:
        result = await session.execute(
            select(col(ChestRecord.account_id)).where(col(ChestRecord.state).in_(["sending", "uncertain"])).distinct()
        )
        return list(result.scalars().all())

    @classmethod
    @with_read_session
    async def unresolved(cls, session: AsyncSession, account_id: str) -> list[ChestRecord]:
        result = await session.execute(
            select(ChestRecord)
            .where(col(ChestRecord.account_id) == account_id, col(ChestRecord.state).in_(["sending", "uncertain"]))
            .order_by(col(ChestRecord.created_at))
        )
        return list(result.scalars().all())

    @classmethod
    @with_read_session
    async def recent_chests(cls, session: AsyncSession, account_id: str, run_id: str, side: str) -> list[ChestRecord]:
        result = await session.execute(
            select(ChestRecord)
            .where(
                col(ChestRecord.account_id) == account_id,
                col(ChestRecord.run_id) == run_id,
                col(ChestRecord.side) == side,
            )
            .order_by(col(ChestRecord.created_at).desc(), col(ChestRecord.id).desc())
            .limit(4)
        )
        return list(result.scalars().all())

    @classmethod
    @with_read_session
    async def notice_accounts(
        cls, session: AsyncSession, until: float, limit: int = 100, after: tuple[float, str] | None = None
    ) -> list[tuple[str, float]]:
        first_at = func.min(col(Notice.created_at))
        query = (
            select(Notice.account_id, first_at)
            .where(Notice.delivery_state == "pending", Notice.created_at <= until)
            .group_by(col(Notice.account_id))
        )
        if after is not None:
            created_at, account_id = after
            query = query.having(
                or_(first_at > created_at, (first_at == created_at) & (Notice.account_id > account_id))
            )
        result = await session.execute(query.order_by(first_at, col(Notice.account_id)).limit(limit))
        return [(account_id, created_at) for account_id, created_at in result.all()]

    @classmethod
    @with_read_session
    async def notices(
        cls, session: AsyncSession, account_id: str | None = None, subscription_id: int | None = None
    ) -> list[Notice]:
        query = select(Notice).where(Notice.delivery_state == "pending")
        if account_id is not None:
            query = query.where(Notice.account_id == account_id)
        if subscription_id is not None:
            query = query.where(Notice.subscription_id == subscription_id)
        result = await session.execute(query.order_by(col(Notice.created_at), col(Notice.id)))
        return list(result.scalars().all())

    @classmethod
    @with_session
    async def mark_unroutable(cls, session: AsyncSession, subscription_id: int) -> list[Notice]:
        subscription = await session.get(Subscribe, subscription_id)
        if subscription is not None and subscription.WS_BOT_ID:
            return []
        result = await session.execute(
            select(Notice).where(Notice.subscription_id == subscription_id, Notice.delivery_state == "pending")
        )
        rows = list(result.scalars().all())
        now = time.time()
        for row in rows:
            row.delivery_state = "unroutable"
            row.finished_at = now
            row.last_error = "通知订阅已删除" if subscription is None else "通知订阅缺少机器人标识"
            session.add(row)
        return rows

    @classmethod
    @timed("database-write", slow=2)
    @with_session
    async def notice_attempts(cls, session: AsyncSession, ids: list[str]) -> None:
        await session.execute(
            update(Notice)
            .where(col(Notice.id).in_(ids), Notice.delivery_state == "pending")
            .values(attempts=col(Notice.attempts) + 1)
            .execution_options(synchronize_session=False)
        )

    @classmethod
    @timed("database-write", slow=2)
    @with_session
    async def mark_submitted(cls, session: AsyncSession, ids: list[str]) -> None:
        now = time.time()
        await session.execute(
            update(Notice)
            .where(col(Notice.id).in_(ids), Notice.delivery_state == "pending")
            .values(submitted=True, submitted_at=now, delivery_state="submitted", finished_at=now, last_error="")
            .execution_options(synchronize_session=False)
        )

    @classmethod
    @timed("database-write", slow=2)
    @with_session
    async def notice_error(cls, session: AsyncSession, ids: list[str], message: str) -> None:
        await session.execute(
            update(Notice)
            .where(col(Notice.id).in_(ids), Notice.delivery_state == "pending")
            .values(last_error=message[:160])
            .execution_options(synchronize_session=False)
        )

    @classmethod
    @with_session
    async def cleanup_subscriptions(
        cls, session: AsyncSession, subscription_ids: list[int] | None = None, limit: int = 100
    ) -> int:
        references = or_(
            *(
                select(model.subscription_id).where(model.subscription_id == Subscribe.id).correlate(Subscribe).exists()
                for model in (ChestTask, ChestRecord, Notice, LoginSession)
            )
        )
        query = select(Subscribe.id).where(
            col(Subscribe.task_name).in_(["WuwaTappo宝箱", "WuwaTappo登录"]), ~references
        )
        if subscription_ids is not None:
            query = query.where(col(Subscribe.id).in_(subscription_ids))
        result = await session.execute(query.order_by(col(Subscribe.id)).limit(limit))
        ids = list(result.scalars().all())
        if ids:
            await session.execute(
                delete(Subscribe).where(col(Subscribe.id).in_(ids)).execution_options(synchronize_session=False)
            )
        return len(ids)

    @classmethod
    @with_read_session
    async def subscription(cls, session: AsyncSession, subscription_id: int) -> Subscribe | None:
        return await session.get(Subscribe, subscription_id)

    @classmethod
    @timed("database-write", slow=2)
    @with_session
    async def save(cls, session: AsyncSession, *rows: SQLModel) -> None:
        # 一次调用内保存任务、开箱结果和待发通知，同一事务提交。
        for row in rows:
            await session.merge(row)
