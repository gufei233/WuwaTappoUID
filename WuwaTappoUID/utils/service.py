"""账号锁、定时宝箱任务、开箱流水与通知提交。"""

import asyncio
import hashlib
import json
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from gsuid_core.aps import scheduler
from gsuid_core.logger import logger
from gsuid_core.models import Event, Message
from gsuid_core.subscribe import gs_subscribe
from gsuid_core.utils.database.base_models import DatabaseWriteTimeout
from gsuid_core.utils.database.write_gate import WriteGateTimeout
from pydantic import BaseModel, JsonValue, ValidationError
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from . import notifications, synthesis_service
from .api import WuwaAPI, with_read_cache
from .backpack import BagFilter
from .cards import Cards, ResultContext
from .command_text import command_hint, command_prefix
from .database import Account, AccountLink, Binding, ChestRecord, ChestTask, Notice, Store, SynthesisRecord, new_id
from .diagnostics import Diagnostics, MeasuredLock, measure
from .failures import Failures
from .formatting import account_text, date_text, mask_cuid, remaining_text, task_text
from .history import HistoryArchive
from .lifecycle import tracked_method
from .notifications import CachedNoticeImage, NoticeSendGate
from .render_common import Progress
from .resources import Resources
from .schema import (
    AUTO_CLICKS,
    CHEST_CLICK_COST,
    CHEST_COOLDOWN,
    CN_SERVER_ID,
    CODE_OK,
    NET_SERVER_IDS,
    RENEW_WINDOW,
    SIDE_NAMES,
    TOKEN_EXPIRED_MESSAGE,
    TOKEN_LIFETIME,
    TOKEN_RENEW_BLOCKED_RETRY,
    TOKEN_RENEW_RETRY,
    TOKEN_RENEW_WINDOW,
    ApiError,
    AuthenticationError,
    ClicksUnavailable,
    DatabaseSchemaError,
    DataError,
    Expired,
    Item,
    MaintenanceError,
    NetworkError,
    OperationStopped,
    SdkLogin,
    Side,
    UserInfo,
    WuwaError,
    token_expiry,
)
from .settings import chest_task_hours, max_click_increment, record_retention_days, retry_limit, settings
from .statistics import CHINA, ChestStatistics, StatisticsPeriod
from .synthesis import SynthesisOptions

if TYPE_CHECKING:
    from .lifecycle import RuntimeOwner


def identity(platform: str, user_id: str) -> str:
    return hashlib.sha256(json.dumps([platform, user_id], ensure_ascii=False).encode()).hexdigest()


def account_id(cuid: str, domestic: int) -> str:
    return f"{domestic}:{cuid}"


class PendingOpen(BaseModel):
    id: str
    account_id: str
    run_id: str
    subscription_id: int
    side: str
    created_at: float
    before_info: str
    result: str
    supplement: str
    info_json: str
    verified_at: float

    def to_record(self) -> ChestRecord:
        return ChestRecord(
            id=self.id,
            account_id=self.account_id,
            run_id=self.run_id,
            subscription_id=self.subscription_id,
            side=self.side,
            created_at=self.created_at,
            state="success",
            before_info=self.before_info,
            result=self.result,
            supplement=self.supplement,
        )


class PendingSynthesis(BaseModel):
    id: str
    run_id: str
    account_id: str
    created_at: float
    mode: str
    material_ids: str
    before_quantity: int
    after_quantity: int | None = None
    state: str = "success"
    result: str = ""
    error: str = ""

    def to_record(self) -> SynthesisRecord:
        return SynthesisRecord(
            id=self.id,
            run_id=self.run_id,
            account_id=self.account_id,
            created_at=self.created_at,
            mode=self.mode,
            material_ids=self.material_ids,
            before_quantity=self.before_quantity,
            after_quantity=self.after_quantity,
            state=self.state,
            result=self.result,
            error=self.error,
        )


class PendingWrites(BaseModel):
    opens: list[PendingOpen] = []
    synthesis: list[PendingSynthesis] = []


class Service:
    runtime: "RuntimeOwner"

    def __init__(self, api: WuwaAPI, resources: Resources) -> None:
        self.api = api
        self.resources = resources
        self.cards = Cards(api, resources)
        self.failures = Failures(resources.root)
        self.diagnostics = Diagnostics()
        self._accounts: dict[str, asyncio.Lock] = {}
        self._users: dict[str, asyncio.Lock] = {}
        self._heavy_users: dict[str, str] = {}
        self._heavy_accounts: dict[str, str] = {}
        self._push_lock = asyncio.Lock()
        self.subscription_lock = asyncio.Lock()
        self._notice_again = False
        self._notice_sends: dict[int, NoticeSendGate] = {}
        self._notice_images: OrderedDict[str, CachedNoticeImage] = OrderedDict()
        self._notice_image_bytes = 0
        self._pending_opens: dict[str, list[PendingOpen]] = {}
        self._pending_synthesis: dict[str, PendingSynthesis] = {}
        self._pending_path = resources.root / "pending-writes.json"
        self._pending_write_lock = asyncio.Lock()
        self._archive_days: int | None = None
        self._archive_next = 0.0
        self.closing = False
        self.database_ready = False
        self.database_error = "插件尚未完成数据库初始化，请稍后重试。"

    def require_database(self) -> None:
        if not self.database_ready:
            raise WuwaError(self.database_error)

    def wake_notices(self, delay: float = 0) -> None:
        if self.closing or not self.runtime.active:
            return
        if self._push_lock.locked():
            self._notice_again = True
            return
        job = scheduler.get_job("WuwaTappoUID:notice")
        if job is not None:
            job.modify(next_run_time=datetime.fromtimestamp(time.time() + delay, timezone.utc))

    @asynccontextmanager
    async def subscription(self, event: Event, name: str, uid: str) -> AsyncIterator[int]:
        if not event.WS_BOT_ID:
            raise WuwaError("当前会话不支持自动推送，请在群聊或私聊中使用。")
        async with self.subscription_lock:
            await gs_subscribe.add_subscribe("single", name, event, uid=uid)
            subscriptions = await gs_subscribe.get_subscribe(name, uid=uid)
            try:
                if len(subscriptions) != 1:
                    raise DataError("推送订阅未能唯一保存，未启动操作。")
                yield subscriptions[0].id
            except (Exception, asyncio.CancelledError):
                # 保存失败时回收孤立路由；保存成功的路由仍有表引用。
                try:
                    await Store.cleanup_subscriptions([sub.id for sub in subscriptions])
                except (SQLAlchemyError, DatabaseWriteTimeout, WriteGateTimeout):
                    logger.error("[WuwaTappoUID] 新建订阅回收失败，后台将重新检查孤立路由。")
                raise

    async def cleanup_subscriptions(self) -> None:
        async with self.subscription_lock:
            await Store.cleanup_subscriptions()

    async def pause_invalid_route(self, task: ChestTask) -> bool:
        sub = await Store.subscription(task.subscription_id)
        if sub is not None and sub.WS_BOT_ID:
            return False
        if task.status == "active":
            task.status = "paused"
            task.pause_reason = "route"
            task.paused_at = None
            task.next_check = task.expires_at
            await Store.save(task)
            self.schedule(task)
        await self.failures.fail(
            f"route:{task.subscription_id}",
            "通知路由失效",
            command_hint("自动开箱已暂停，请由原用户发送 wt恢复 重建路由。"),
            task.account_id,
            limit=1,
        )
        return True

    async def disable_database(self, error: SQLAlchemyError | DatabaseSchemaError) -> None:
        message = "插件数据库初始化或操作失败，后台任务已停止；请更新插件并重启 GsCore。"
        if self.database_error == message:
            return
        self.database_ready = False
        self.database_error = message
        if self.runtime.active:
            for job in scheduler.get_jobs():
                if job.id.startswith("WuwaTappoUID:chest:") or job.id == "WuwaTappoUID:notice":
                    job.remove()
        # 不输出 ORM SQL 或绑定参数，其中可能含 token。
        detail = (
            str(error.orig)
            if isinstance(error, DBAPIError)
            else str(error)
            if isinstance(error, DatabaseSchemaError)
            else type(error).__name__
        )
        logger.error(f"[WuwaTappoUID] {message} {type(error).__name__}：{detail}")
        await self.failures.fail("database", "数据库故障", detail, limit=1)

    @staticmethod
    def error_detail(error: Exception) -> str:
        if isinstance(error, (ApiError, NetworkError, MaintenanceError)):
            return error.details
        return str(error) if isinstance(error, WuwaError) else type(error).__name__

    async def task_failure(self, task: ChestTask, error: Exception) -> None:
        failure = await self.failures.fail(
            "check:" + task.account_id,
            "后台开箱检查",
            self.error_detail(error),
            task.account_id,
        )
        if failure.blocked:
            task.status = "paused"
            task.pause_reason = "failure"
            task.next_check = task.expires_at
            await Store.save(
                task,
                Notice(
                    subscription_id=task.subscription_id,
                    account_id=task.account_id,
                    text=(
                        f"后台任务持续异常（{failure.attempts}次），已暂停并提醒主人检查。\n"
                        f"{self.error_detail(error)}\n问题处理后可用 {command_prefix()}恢复 继续任务。"
                    ),
                ),
            )
        else:
            await Store.save(task)

    def account_lock(self, key: str) -> asyncio.Lock:
        return self._accounts.setdefault(key, MeasuredLock("account-lock"))

    def user_lock(self, key: str) -> asyncio.Lock:
        return self._users.setdefault(key, MeasuredLock("user-lock"))

    @asynccontextmanager
    async def heavy_operation(
        self,
        event: Event,
        name: str,
        on_started: Callable[[], Awaitable[None]] | None = None,
        *,
        request_scope: AsyncExitStack,
    ) -> AsyncIterator[tuple[Binding, Account]]:
        owner_id = identity(event.bot_id, event.user_id)
        busy = self._heavy_users.get(owner_id)
        if busy is not None:
            raise WuwaError(f"已有{busy}请求正在处理，请等待本轮结果。")
        self._heavy_users[owner_id] = name
        # 请求范围包括绘图和回复；互斥锁仅覆盖下方的数据操作。
        request_scope.callback(self._heavy_users.pop, owner_id, None)
        async with self.user_lock(owner_id):
            binding, account = await self.bound(event)
            busy = self._heavy_accounts.get(account.id)
            if busy is not None:
                raise WuwaError(f"该账号正在{busy}，请等待本轮结果。")
            self._heavy_accounts[account.id] = name
            request_scope.callback(self._heavy_accounts.pop, account.id, None)
            async with self.account_lock(account.id):
                if on_started is not None:
                    await on_started()
                yield binding, account

    async def user_binding(self, event: Event) -> Binding:
        binding = await Store.binding(identity(event.bot_id, event.user_id))
        if binding is None:
            raise WuwaError(command_hint("请先发送 wt登录 或 wt添加token TOKEN（海外账号加上海外）。"))
        return binding

    async def bound(self, event: Event, *, allow_expired: bool = False) -> tuple[Binding, Account]:
        binding = await self.user_binding(event)
        account = await Store.account(binding.account_id)
        if account is None:
            raise WuwaError("绑定的账号数据缺失，请重新绑定。")
        if account.token_state == "expired" and not allow_expired:
            raise AuthenticationError(self.expired_message(account, binding.id))
        if account.token_state == "recovering" and not allow_expired:
            raise AuthenticationError(command_hint("新登录凭据已保存，尚待验证；请发送 wt恢复 继续验证。"))
        return binding, account

    @staticmethod
    def expired_message(account: Account, owner_id: str) -> str:
        if account.can_recover(owner_id):
            return (
                f"登录态已失效，发送 {command_prefix()}恢复 可免验证码重登（将顶掉电脑端）。\n"
                f"恢复凭据有效至：{date_text(account.sdk_expires_at)}"
            )
        return command_hint(TOKEN_EXPIRED_MESSAGE)

    async def authentication_message(self, event: Event, error: AuthenticationError) -> str:
        binding = await Store.binding(identity(event.bot_id, event.user_id))
        if binding is not None:
            account = await Store.account(binding.account_id)
            if account is not None and account.token_state == "expired":
                return self.expired_message(account, binding.id)
        return str(error)

    async def expire_account(self, account: Account) -> bool:
        """已确认 token 失效；只通知一次，更新有效 token 后恢复鉴权暂停的任务。返回本次是否生成了失效通知。"""
        was_expired = account.token_state == "expired"
        account.token_state = "expired"
        task = await Store.task(account.id)
        announced = False
        was_active = False
        if task is not None and task.status in ("active", "paused"):
            was_active = task.status == "active"
            if was_active:
                task.status = "paused"
                task.pause_reason = "auth"
                task.next_check = task.expires_at
            rows = [account, task]
            if not was_expired:
                rows.append(
                    Notice(
                        subscription_id=task.subscription_id,
                        account_id=account.id,
                        text=self.expired_message(account, task.owner_id)
                        + (
                            "\n自动开箱已暂停，剩余时长保留。"
                            if task.pause_reason == "auth"
                            else "\n任务维持原暂停状态。"
                        ),
                    )
                )
                announced = True
            await Store.save(*rows)
            self.schedule(task)
        elif not was_expired:
            # 没有运行中的任务时，只在有待判 NEW 的结果要留到登录恢复后才说明一次。
            waiting = sorted(
                {
                    notice.subscription_id
                    for notice in await Store.notices(account.id)
                    if notice.record_ids and not notice.new_checked
                }
            )
            message = self.expired_message(account, task.owner_id if task is not None else "")
            notices = [
                Notice(
                    subscription_id=subscription_id,
                    account_id=account.id,
                    text=f"{message}\n未发送的开箱结果将在登录恢复后补发。",
                )
                for subscription_id in waiting
            ]
            await Store.save(account, *notices)
            announced = bool(notices)

        self.api.retire_token(account.token)
        if was_active:
            try:
                await self.queue_results(account.id, force=True, reason="任务中断，汇总已开结果")
            except (DatabaseWriteTimeout, WriteGateTimeout) as error:
                logger.warning(
                    f"[WuwaTappoUID] 鉴权暂停后暂未能入队开箱汇总，将由后台巡检补发：{self.error_detail(error)}"
                )
        return announced

    async def account_notice(self, account: Account, text: str) -> None:
        task = await Store.task(account.id)
        if task is not None and task.status in ("active", "paused"):
            await Store.save(Notice(subscription_id=task.subscription_id, account_id=account.id, text=text))

    async def account_reply(self, account: Account, task: ChestTask | None, event: Event) -> list[str | bytes]:
        try:
            image = await self.cards.account_summary(account, task, event)
        except SQLAlchemyError:
            raise
        except Exception:
            logger.exception("[WuwaTappoUID] 账号数据已取得，图片绘制失败，返回文字")
        else:
            return [image]
        info = UserInfo.model_validate_json(account.info)
        summary = account_text(info, account.domestic)
        return ["账号数据已取得，图片暂不可用。\n" + summary + "\n" + task_text(task)]

    async def refresh(self, account: Account, *, fresh: bool = False, force_remote: bool = False) -> UserInfo:
        # 调用方进入账号锁前可能已读取绑定，锁内以数据库中最新 token 和本地快照为准。
        saved = await Store.account(account.id)
        if saved is not None:
            account.token = saved.token
            account.subscribed = saved.subscribed
            account.token_expires_at = saved.token_expires_at
            account.token_renew_after = saved.token_renew_after
            account.token_state = saved.token_state
            account.sdk_token = saved.sdk_token
            account.sdk_openid = saved.sdk_openid
            account.sdk_username = saved.sdk_username
            account.sdk_expires_at = saved.sdk_expires_at
            account.sdk_owner_id = saved.sdk_owner_id
            account.info = saved.info
            account.bag_snapshot = saved.bag_snapshot
            account.verified_at = saved.verified_at
        if account.token_state == "expired":
            raise AuthenticationError(command_hint(TOKEN_EXPIRED_MESSAGE))
        if account.token_state == "recovering":
            raise AuthenticationError(command_hint("新登录凭据已保存，尚待验证；请发送 wt恢复 继续验证。"))
        try:
            info = await self.api.me(account.token, fresh=fresh)
        except ApiError as error:
            if not error.is_auth:
                raise
            await self.expire_account(account)
            raise AuthenticationError(command_hint(TOKEN_EXPIRED_MESSAGE)) from None
        if str(info.cuid) != account.cuid:
            raise DataError("服务端返回的 CUID 与绑定账号不一致，已停止本次操作。")
        if info.is_domestic is not None and info.is_domestic != account.domestic:
            raise DataError("服务端返回的区服与绑定账号不一致，已停止本次操作。")
        if not force_remote:
            info = info.merge_saved(account.info)
        account.info = info.model_dump_json()
        account.verified_at = time.time()
        account.token_state = "ready"
        await Store.save(account)
        await self.renew_token_if_needed(account)
        return info

    async def renew_token_if_needed(self, account: Account) -> None:
        now = time.time()
        failure_key = "renew:" + account.id
        if not self.failures.cooldown_ready(failure_key):
            return
        if account.token_renew_after > now:
            return
        if account.token_expires_at and account.token_expires_at - now >= TOKEN_RENEW_WINDOW:
            return
        try:
            expires_at = await self.api.renew_token(account.token)
            if expires_at is not None and expires_at <= now:
                raise DataError("token 续期返回的有效期未推进，原会话已保留。")
        except MaintenanceError:
            raise
        except WuwaError as error:
            if isinstance(error, ApiError) and error.is_auth:
                await self.expire_account(account)
                await self.failures.reset(failure_key)
                raise AuthenticationError(command_hint(TOKEN_EXPIRED_MESSAGE)) from None
            if isinstance(error, ApiError) and error.code == 10014:
                return
            reason = self.error_detail(error)
            await self.failures.fail(
                failure_key,
                "后台token续期",
                reason,
                account.id,
                delay=TOKEN_RENEW_RETRY,
                blocked_delay=TOKEN_RENEW_BLOCKED_RETRY,
            )
            return
        account.token_expires_at = expires_at if expires_at is not None else now + TOKEN_LIFETIME
        remaining = account.token_expires_at - now
        account.token_renew_after = (
            now + min(TOKEN_RENEW_BLOCKED_RETRY, max(60.0, remaining / 2)) if remaining < TOKEN_RENEW_WINDOW else 0
        )
        await Store.save(account)
        await self.failures.success(failure_key)
        logger.info(
            f"[WuwaTappoUID] CUID {mask_cuid(account.cuid)}："
            f"原 token 续期成功，有效期至 {date_text(account.token_expires_at)}，未重新登录。"
        )

    async def resume_after_token(self, account: Account, info: UserInfo) -> str:
        """调用方持有账号锁；只恢复因鉴权失效暂停的有效任务。"""
        task = await Store.task(account.id)
        if task is None or task.status not in ("active", "paused"):
            return command_hint("发送 wt宝箱 可开启自动开箱。")
        if task.status == "active":
            return "宝箱任务继续运行。"
        if task.pause_reason != "auth":
            return command_hint("宝箱任务保持暂停，可用 wt恢复 操作。")
        if task.remaining() <= 0:
            await self.finish(task, "expired", "本轮已到期", notify=False)
            return command_hint("原宝箱任务已到期，发送 wt宝箱 可开启新一轮。")
        # 暂停瞬间若开箱流水或结果入队曾超时，恢复为 active 前先补写再入队，避免余量继续滞留。
        await self._flush_pending_opens(account.id, task)
        await self.queue_results(account.id, force=True, reason="任务中断，汇总已开结果")
        if task.paused_at is not None:
            task.expires_at += time.time() - task.paused_at
        task.paused_at = None
        task.status = "active"
        task.pause_reason = ""
        task.next_check = self.next_wakeup(task, info)
        await self.failures.reset_account(account.id)
        await Store.save(task)
        self.schedule(task)
        return "宝箱任务已自动恢复，继续剩余有效期。"

    async def bind_token(self, event: Event, token: str, domestic: int | None = None) -> list[str | bytes]:
        owner = identity(event.bot_id, event.user_id)
        async with self.user_lock(owner):
            info = await self.api.me(token)
            cuid = str(info.cuid)
            if domestic is None:
                domestic = info.is_domestic
                if domestic is None:
                    domestic = 0 if str(info.server_id) in NET_SERVER_IDS else 1
            target = account_id(cuid, domestic)
            async with self.account_lock(target):
                # 等锁期间凭据和游戏状态可能改变；不能拿锁外验证快照覆盖当前账号。
                info = await self.api.me(token, fresh=True)
                if str(info.cuid) != cuid:
                    raise DataError("锁内验证的CUID与首次验证不一致，本次未保存token。")
                if info.is_domestic is not None and info.is_domestic != domestic:
                    raise WuwaError("token所属区服与指定区服不同，本次未保存；请指定正确区服。")
                account = await Store.account(target)
                if account is None:
                    account = Account(id=target, cuid=cuid, domestic=domestic)
                if account.sdk_owner_id == owner:
                    account.clear_sdk()
                # refresh 会优先使用数据库里的旧 token；新会话验证后直接保存。
                token_expires_at = 0.0
                try:
                    renewed = await self.api.renew_token(token)
                except WuwaError:
                    pass
                else:
                    now = time.time()
                    if renewed is None:
                        token_expires_at = now + TOKEN_LIFETIME
                    elif renewed > now:
                        token_expires_at = renewed
                previous_token = account.token
                account.token = token
                account.token_state = "ready"
                account.token_expires_at = token_expires_at
                account.token_renew_after = 0
                info = info.merge_saved(account.info)
                account.info = info.model_dump_json()
                account.verified_at = time.time()
                await Store.save(
                    account,
                    Binding(id=owner, platform=event.bot_id, user_id=event.user_id, account_id=target),
                    AccountLink(owner_id=owner, account_id=target),
                )
                if previous_token and previous_token != token:
                    self.api.retire_token(previous_token)
                await self.failures.success("renew:" + account.id)
                try:
                    resume_note = await self.resume_after_token(account, info)
                except SQLAlchemyError:
                    raise
                except Exception as error:
                    detail = self.error_detail(error)
                    logger.error(f"[WuwaTappoUID] token更新成功但任务恢复失败：{detail}")
                    resume_note = f"登录成功，但任务尚未恢复：{detail}；可稍后用 {command_prefix()}恢复。"
                task = await Store.task(account.id)
        self.wake_notices()
        reply = await self.account_reply(account, task, event)
        notice = "token 登录成功，已绑定为当前账号。\n" + resume_note
        return [notice, *reply]

    @staticmethod
    def select_account(accounts: list[Account], reference: str, domestic: int | None) -> Account:
        if reference.startswith("["):
            index = int(reference[1:-1])
            if not 1 <= index <= len(accounts):
                raise WuwaError(command_hint("账号编号不存在，请发送 wt查看 查看当前编号。"))
            target = accounts[index - 1]
            if domestic is not None and target.domestic != domestic:
                raise WuwaError(command_hint("该编号的区服与指定区服不同，请按 wt查看 的编号操作。"))
            return target
        matches = [a for a in accounts if a.cuid == reference and (domestic is None or a.domestic == domestic)]
        if len(matches) > 1:
            raise WuwaError("该 CUID 同时绑定了国服与海外，请使用编号或在末尾指定区服。")
        if matches:
            return matches[0]
        if domestic is None and 1 <= int(reference) <= len(accounts):
            return accounts[int(reference) - 1]
        raise WuwaError(command_hint("尚未绑定该账号，请发送 wt查看 查看编号或 CUID。"))

    async def view_accounts(self, event: Event) -> str:
        owner = identity(event.bot_id, event.user_id)
        async with self.user_lock(owner):
            accounts = await Store.linked_accounts(owner)
            if not accounts:
                raise WuwaError(command_hint("请先发送 wt登录 或 wt添加token TOKEN。"))
            binding = await Store.binding(owner)
            current_id = binding.account_id if binding is not None else ""
            lines = ["已绑定的账号："]
            for index, account in enumerate(accounts, 1):
                current = " · 当前" if account.id == current_id else ""
                status = " · token已过期" if account.token_state == "expired" else ""
                lines.append(
                    f"[{index}] {mask_cuid(account.cuid)}（{'国服' if account.domestic else '海外'}）{current}{status}"
                )
        return "\n".join(lines) + command_hint("\nwt切换 [x] 切换账号；wt删除 [x] 删除绑定。")

    async def delete_account(self, event: Event, cuid: str = "", domestic: int | None = None) -> str:
        owner = identity(event.bot_id, event.user_id)
        async with self.user_lock(owner):
            binding = await self.user_binding(event)
            accounts = await Store.linked_accounts(owner)
            if cuid:
                target = self.select_account(accounts, cuid, domestic)
            else:
                target = next((a for a in accounts if a.id == binding.account_id), None)
                if target is None:
                    raise WuwaError("当前绑定的账号数据缺失，请先重新绑定或切换到已有账号后再删除。")
            async with self.account_lock(target.id):
                task = await Store.task(target.id)
                stopped = task is not None and task.owner_id == owner and task.status in ("active", "paused")
                if stopped:
                    await self.finish(task, "stopped", "已删除绑定，自动开箱已停止", notify=False)
                remaining = [a for a in accounts if a.id != target.id]
                replacement = remaining[0].id if remaining else None
                finalized_notices = await Store.delete_link(owner, target.id, replacement)
                for notice_id in finalized_notices:
                    self.release_notice_image(notice_id)
                updated = await Store.account(target.id)
                if target.token and (updated is None or not updated.token):
                    self.api.retire_token(target.token)
                if binding.account_id == target.id:
                    current = (
                        f"当前账号已切换为 CUID {mask_cuid(remaining[0].cuid)}。" if remaining else "暂无绑定账号。"
                    )
                else:
                    current = ""
        self.wake_notices()
        return (
            f"已删除 CUID {mask_cuid(target.cuid)}（{'国服' if target.domestic else '海外'}）的绑定。"
            + ("本人开启的任务已停止并汇总。" if stopped else "")
            + ("剩余开箱结果将补发；未判定的新物品不标 NEW。" if finalized_notices else "")
            + current
        )

    async def status(self, event: Event) -> str:
        if event.user_pm > 1:
            raise WuwaError(command_hint("wt状态仅限 Bot 管理员使用。"))
        if not self.database_ready:
            return f"WT 已停止\n{self.database_error}\n连续失败上限：{retry_limit()} 次"
        bound_people, task_people, total_chests = await Store.statistics()
        stats = await Store.operational_statistics()
        external = len(self.runtime.login._external_workers)
        local = len(self.runtime.login._workers) - external
        oldest = stats["oldest"]
        waiting = remaining_text(time.time() - oldest) if oldest is not None else "无积压"
        lines = [
            f"WT 状态\n绑定人数：{bound_people} 人\n开启任务人数：{task_people} 人（含暂停）",
            f"累计成功开箱：{total_chests} 个\n连续失败上限：{retry_limit()} 次",
            f"运行任务：{stats['active']}；手动暂停：{stats['manual']}；鉴权暂停：{stats['auth']}",
            f"故障暂停：{stats['failure']}；路由暂停：{stats['route']}",
            f"待发通知：{stats['pending']}；最早等待：{waiting}；无法投递：{stats['unroutable']}",
            f"登录工作：本地 {local}，外置 {external}；待补写：{len(self.runtime.login._checkpoints)}",
        ]
        for stage, label in (("notice-loop", "通知"), ("maintain", "巡检"), ("login", "登录")):
            sample = self.diagnostics.stages.get(stage)
            if sample is not None:
                lines.append(
                    f"{label}最近完成：{date_text(sample.finished_at)}，{sample.seconds:.2f}s"
                    + (f"，异常 {sample.error}" if sample.error else "")
                )
        if self.diagnostics.last_error is not None:
            stage, error = self.diagnostics.last_error
            lines.append(f"最近异常阶段：{stage} · {error}")
        if self.failures.last_failure is not None:
            failed_at, phase = self.failures.last_failure
            lines.append(f"最近后台故障：{date_text(failed_at)} · {phase}")
        return "\n".join(lines)

    async def switch(self, event: Event, cuid: str = "", domestic: int | None = None) -> str:
        owner = identity(event.bot_id, event.user_id)
        async with self.user_lock(owner):
            binding = await self.user_binding(event)
            accounts = await Store.linked_accounts(owner)
            if not accounts:
                raise WuwaError("没有可切换的有效账号，请先重新绑定。")
            if cuid:
                target = self.select_account(accounts, cuid, domestic)
            else:
                current = next((i for i, a in enumerate(accounts) if a.id == binding.account_id), None)
                target = accounts[0] if current is None else accounts[(current + 1) % len(accounts)]
            binding.account_id = target.id
            await Store.save(binding)
        return (
            f"已切换到 CUID {mask_cuid(target.cuid)}（{'国服' if target.domestic else '海外'}）。"
            "原账号的任务、订阅及推送位置不变。"
            + ("\n" + self.expired_message(target, owner) if target.token_state == "expired" else "")
        )

    async def subscribe(self, event: Event, enabled: bool) -> str:
        async with self.user_lock(identity(event.bot_id, event.user_id)):
            binding, account = await self.bound(event)
            async with self.account_lock(account.id):
                account = await Store.account(account.id)
                assert account is not None
                task = await Store.task(account.id)
                if task is not None and task.status in ("active", "paused") and task.owner_id != binding.id:
                    raise WuwaError("当前账号的任务属于另一用户，请由任务所属用户修改订阅。")
                account.subscribed = enabled
                await Store.save(account)
                if enabled:
                    await self.queue_results(account.id)
        self.wake_notices()
        return f"CUID {mask_cuid(account.cuid)}：" + (
            "已开启每满10次推送；中断或任务结束时也会补发已有结果。"
            if enabled
            else "已关闭每满10次推送，改为任务结束汇总；中断时仍补发已有结果。"
        )

    async def queue_results(self, key: str, *, force: bool = False, reason: str = "") -> None:
        """调用方持有账号锁；结果归入通知和 notice_id 在同一事务落库。"""
        account = await Store.account(key)
        if account is None:
            return
        grouped: dict[tuple[str, int], list[ChestRecord]] = {}
        for record in await Store.pending_results(key):
            grouped.setdefault((record.run_id, record.subscription_id), []).append(record)
        info = UserInfo.model_validate_json(account.info) if account.info else None
        task = await Store.task(key)
        unbound = not account.token and account.token_state == "expired"
        for (run_id, subscription_id), records in grouped.items():
            send_rest = force or unbound or task is None or task.run_id != run_id or task.status != "active"
            while records and (send_rest or account.subscribed and len(records) >= 10):
                # 订阅按十件推送；未订阅保留整轮通知，绘图入口按页绘制。
                size = min(len(records), 10) if account.subscribed else len(records)
                batch, records = records[:size], records[size:]
                text = (
                    f"本次 {len(batch)} 个 · 账号已解绑，未判定物品不标 NEW"
                    if unbound
                    else f"{reason + ' · ' if reason else ''}本次 {len(batch)} 个 · {date_text(batch[-1].created_at)}"
                )
                notice = Notice(
                    subscription_id=subscription_id,
                    account_id=key,
                    text=text,
                    record_ids=json.dumps([r.id for r in batch]),
                    available_clicks=info.remaining if info else None,
                    new_checked=unbound,
                )
                for record in batch:
                    record.notice_id = notice.id
                await Store.save(*batch, notice)

    async def info(self, event: Event) -> list[str | bytes]:
        async with self.user_lock(identity(event.bot_id, event.user_id)):
            _, account = await self.bound(event)
            async with self.account_lock(account.id):
                await self.refresh(account, fresh=True)
                task = await Store.task(account.id)
        self.wake_notices()
        reply = await self.account_reply(account, task, event)
        return reply

    async def query(self, event: Event, bag: bool = False, filters: BagFilter | None = None) -> list[str | bytes]:
        owner = identity(event.bot_id, event.user_id)
        async with self.user_lock(owner):
            _, account = await self.bound(event)
            async with self.account_lock(account.id):
                await self.refresh(account)
                items = await self.read_bag(account) if bag else None
        # 绘图只读取本次快照，不占用自动开箱所需的账号锁。
        if items is not None:
            answer: list[str | bytes] = list(await self.cards.backpack(account, items, event, filters))
        else:
            image = await self.cards.click_summary(account, event)
            answer = [image]
        self.wake_notices()
        return answer

    async def atlas(self, event: Event, kind: str | None = None) -> list[str | bytes]:
        owner = identity(event.bot_id, event.user_id)
        async with self.user_lock(owner):
            _, account = await self.bound(event)
            async with self.account_lock(account.id):
                await self.refresh(account)
                items = await self.read_bag(account)
        images: list[str | bytes] = list(await self.cards.atlas(account, items, event, kind))
        self.wake_notices()
        return images

    async def read_bag(self, account: Account, *, fresh: bool = False) -> list[Item]:
        try:
            return await self.api.bags(account.token, fresh=fresh)
        except ApiError as error:
            if not error.is_auth:
                raise
            await self.expire_account(account)
            raise AuthenticationError(command_hint(TOKEN_EXPIRED_MESSAGE)) from None

    async def redeem(self, event: Event, code: str) -> list[str | bytes]:
        async with self.user_lock(identity(event.bot_id, event.user_id)):
            _, account = await self.bound(event)
            async with self.account_lock(account.id):
                before_info = await self.refresh(account, fresh=True)
                before_items = await self.read_bag(account, fresh=True)
                try:
                    await self.api.redeem(account.token, code)
                except ApiError as error:
                    if error.is_auth:
                        await self.expire_account(account)
                        raise AuthenticationError(command_hint(TOKEN_EXPIRED_MESSAGE)) from None
                    raise
                except MaintenanceError:
                    raise
                except WuwaError as error:
                    logger.warning(f"[WuwaTappoUID] 兑换结果未确认：{self.error_detail(error)}")
                    return [
                        f"兑换结果尚未确认：{error}。本次未自动重发，"
                        f"请用 {command_prefix()}背包、{command_prefix()}次数 核对实际变化。"
                    ]
                # 从这里起官方已确认兑换成功，后续故障不得提示成兑换失败。
                try:
                    after_info = await self.refresh(account, fresh=True)
                    after_items = await self.read_bag(account, fresh=True)
                except SQLAlchemyError as error:
                    await self.disable_database(error)
                    return ["兑换成功，但结果刷新失败。\n" + self.database_error]
                except (DatabaseWriteTimeout, WriteGateTimeout):
                    return [
                        command_hint("兑换成功，但数据库写入超时，结果未能完成刷新。请稍后用 wt背包、wt次数 查看。")
                    ]
                except WuwaError as error:
                    logger.warning(f"[WuwaTappoUID] 兑换成功但结果刷新失败：{self.error_detail(error)}")
                    return [
                        f"兑换成功，但结果刷新失败：{error}。"
                        f"请稍后用 {command_prefix()}背包、{command_prefix()}次数 查看。"
                    ]
                except Exception as error:
                    logger.exception("[WuwaTappoUID] 兑换成功后刷新出现意外错误")
                    return [
                        f"兑换成功，但结果刷新异常（{type(error).__name__}），已记录。"
                        f"请用 {command_prefix()}背包、{command_prefix()}次数 查看。"
                    ]
                before = {str(item.asset_id): item.quantity for item in before_items}
                gains = [
                    item.model_copy(update={"quantity": item.quantity - before.get(str(item.asset_id), 0)})
                    for item in after_items
                    if item.quantity > before.get(str(item.asset_id), 0)
                ]
                clicks = after_info.total_click_count - before_info.total_click_count
        summary = "兑换成功。"
        if clicks:
            summary += f"\n累计点击变化：{clicks:+,}。"
        if not gains:
            return [summary + "\n背包未检测到物品增加。"]
        summary += "\n背包增加：\n" + "\n".join(
            f"{self.cards.item_name(item.asset_id, item.asset_name)} ×{item.quantity}" for item in gains
        )
        try:
            images = await self.cards.redeemed(account, gains, event)
        except Exception:
            logger.exception("[WuwaTappoUID] 兑换成功，但结果图绘制失败")
            return [summary + command_hint("\n结果图绘制失败，可用 wt背包 查看。")]
        return [summary, *images]

    async def chest_statistics(self, event: Event, period: StatisticsPeriod) -> list[str | bytes]:
        async with self.user_lock(identity(event.bot_id, event.user_id)):
            _, account = await self.bound(event, allow_expired=True)
            first_at, groups = await Store.chest_history(account.id, period.since, period.until)
        stats = await asyncio.to_thread(ChestStatistics.aggregate, groups)
        return [await self.cards.chest_statistics(account, event, period, stats, first_at)]

    async def synthesize(
        self,
        event: Event,
        options: SynthesisOptions,
        on_started: Callable[[], Awaitable[None]] | None = None,
        *,
        request_scope: AsyncExitStack | None = None,
    ) -> list[str | bytes]:
        return await synthesis_service.synthesize(self, event, options, on_started, request_scope=request_scope)

    @staticmethod
    def check_deadline(task: ChestTask | None) -> None:
        if task is not None and task.remaining() <= 0:
            raise Expired

    async def add_clicks(
        self,
        account: Account,
        number: int,
        task: ChestTask | None = None,
        current_info: UserInfo | None = None,
    ) -> tuple[UserInfo, int, str]:
        before = current_info if current_info is not None else await self.refresh(account)
        self.check_deadline(task)
        if self.closing:
            return before, 0, "插件正在重新加载，本次未提交点击。"
        target = before.total_click_count + number
        problem = ""
        submission_error = None
        try:
            ack = await self.api.set_clicks(account.token, target, task.expires_at if task is not None else None)
            if ack in CODE_OK:
                after = before.model_copy(update={"total_click_count": target})
                account.info = after.model_dump_json()
                account.verified_at = time.time()
                await Store.save(account)
                return after, number, ""
            problem = str(ApiError(ack))
        except OperationStopped as error:
            return before, 0, str(error)
        except ApiError as error:
            if error.is_auth:
                await self.expire_account(account)
                raise AuthenticationError(command_hint(TOKEN_EXPIRED_MESSAGE)) from None
            if task is not None and error.code == 10014:
                raise
            problem = str(error)
        except (NetworkError, DataError) as error:
            submission_error = error
            problem = str(error)
        # 仅在提交异常或返回非成功码（如 80022）时回读服务端值；保留单调合并避免冲掉刚推进的本地冷却。
        after = await self.refresh(account, fresh=True)
        delta = after.total_click_count - before.total_click_count
        if task is not None and delta <= 0 and submission_error is not None:
            raise submission_error
        return after, delta, problem

    async def increase(
        self,
        event: Event,
        number: int,
        on_started: Callable[[], Awaitable[None]] | None = None,
        *,
        request_scope: AsyncExitStack | None = None,
    ) -> list[str | bytes]:
        limit = max_click_increment()
        if not 1 <= number <= limit:
            raise WuwaError(f"单次增加次数须为 1～{limit:,}，请核对输入。")
        async with AsyncExitStack() as local_scope:
            async with self.heavy_operation(
                event, "增加次数", on_started, request_scope=request_scope if request_scope is not None else local_scope
            ) as (_, account):
                _, delta, problem = await self.add_clicks(account, number)
            self.wake_notices()
            image = await self.cards.click_summary(account, event, number, delta)
            hint = f"提示：{problem}（已同步最新累计值）" if problem else ""
            return [hint, image] if hint else [image]

    def job_id(self, key: str) -> str:
        return "WuwaTappoUID:chest:" + key

    def schedule(self, task: ChestTask, *, run_at: float | None = None) -> None:
        if not self.runtime.active:
            return
        if task.timer_paused:
            self.unschedule(task.account_id)
            return
        if self.closing or not self.database_ready or task.status not in ("active", "paused"):
            return
        if task.status == "active" and (
            self.failures.blocked("check:" + task.account_id) or self.failures.blocked("save:" + task.account_id)
        ):
            return
        target_time = (
            run_at if run_at is not None else (task.expires_at if task.status == "paused" else task.next_check)
        )
        scheduler.add_job(
            self.tick,
            "date",
            run_date=datetime.fromtimestamp(max(time.time() + 0.1, target_time), timezone.utc),
            args=[task.account_id],
            id=self.job_id(task.account_id),
            name="WuwaTappo 账号宝箱检查",
            replace_existing=True,
            misfire_grace_time=None,
            # tick 在收尾时给自己排下一次；只允许 1 个实例时，本次未结束就到点的一次性任务会被跳过并丢失。
            # 允许第 2 个实例，它会在账号锁上等本次结束。
            max_instances=2,
        )

    def unschedule(self, key: str) -> None:
        if self.closing or not self.runtime.active:
            return
        job = scheduler.get_job(self.job_id(key))
        if job is not None:
            job.remove()

    def next_wakeup(self, task: ChestTask, info: UserInfo | None, retry: bool = False) -> float:
        now = time.time()
        if task.status == "paused":
            return max(now + 0.1, task.expires_at)
        if retry:
            # 失败后不使用已经过去的提醒/到期点，避免 0.1 秒循环。
            wakeup = max(now + 60, self.failures.retry_after("check:" + task.account_id))
            return min(wakeup, task.expires_at) if task.expires_at > now else wakeup
        candidates = [task.expires_at]
        if not task.reminded:
            # 剩余不超过 1 小时进入提醒与续期窗口。
            candidates.append(task.expires_at - RENEW_WINDOW)
        if info is None:
            candidates.append(now + 60)
        else:
            candidates.extend(max(now + 5, info.cooldown(side)) for side in ("left", "right"))
        return max(now + 0.1, min(candidates))

    async def start_or_renew(self, event: Event) -> list[str | bytes]:
        hours = chest_task_hours()
        async with self.user_lock(identity(event.bot_id, event.user_id)):
            binding, account = await self.bound(event)
            async with self.account_lock(account.id):
                task = await Store.task(account.id)
                now = time.time()
                if (
                    task is not None
                    and task.status in ("active", "paused")
                    and task.remaining(now) > 0
                    and task.owner_id != binding.id
                ):
                    raise WuwaError("这个账号已有其他用户开启的任务，请由任务所属用户操作。")
                if task is not None and task.status in ("active", "paused") and task.remaining(now) <= 0:
                    await self.finish(task, "expired", "本轮已到期", notify=False)
                if task is not None and task.status in ("active", "paused"):
                    if task.owner_id != binding.id:
                        raise WuwaError("这个账号已有其他用户开启的任务，请由任务所属用户操作。")
                    if task.status == "paused":
                        action = command_hint("任务已暂停，可发送 wt恢复 继续本轮。")
                    else:
                        action = "任务正在运行。"
                    if 0 < task.remaining(now) <= RENEW_WINDOW:
                        task.expires_at += hours * 3600
                        task.reminded = False
                        if task.status == "paused":
                            task.next_check = task.expires_at
                        else:
                            latest = await Store.account(account.id)
                            if latest is not None:
                                account = latest
                            saved_info = UserInfo.model_validate_json(account.info) if account.info else None
                            retrying = self.failures.attempts("check:" + account.id) > 0
                            task.next_check = self.next_wakeup(task, saved_info, retry=retrying)
                        action += f"已在原到期时间上续期 {hours} 小时。"
                    await Store.save(task)
                    self.schedule(task)
                else:
                    await self.refresh(account)
                    if not event.WS_BOT_ID:
                        raise WuwaError("当前会话不支持自动推送，请在群聊或私聊中使用。")
                    run_id = new_id()
                    async with self.subscription(event, "WuwaTappo宝箱", run_id) as subscription_id:
                        now = time.time()
                        task = ChestTask(
                            account_id=account.id,
                            run_id=run_id,
                            owner_id=binding.id,
                            subscription_id=subscription_id,
                            started_at=now,
                            expires_at=now + hours * 3600,
                            next_check=now,
                        )
                        await Store.save(task)
                    await self.failures.reset("check:" + account.id)
                    await self.failures.reset("save:" + account.id)
                    self.schedule(task)
                    action = f"已开启 {hours} 小时自动开箱，将立即检查左右宝箱。"
                answer = action
        self.wake_notices()
        image = await self.cards.task_summary(account, task, event)
        return [answer, image]

    async def finish(self, task: ChestTask, status: str, reason: str, *, notify: bool = True) -> None:
        # 先补写当前账号已确认但待落库的开箱流水，再持久化结束状态并撤销调度。
        try:
            await self._flush_pending_opens(task.account_id, task)
        except (DatabaseWriteTimeout, WriteGateTimeout) as error:
            logger.warning(
                f"[WuwaTappoUID] 任务结束前补写开箱流水超时，将由后台巡检继续补写：{self.error_detail(error)}"
            )
        task.status = status
        task.paused_at = None
        await Store.save(task)
        self.unschedule(task.account_id)
        await self.queue_results(task.account_id, force=True, reason=reason)
        if not notify:
            return
        suffix = command_hint("发送 wt宝箱 可开启新一轮。") if status == "expired" else ""
        await Store.save(
            task,
            Notice(
                subscription_id=task.subscription_id,
                account_id=task.account_id,
                text=f"{reason}，本轮共开启 {task.successes} 个宝箱。{suffix}",
            ),
        )

    async def stop_owned(self, binding: Binding, reason: str) -> str:
        task = await Store.task(binding.account_id)
        if task is None or task.status not in ("active", "paused"):
            return "当前没有运行中的自动开箱任务，账号绑定已保留。"
        if task.owner_id != binding.id:
            raise WuwaError("任务属于绑定此账号的另一用户，请由任务所属用户停止。")
        await self.finish(task, "stopped", reason, notify=False)
        return f"自动开箱已停止，账号绑定已保留。本轮共开启 {task.successes} 个宝箱。"

    async def stop(self, event: Event) -> str:
        async with self.user_lock(identity(event.bot_id, event.user_id)):
            binding, account = await self.bound(event, allow_expired=True)
            async with self.account_lock(account.id):
                answer = await self.stop_owned(binding, "已手动停止自动开箱")
        self.wake_notices()
        return answer

    async def pause(self, event: Event) -> str:
        async with self.user_lock(identity(event.bot_id, event.user_id)):
            binding, account = await self.bound(event, allow_expired=True)
            async with self.account_lock(account.id):
                account = await Store.account(binding.account_id)
                if account is None:
                    raise WuwaError("绑定的账号数据缺失，请重新绑定。")
                task = await Store.task(account.id)
                if task is None or task.status not in ("active", "paused"):
                    raise WuwaError(command_hint("当前没有可暂停的任务；请用 wt宝箱 开启新任务。"))
                if task.owner_id != binding.id:
                    raise WuwaError("任务属于另一用户，请由任务所属用户操作。")
                if task.remaining() <= 0:
                    await self.finish(task, "expired", "本轮已到期", notify=False)
                    answer = command_hint("本轮已到期，发送 wt宝箱 可开启新一轮。")
                elif task.timer_paused:
                    answer = "任务和计时已经暂停。\n" + task_text(task)
                else:
                    task.status = "paused"
                    task.pause_reason = "manual"
                    task.paused_at = time.time()
                    action = "宝箱任务和计时已暂停，剩余有效期保留。"
                    task.next_check = time.time()
                    await Store.save(task)
                    self.unschedule(account.id)
                    await self.queue_results(account.id, force=True, reason="任务暂停")
                    answer = action + "\n" + task_text(task)
        self.wake_notices()
        return answer

    async def recover(self, event: Event) -> str:
        """用户主动恢复；每次至多交换一次，已取得的新token只继续验证。"""
        async with self.user_lock(identity(event.bot_id, event.user_id)):
            binding, account = await self.bound(event, allow_expired=True)
            async with self.account_lock(account.id):
                account = await Store.account(binding.account_id)
                if account is None:
                    raise WuwaError("绑定的账号数据缺失，请重新绑定。")
                task = await Store.task(account.id)
                if task is not None and task.status in ("active", "paused") and task.owner_id != binding.id:
                    raise WuwaError("任务属于另一用户，请由任务所属用户恢复。")
                if task is not None and task.status == "active":
                    await self.pause_invalid_route(task)
                pending = account.token_state == "recovering"
                exchanged = pending
                info = None
                if account.token_state != "expired":
                    try:
                        info = await self.api.me(account.token, fresh=True)
                    except ApiError as error:
                        if not error.is_auth:
                            raise
                        await self.expire_account(account)
                        if pending:
                            raise AuthenticationError(
                                command_hint("新登录态已被拒绝，本次未重复登录；请稍后重新发送 wt恢复。")
                            ) from None
                    except (NetworkError, DataError):
                        if not pending:
                            raise
                        return command_hint(
                            "新登录凭据已保存，验证尚未完成；请稍后发送 wt恢复 继续验证，不会重复交换token。"
                        )
                if info is None:
                    if not account.can_recover(binding.id):
                        if account.sdk_owner_id == binding.id:
                            account.clear_sdk()
                            await Store.save(account)
                        raise AuthenticationError(self.expired_message(account, binding.id))
                    sdk = SdkLogin(
                        token=account.sdk_token,
                        cuid=account.cuid,
                        sdk_openid=account.sdk_openid,
                        username=account.sdk_username,
                    )
                    try:
                        roles = await self.api.login_roles(sdk)
                        role = next(
                            (
                                item
                                for item in roles.items
                                if str(item.playerId).isdigit()
                                and int(item.playerId) > 0
                                and item.serverId == CN_SERVER_ID
                            ),
                            None,
                        )
                        token = await self.api.exchange_sdk_token(sdk, role, roles.channelId)
                    except ApiError as error:
                        if error.code == 10024 or error.is_auth:
                            account.clear_sdk()
                            await Store.save(account)
                            raise AuthenticationError(
                                command_hint("保存的SDK凭据已被拒绝，请发送 wt登录 或 wt添加token 更新登录态。")
                            ) from None
                        raise
                    previous_token = account.token
                    account.token = token.token
                    account.token_state = "recovering"
                    account.token_expires_at = 0
                    account.token_renew_after = 0
                    await Store.save(account)
                    if previous_token and previous_token != token.token:
                        self.api.retire_token(previous_token)
                    try:
                        account.token_expires_at = token_expiry(token.expire_time)
                        await Store.save(account)
                        info = await self.api.me(account.token, fresh=True)
                    except (NetworkError, DataError):
                        return command_hint(
                            "新登录凭据已取得并保存，验证尚未完成；请稍后发送 wt恢复 继续验证，不会重复交换token。"
                        )
                    except ApiError as error:
                        if error.is_auth:
                            await self.expire_account(account)
                            raise AuthenticationError(
                                command_hint("新登录态已被拒绝，本次未重复登录；请稍后重新发送 wt恢复。")
                            ) from None
                        raise
                    exchanged = True
                if (
                    str(info.cuid) != account.cuid
                    or info.is_domestic is not None
                    and info.is_domestic != account.domestic
                    or exchanged
                    and str(info.server_id) not in ("0", CN_SERVER_ID)
                ):
                    account.token_state = "expired"
                    await Store.save(account)
                    raise DataError("恢复结果的账号或区服不一致，本次未恢复任务。")
                info = info.merge_saved(account.info)
                account.info = info.model_dump_json()
                account.verified_at = time.time()
                account.token_state = "ready"
                await Store.save(account)
                if exchanged:
                    await self.failures.success("renew:" + account.id)
                prefix = "登录已恢复，同账号其他客户端的旧登录态已失效。" if exchanged else "登录态正常。"
                task = await Store.task(account.id)
                if task is None or task.status not in ("active", "paused"):
                    answer = prefix + "当前没有待恢复的宝箱任务。"
                elif task.remaining() <= 0:
                    await self.finish(task, "expired", "本轮已到期", notify=False)
                    answer = prefix + command_hint("原任务已到期，发送 wt宝箱 可开启新一轮。")
                elif task.status == "active" and not (
                    self.failures.blocked("check:" + account.id) or self.failures.blocked("save:" + account.id)
                ):
                    answer = prefix + "宝箱任务正在运行。"
                else:
                    route = await Store.subscription(task.subscription_id)
                    if task.pause_reason == "route" or route is None or not route.WS_BOT_ID:
                        async with self.subscription(event, "WuwaTappo宝箱", new_id()) as subscription_id:
                            task.subscription_id = subscription_id
                            await Store.save(task)
                    if task.status == "paused":
                        await self._flush_pending_opens(account.id, task)
                        await self.queue_results(account.id, force=True, reason="任务中断，汇总已开结果")
                    if task.paused_at is not None:
                        task.expires_at += time.time() - task.paused_at
                    task.paused_at = None
                    task.status = "active"
                    task.pause_reason = ""
                    task.next_check = self.next_wakeup(task, info)
                    await Store.save(task)
                    await self.failures.reset_account(account.id)
                    self.schedule(task)
                    answer = prefix + "宝箱任务已恢复，继续原轮次。\n" + task_text(task)
        self.wake_notices()
        return answer

    @staticmethod
    def _write_pending_file(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(".tmp")
        temp.write_bytes(data)
        temp.replace(path)

    @staticmethod
    def _remove_pending_file(path: Path) -> None:
        if path.exists():
            path.unlink()

    async def save_pending_writes(self) -> None:
        payload = PendingWrites(
            opens=[item for items in self._pending_opens.values() for item in items],
            synthesis=list(self._pending_synthesis.values()),
        )
        async with self._pending_write_lock:
            try:
                if not payload.opens and not payload.synthesis:
                    await asyncio.to_thread(self._remove_pending_file, self._pending_path)
                else:
                    await asyncio.to_thread(
                        self._write_pending_file, self._pending_path, payload.model_dump_json().encode()
                    )
            except OSError as error:
                logger.error(f"[WuwaTappoUID] 待补写队列落盘失败：{error}")

    async def load_pending_writes(self) -> None:
        async with self._pending_write_lock:
            if not self._pending_path.exists():
                return
            try:
                raw = await asyncio.to_thread(self._pending_path.read_text, "utf-8")
                payload = PendingWrites.model_validate_json(raw)
            except (OSError, ValidationError) as error:
                logger.error(f"[WuwaTappoUID] 待补写队列文件读取失败，已忽略损坏数据：{error}")
                return
        for item in payload.opens:
            bucket = self._pending_opens.setdefault(item.account_id, [])
            if all(existing.id != item.id for existing in bucket):
                bucket.append(item)
        for item in payload.synthesis:
            self._pending_synthesis.setdefault(item.id, item)

    def export_pending_writes(self) -> JsonValue:
        return PendingWrites(
            opens=[item for items in self._pending_opens.values() for item in items],
            synthesis=list(self._pending_synthesis.values()),
        ).model_dump(mode="json")

    def import_pending_writes(self, data: JsonValue) -> None:
        if not isinstance(data, dict):
            return
        payload = PendingWrites.model_validate(data)
        for item in payload.opens:
            bucket = self._pending_opens.setdefault(item.account_id, [])
            if all(existing.id != item.id for existing in bucket):
                bucket.append(item)
        for item in payload.synthesis:
            self._pending_synthesis.setdefault(item.id, item)

    async def stash_pending_synthesis(self, record: SynthesisRecord) -> None:
        self._pending_synthesis[record.id] = PendingSynthesis(
            id=record.id,
            run_id=record.run_id,
            account_id=record.account_id,
            created_at=record.created_at,
            mode=record.mode,
            material_ids=record.material_ids,
            before_quantity=record.before_quantity,
            after_quantity=record.after_quantity,
            state=record.state,
            result=record.result,
            error=record.error,
        )
        await self.save_pending_writes()

    async def _flush_pending_opens(self, key: str, task: ChestTask | None = None) -> int:
        if key not in self._pending_opens:
            return 0
        pending = self._pending_opens[key]
        committed_count = 0
        changed = False
        try:
            while pending:
                item = pending[0]
                committed = await Store.record_chest_open(
                    item.account_id, item.info_json, item.verified_at, item.to_record()
                )
                pending.pop(0)
                changed = True
                if committed:
                    committed_count += 1
                    if task is not None and task.run_id == item.run_id:
                        task.successes += 1
        finally:
            if not pending:
                self._pending_opens.pop(key, None)
            if changed:
                await self.save_pending_writes()
        return committed_count

    async def _flush_pending_synthesis(self) -> int:
        if not self._pending_synthesis:
            return 0
        committed_count = 0
        changed = False
        try:
            for item_id in tuple(self._pending_synthesis):
                item = self._pending_synthesis[item_id]
                await Store.save(item.to_record())
                self._pending_synthesis.pop(item_id, None)
                committed_count += 1
                changed = True
        finally:
            if changed:
                await self.save_pending_writes()
        return committed_count

    async def flush_all_pending_writes(self, *, timeout: float = 5.0) -> None:
        """退出或启动时尽力补写已确认的开箱和合成回执；未写完的保留在磁盘供下次启动恢复。"""
        if not self.database_ready or (not self._pending_opens and not self._pending_synthesis):
            return
        try:
            async with asyncio.timeout(timeout):
                for key in tuple(self._pending_opens):
                    async with self.account_lock(key):
                        if await self._flush_pending_opens(key) > 0:
                            await self.queue_results(key)
                await self._flush_pending_synthesis()
        except (TimeoutError, DatabaseWriteTimeout, WriteGateTimeout, SQLAlchemyError, WuwaError) as error:
            logger.warning(f"[WuwaTappoUID] 退出前补写尚未全部完成，剩余已落盘保留：{self.error_detail(error)}")
        await self.save_pending_writes()

    async def open_side(self, account: Account, task: ChestTask, side: Side, info: UserInfo) -> UserInfo:
        supplement = ""
        if info.remaining < CHEST_CLICK_COST:
            info, delta, problem = await self.add_clicks(account, AUTO_CLICKS, task, info)
            supplement = f"自动补充 {AUTO_CLICKS} 点击后实际变化 {delta:+d}" + (f"，{problem}" if problem else "")
            if info.remaining < CHEST_CLICK_COST:
                raise ClicksUnavailable(supplement)
        for attempt in range(2):
            if self.closing:
                return info
            self.check_deadline(task)
            before_json = info.model_dump_json()
            try:
                result = await self.api.open(account.token, side, info.total_click_count)
            except OperationStopped:
                return info
            except ApiError as error:
                if error.code == 70005 and attempt == 0 and not supplement:
                    info, delta, problem = await self.add_clicks(account, AUTO_CLICKS, task, info)
                    supplement = f"自动补充 {AUTO_CLICKS} 点击后实际变化 {delta:+d}" + (
                        f"，{problem}" if problem else ""
                    )
                    if delta > 0 and info.remaining >= CHEST_CLICK_COST:
                        continue
                    raise ClicksUnavailable(supplement) from None
                if error.code == 70005:
                    raise ClicksUnavailable(f"{supplement}，再次开箱仍提示点击不足") from None
                if error.is_auth:
                    await self.expire_account(account)
                    raise AuthenticationError(command_hint(TOKEN_EXPIRED_MESSAGE)) from None
                if supplement:
                    await self.account_notice(account, f"{SIDE_NAMES[side]}宝箱重试未成功：{error}（{supplement}）。")
                raise
            info.total_click_consume += result.click_consume
            info.set_cooldown(side, time.time() + CHEST_COOLDOWN)
            info_json = info.model_dump_json()
            verified_at = time.time()
            account.info = info_json
            account.verified_at = verified_at
            record = ChestRecord(
                account_id=account.id,
                run_id=task.run_id,
                subscription_id=task.subscription_id,
                side=side,
                state="success",
                before_info=before_json,
                result=result.model_dump_json(),
                supplement=supplement,
            )
            pending = PendingOpen(
                id=record.id,
                account_id=record.account_id,
                run_id=record.run_id,
                subscription_id=record.subscription_id,
                side=record.side,
                created_at=record.created_at,
                before_info=record.before_info,
                result=record.result,
                supplement=record.supplement,
                info_json=info_json,
                verified_at=verified_at,
            )
            try:
                committed = await Store.record_chest_open(account.id, info_json, verified_at, record)
            except (DatabaseWriteTimeout, WriteGateTimeout, SQLAlchemyError):
                bucket = self._pending_opens.setdefault(account.id, [])
                if all(existing.id != pending.id for existing in bucket):
                    bucket.append(pending)
                await self.save_pending_writes()
                raise
            if committed:
                task.successes += 1
            await self.failures.success("check:" + task.account_id)
            await self.queue_results(account.id)
            return info
        raise RuntimeError("开箱重试流程没有产生结果")

    @tracked_method
    @with_read_cache
    async def tick(self, key: str) -> None:
        if self.closing or not self.database_ready:
            return
        attempts_before = self.failures.attempts("check:" + key)
        try:
            await self._check_account(key)
            self.wake_notices()
        except SQLAlchemyError as error:
            await self.disable_database(error)
        except MaintenanceError:
            return
        except Exception as error:
            if self.failures.attempts("check:" + key) > attempts_before:
                return
            if self.failures.attempts("check:" + key) == 0:
                logger.exception("[WuwaTappoUID] 账号检查未完成")
            failure = await self.failures.fail("check:" + key, "账号检查", self.error_detail(error), key)
            if failure.blocked:
                self.unschedule(key)

    async def _check_account(self, key: str) -> None:
        async with self.account_lock(key):
            if self.closing or not self.database_ready:
                return
            task = await Store.task(key)
            if task is None or task.status not in ("active", "paused"):
                return
            if task.timer_paused:
                self.unschedule(key)
                return
            info: UserInfo | None = None
            retry = False
            interrupted = False
            try:
                if await self._flush_pending_opens(key, task) > 0:
                    reloaded = await Store.task(key)
                    if reloaded is None or reloaded.status not in ("active", "paused"):
                        return
                    task = reloaded
                    await self.queue_results(key)
                if task.remaining() <= 0:
                    await self.finish(task, "expired", "自动开箱已到期")
                    return
                if await self.pause_invalid_route(task):
                    return
                if task.status == "paused":
                    return
                if not task.reminded and task.expires_at - time.time() <= RENEW_WINDOW:
                    task.reminded = True
                    await Store.save(
                        task,
                        Notice(
                            subscription_id=task.subscription_id,
                            account_id=key,
                            text=(
                                f"自动开箱剩余不足 1 小时（{date_text(task.expires_at)} 到期），"
                                f"发送 {command_prefix()}宝箱 可顺延 {chest_task_hours()} 小时。"
                            ),
                            task_run_id=task.run_id,
                            task_expires_at=task.expires_at,
                        ),
                    )
                if not self.failures.allows("check:" + key):
                    retry = True
                    return
                account = await Store.account(key)
                if account is None:
                    raise DataError("任务关联的账号数据缺失。")
                info = await self.refresh(account)
                for side in ("left", "right"):
                    if self.closing:
                        break
                    self.check_deadline(task)
                    if info.cooldown(side) > time.time():
                        continue
                    try:
                        info = await self.open_side(account, task, side, info)
                    except ApiError as error:
                        if error.code == 70006:
                            retry = True
                            continue
                        if error.code == 70003:
                            info = await self.refresh(account, fresh=True, force_remote=True)
                            await self.task_failure(task, error)
                            interrupted = True
                            retry = True
                            return
                        if error.code in (70004, 10014):
                            retry = True
                            break
                        raise
                await self.failures.success("check:" + key)
            except OperationStopped:
                return
            except Expired:
                await self.finish(task, "expired", "自动开箱已到期")
            except MaintenanceError:
                retry = True
            except ClicksUnavailable as error:
                interrupted = True
                task.status = "paused"
                task.pause_reason = "failure"
                task.paused_at = None
                task.next_check = task.expires_at
                await self.failures.success("check:" + key)
                await Store.save(
                    task,
                    Notice(
                        subscription_id=task.subscription_id,
                        account_id=key,
                        text=(
                            f"剩余点击不足，自动开箱已暂停（{error}）。\n"
                            f"请发送 {command_prefix()}增加次数 补足后，再发送 {command_prefix()}恢复。"
                        ),
                    ),
                )
            except NetworkError as error:
                # 每轮网络请求自身有有限重试；持续失败也有后台轮次上限。
                await self.task_failure(task, error)
                retry = True
                interrupted = True
            except AuthenticationError:
                interrupted = True
                task.status = "paused"
                task.pause_reason = "auth"
                task.next_check = task.expires_at
                await Store.save(task)
                self.unschedule(key)
            except ApiError as error:
                if error.code == 10014:
                    retry = True
                else:
                    interrupted = True
                    if error.is_auth:
                        account = await Store.account(key)
                        if account is not None:
                            await self.expire_account(account)
                        task.status = "paused"
                        task.pause_reason = "auth"
                        task.next_check = task.expires_at
                        await Store.save(task)
                        self.unschedule(key)
                    else:
                        await self.task_failure(task, error)
                        retry = True
            except WuwaError as error:
                interrupted = True
                await self.task_failure(task, error)
                retry = True
            except SQLAlchemyError as error:
                await self.disable_database(error)
            except Exception as error:
                if self.failures.attempts("check:" + key) == 0:
                    logger.exception("[WuwaTappoUID] 宝箱检查出现意外错误")
                await self.task_failure(task, error)
                retry = True
                interrupted = True
            finally:
                # 先在内存里排好下次检查，数据库写入或推送再出错也不会让任务停摆。
                if self.database_ready and task.status in ("active", "paused"):
                    task.next_check = self.next_wakeup(task, info, retry)
                    self.schedule(task)
                try:
                    if self.database_ready and interrupted and task.status != "active":
                        await self.queue_results(key, force=True, reason="任务中断，汇总已开结果")
                    if self.database_ready and task.status in ("active", "paused"):
                        await Store.save(task)
                        await self.failures.success("save:" + key)
                except SQLAlchemyError as error:
                    await self.disable_database(error)
                except Exception as error:
                    if self.failures.attempts("save:" + key) == 0:
                        logger.exception("[WuwaTappoUID] 保存检查结果失败")
                    failure = await self.failures.fail("save:" + key, "保存检查结果", self.error_detail(error), key)
                    if failure.blocked:
                        self.unschedule(key)
                    else:
                        retry_at = max(time.time() + 60, self.failures.retry_after("save:" + key))
                        task.next_check = retry_at
                        self.schedule(task, run_at=retry_at)

    async def restore(self) -> None:
        for snapshot in await Store.active_tasks():
            try:
                await self.restore_account(snapshot.account_id)
            except (DatabaseWriteTimeout, WriteGateTimeout) as error:
                await self.failures.fail(
                    "restore:" + snapshot.account_id,
                    "启动任务恢复",
                    self.error_detail(error),
                    snapshot.account_id,
                    limit=1,
                )

    async def restore_account(self, key: str) -> None:
        async with self.account_lock(key):
            # 启动后命令已可能进入，锁内重读，避免覆盖用户刚完成的续期/暂停。
            task = await Store.task(key)
            if task is not None and task.status in ("active", "paused"):
                account = await Store.account(task.account_id)
                if account is not None and account.token_state == "expired":
                    if task.status == "active":
                        task.status = "paused"
                        task.pause_reason = "auth"
                        task.next_check = task.expires_at
                        await Store.save(task)
                    self.schedule(task)
                    return
                if self.failures.blocked("check:" + task.account_id) or self.failures.blocked(
                    "save:" + task.account_id
                ):
                    task.status = "paused"
                    task.pause_reason = "failure"
                if task.timer_paused:
                    self.unschedule(task.account_id)
                    return
                # 不改变 started_at / expires_at；停机期间不补开历史轮次。
                task.next_check = task.expires_at if task.status == "paused" else time.time()
                await Store.save(task)
                self.schedule(task)

    @tracked_method
    async def archive_history(self) -> None:
        if self.closing or not self.database_ready:
            return
        configured = settings.get_config("record_retention_days").data
        if configured != self._archive_days:
            self._archive_days = configured
            self._archive_next = 0
            await self.failures.success("history")
        if time.time() < self._archive_next or not self.failures.allows("history"):
            return
        try:
            days = record_retention_days()
            midnight = datetime.now(CHINA).replace(hour=0, minute=0, second=0, microsecond=0)
            cutoff = (midnight - timedelta(days=days - 1)).timestamp()
            daily_cutoff = (midnight - timedelta(days=29)).timestamp()
            records = other = 0
            for _ in range(10):
                if self.closing or not self.database_ready:
                    return
                archived = await HistoryArchive.archive_batch(cutoff, daily_cutoff)
                cleaned = await HistoryArchive.cleanup_batch(cutoff, daily_cutoff)
                records += archived.archived
                other += cleaned
                if archived.quarantined:
                    logger.error(
                        f"[WuwaTappoUID] 已隔离异常开箱流水 {len(archived.quarantined)} 条，"
                        f"流水ID {','.join(archived.quarantined[:10])}。"
                    )
                    await self.failures.fail(
                        "history-data",
                        "开箱详情异常",
                        "异常原始流水已保留，请检查 archive_error 并修正详情。",
                        limit=1,
                    )
                await self.cleanup_subscriptions()
                if not archived.processed and not cleaned:
                    break
                await asyncio.sleep(0.1)
            # 有积压时下分钟继续，空闲时每小时检查；配置变化在下一次分钟巡检生效。
            self._archive_next = time.time() + (60 if archived.processed or cleaned else 3600)
            await self.failures.success("history")
            if records or other:
                logger.info(
                    f"[WuwaTappoUID] 历史归档完成：开箱流水 {records} 条，已完成记录及过期按日汇总 {other} 条。"
                )
        except SQLAlchemyError as error:
            await self.disable_database(error)
        except Exception as error:
            if self.failures.attempts("history") == 0:
                logger.exception("[WuwaTappoUID] 历史归档出现异常")
            await self.failures.fail("history", "历史归档", self.error_detail(error), delay=300)

    @tracked_method
    @with_read_cache
    async def maintain(self) -> None:
        """每分钟执行：补排丢失了下次检查的任务、保活临期 token，再补发待提交的通知。"""
        if self.closing or not self.database_ready:
            return
        # 冷却期内不重试；达到告警阈值后冷却结束仍继续尝试，成功即恢复，不必等重启。
        if self.failures.retry_after("maintain") > time.time():
            return
        try:
            with measure("maintain"):
                await self._maintain()
                await self.cards.prune_caches()
                await self.cleanup_subscriptions()
            await self.failures.success("maintain")
        except SQLAlchemyError as error:
            await self.disable_database(error)
        except MaintenanceError:
            return
        except Exception as error:
            if self.failures.attempts("maintain") == 0:
                logger.exception("[WuwaTappoUID] 后台巡检出现意外错误")
            await self.failures.fail("maintain", "后台巡检", self.error_detail(error))

    async def _maintain(self) -> None:
        if not self.closing:
            for key in tuple(self._pending_opens):
                if self.closing:
                    break
                if self.account_lock(key).locked() or not self.failures.cooldown_ready("open-save:" + key):
                    continue
                async with self.account_lock(key):
                    try:
                        if await self._flush_pending_opens(key) > 0:
                            await self.queue_results(key)
                        await self.failures.success("open-save:" + key)
                    except SQLAlchemyError:
                        raise
                    except Exception as error:
                        await self.failures.fail(
                            "open-save:" + key, "开箱流水补写", self.error_detail(error), key, blocked_delay=300
                        )
            if self._pending_synthesis and self.failures.cooldown_ready("synthesis-save"):
                try:
                    await self._flush_pending_synthesis()
                    await self.failures.success("synthesis-save")
                except SQLAlchemyError:
                    raise
                except Exception as error:
                    await self.failures.fail(
                        "synthesis-save", "合成流水补写", self.error_detail(error), blocked_delay=300
                    )
            for task in await Store.active_tasks():
                if task.timer_paused:
                    continue
                key = task.account_id
                if self.failures.blocked("save:" + key) or (
                    task.status == "active" and self.failures.blocked("check:" + key)
                ):
                    continue
                # 正在检查的账号持有账号锁，收尾时会自己排下一次。
                if scheduler.get_job(self.job_id(key)) is None and not self.account_lock(key).locked():
                    logger.warning("[WuwaTappoUID] 发现没有排期的宝箱任务，已重新安排检查。")
                    task.next_check = task.expires_at if task.status == "paused" else time.time()
                    self.schedule(task)
                    await self.failures.success("restore:" + key)
            for key in await Store.result_accounts():
                if self.account_lock(key).locked() or not self.failures.cooldown_ready("results:" + key):
                    continue
                async with self.account_lock(key):
                    try:
                        await self.queue_results(key)
                        await self.failures.success("results:" + key)
                    except SQLAlchemyError:
                        raise
                    except MaintenanceError:
                        continue
                    except Exception as error:
                        await self.failures.fail(
                            "results:" + key, "开箱结果汇总", self.error_detail(error), key, blocked_delay=900
                        )
            now = time.time()
            for key in await Store.renewable_accounts(now + TOKEN_RENEW_WINDOW, now):
                if self.closing:
                    break
                if self.account_lock(key).locked() or not self.failures.cooldown_ready("renew:" + key):
                    continue
                async with self.account_lock(key):
                    account = await Store.account(key)
                    if account is None or account.token_state != "ready" or not account.token:
                        continue
                    try:
                        await self.renew_token_if_needed(account)
                    except AuthenticationError:
                        continue
                    except MaintenanceError:
                        break
        self.wake_notices()

    def release_notice_image(self, key: str) -> None:
        return notifications.release_notice_image(self, key)

    def cached_notice_image(self, key: str) -> tuple[bytes, ...] | None:
        return notifications.cached_notice_image(self, key)

    def cache_notice_image(self, key: str, revision: int, name: str, prefix: str, body: tuple[bytes, ...]) -> None:
        return notifications.cache_notice_image(self, key, revision, name, prefix, body)

    async def notice_messages(
        self, notice: Notice, event: Event, account: Account | None, contexts: dict[str, ResultContext]
    ) -> list[Message]:
        return await notifications.notice_messages(self, notice, event, account, contexts)

    @tracked_method
    async def flush_notices(self) -> None:
        return await notifications.flush_notices(self)

    async def _flush_notices(self) -> None:
        return await notifications._flush_notices(self)

    async def _flush_account_notices(self, key: str) -> None:
        return await notifications._flush_account_notices(self, key)

    async def _send_notice_group(
        self,
        key: str,
        subscription_id: int,
        event: Event,
        notices: list[Notice],
        account: Account | None,
        progress: list[Progress] | None,
        unchecked: bool,
    ) -> bool:
        return await notifications._send_notice_group(
            self, key, subscription_id, event, notices, account, progress, unchecked
        )
