"""账号锁、定时宝箱任务、开箱流水与通知提交。"""

import asyncio
import hashlib
import json
import time
from collections import OrderedDict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from gsuid_core.aps import scheduler
from gsuid_core.gss import gss
from gsuid_core.logger import logger
from gsuid_core.models import Event, Message
from gsuid_core.segment import MessageSegment
from gsuid_core.subscribe import gs_subscribe
from gsuid_core.utils.database.base_models import DatabaseWriteTimeout
from gsuid_core.utils.database.write_gate import WriteGateTimeout
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from .api import WuwaAPI, with_read_cache
from .backpack import BagFilter
from .cards import Cards, ResultContext
from .database import Account, AccountLink, Binding, ChestRecord, ChestTask, Notice, Store, SynthesisRecord, new_id
from .failures import Failures
from .formatting import account_text, date_text, mask_cuid, task_text
from .history import HistoryArchive
from .lifecycle import tracked_method
from .render_common import Progress
from .resources import Resources
from .schema import (
    AUTO_CLICKS,
    CN_SERVER_ID,
    CODE_OK,
    NET_SERVER_IDS,
    RENEW_WINDOW,
    SETTLE_SECONDS,
    SIDE_NAMES,
    TOKEN_EXPIRED_MESSAGE,
    TOKEN_LIFETIME,
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
    SynthesisResult,
    UserInfo,
    WuwaError,
    token_expiry,
)
from .settings import bot_name, chest_task_hours, max_click_increment, record_retention_days, retry_limit, settings
from .statistics import CHINA, ChestStatistics, StatisticsPeriod
from .synthesis import highest_tiers, next_recipe

NOTICE_CACHE_BYTES = 32 * 1024 * 1024

if TYPE_CHECKING:
    from .lifecycle import RuntimeOwner


@dataclass
class CachedNoticeImage:
    revision: int
    bot_name: str
    body: tuple[bytes, ...]


@dataclass
class NoticeSendGate:
    lock: asyncio.Lock
    users: int = 0


def identity(platform: str, user_id: str) -> str:
    return hashlib.sha256(json.dumps([platform, user_id], ensure_ascii=False).encode()).hexdigest()


def account_id(cuid: str, domestic: int) -> str:
    return f"{domestic}:{cuid}"


class Service:
    runtime: "RuntimeOwner"

    def __init__(self, api: WuwaAPI, resources: Resources) -> None:
        self.api = api
        self.resources = resources
        self.cards = Cards(api, resources)
        self.failures = Failures(resources.root)
        self._accounts: dict[str, asyncio.Lock] = {}
        self._users: dict[str, asyncio.Lock] = {}
        self._push_lock = asyncio.Lock()
        self.subscription_lock = asyncio.Lock()
        self._notice_again = False
        self._notice_sends: dict[int, NoticeSendGate] = {}
        self._notice_images: OrderedDict[str, CachedNoticeImage] = OrderedDict()
        self._notice_image_bytes = 0
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
            raise WuwaError("当前会话缺少 GS WebSocket 路由，无法保存自动推送目标。")
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
            await Store.save(task)
            self.schedule(task)
        await self.failures.fail(
            f"route:{task.subscription_id}",
            "通知路由失效",
            "自动开箱已暂停，请由原用户发送 wt恢复 重建路由。",
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
        already_paused = task.status == "paused"
        failure = await self.failures.fail(
            "check:" + task.account_id,
            "开箱请求重试耗尽" if already_paused else "后台开箱检查",
            self.error_detail(error),
            task.account_id,
            limit=1 if already_paused else None,
        )
        if failure.blocked:
            if already_paused:
                # 开箱重试耗尽的通知已由 open_side 保存，只补主人告警。
                await Store.save(task)
                return
            task.status = "paused"
            task.pause_reason = "failure"
            await Store.save(
                task,
                Notice(
                    subscription_id=task.subscription_id,
                    account_id=task.account_id,
                    text=f"后台任务持续异常（{failure.attempts}次），已暂停并提醒主人检查。"
                    f"\n{self.error_detail(error)}\n问题处理后可用 wt恢复 继续任务。",
                ),
            )
        else:
            await Store.save(task)

    def account_lock(self, key: str) -> asyncio.Lock:
        return self._accounts.setdefault(key, asyncio.Lock())

    def user_lock(self, key: str) -> asyncio.Lock:
        return self._users.setdefault(key, asyncio.Lock())

    async def user_binding(self, event: Event) -> Binding:
        binding = await Store.binding(identity(event.bot_id, event.user_id))
        if binding is None:
            raise WuwaError("请先发送 wt登录 或 wt添加token TOKEN（海外账号加上海外）。")
        return binding

    async def bound(self, event: Event, *, allow_expired: bool = False) -> tuple[Binding, Account]:
        binding = await self.user_binding(event)
        account = await Store.account(binding.account_id)
        if account is None:
            raise WuwaError("绑定的账号数据缺失，请重新绑定。")
        if account.token_state == "expired" and not allow_expired:
            raise AuthenticationError(self.expired_message(account, binding.id))
        if account.token_state == "recovering" and not allow_expired:
            raise AuthenticationError("新登录凭据已保存，尚待验证；请发送 wt恢复 继续验证。")
        return binding, account

    @staticmethod
    def expired_message(account: Account, owner_id: str) -> str:
        if account.can_recover(owner_id):
            return (
                "登录态已失效，可发送 wt恢复 使用已保存的SDK凭据重新登录。"
                f"\nSDK恢复凭据有效至：{date_text(account.sdk_expires_at)}"
                "\n恢复登录会使官方客户端当前的登录态失效。"
            )
        return TOKEN_EXPIRED_MESSAGE

    async def authentication_message(self, event: Event, error: AuthenticationError) -> str:
        binding = await Store.binding(identity(event.bot_id, event.user_id))
        if binding is not None:
            account = await Store.account(binding.account_id)
            if account is not None and account.token_state == "expired":
                return self.expired_message(account, binding.id)
        return str(error)

    async def expire_account(self, account: Account) -> None:
        """已确认 token 失效；只通知一次，更新有效 token 后恢复鉴权暂停的任务。"""
        was_expired = account.token_state == "expired"
        account.token_state = "expired"
        task = await Store.task(account.id)
        if task is not None and task.status in ("active", "paused"):
            if task.status == "active":
                task.status = "paused"
                task.pause_reason = "auth"
            rows = [account, task]
            if not was_expired:
                rows.append(
                    Notice(
                        subscription_id=task.subscription_id,
                        account_id=account.id,
                        text=f"{date_text(time.time())}：{self.expired_message(account, task.owner_id)}"
                        + (
                            "\n自动开箱已暂停，原到期时间保留。"
                            if task.pause_reason == "auth"
                            else "\n任务维持原暂停状态。"
                        ),
                    )
                )
            await Store.save(*rows)
            self.schedule(task)
        elif not was_expired:
            await Store.save(account)

        self.api.retire_token(account.token)

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

    async def refresh(self, account: Account, *, fresh: bool = False) -> UserInfo:
        # 调用方进入账号锁前可能已读取绑定，锁内以数据库中最新 token 为准。
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
            raise AuthenticationError(TOKEN_EXPIRED_MESSAGE)
        if account.token_state == "recovering":
            raise AuthenticationError("新登录凭据已保存，尚待验证；请发送 wt恢复 继续验证。")
        try:
            info = await self.api.me(account.token, fresh=fresh)
        except ApiError as error:
            if not error.is_auth:
                raise
            await self.expire_account(account)
            raise AuthenticationError(TOKEN_EXPIRED_MESSAGE) from None
        if str(info.cuid) != account.cuid:
            raise DataError("服务端返回的 CUID 与绑定账号不一致，已停止本次操作。")
        if info.is_domestic is not None and info.is_domestic != account.domestic:
            raise DataError("服务端返回的区服与绑定账号不一致，已停止本次操作。")
        account.info = info.model_dump_json()
        account.verified_at = time.time()
        account.token_state = "ready"
        await Store.save(account)
        await self.renew_token_if_needed(account)
        return info

    async def renew_token_if_needed(self, account: Account) -> None:
        now = time.time()
        failure_key = "renew:" + account.id
        if not self.failures.allows(failure_key):
            return
        if account.token_renew_after > now:
            return
        if account.token_expires_at and account.token_expires_at - now >= TOKEN_RENEW_WINDOW:
            return
        try:
            await self.api.renew_token(account.token)
        except MaintenanceError:
            raise
        except WuwaError as error:
            account.token_renew_after = now + TOKEN_RENEW_RETRY
            await Store.save(account)
            if isinstance(error, ApiError) and error.is_auth:
                try:
                    await self.api.me(account.token, fresh=True)
                except ApiError as confirmation:
                    if confirmation.is_auth:
                        await self.expire_account(account)
                        raise AuthenticationError(TOKEN_EXPIRED_MESSAGE) from None
                    raise
            if isinstance(error, ApiError) and error.code == 10014:
                return
            reason = self.error_detail(error)
            await self.failures.fail(failure_key, "后台token续期", reason, account.id, delay=TOKEN_RENEW_RETRY)
            return
        account.token_expires_at = max(now, account.token_expires_at) + TOKEN_LIFETIME
        account.token_renew_after = 0
        await Store.save(account)
        await self.failures.success(failure_key)
        logger.info(f"[WuwaTappoUID] CUID {mask_cuid(account.cuid)}：原 token 续期成功，未重新登录。")

    async def resume_after_token(self, account: Account, info: UserInfo) -> str:
        """调用方持有账号锁；只恢复因鉴权失效暂停的有效任务。"""
        task = await Store.task(account.id)
        if task is None or task.status not in ("active", "paused"):
            return "当前没有待恢复的宝箱任务。"
        if task.status == "active":
            return "宝箱任务继续运行。"
        if task.pause_reason != "auth":
            return "宝箱任务保持暂停，可用 wt恢复 操作。"
        if task.remaining() <= 0:
            await self.finish(task, "expired", "本轮已到期")
            return "宝箱任务已到期，请用 wt宝箱 开启新任务。"
        try:
            if not await self.reconcile(task, info):
                return "登录成功，但任务尚未恢复：开箱结果仍待确认，可稍后用 wt恢复。"
        except WuwaError as error:
            return f"登录成功，但任务尚未恢复：{error}；可稍后用 wt恢复。"
        if task.remaining() <= 0:
            await self.finish(task, "expired", "本轮已到期")
            return "宝箱任务已到期，请用 wt宝箱 开启新任务。"
        if task.paused_at is not None:
            task.expires_at += time.time() - task.paused_at
        task.paused_at = None
        task.status = "active"
        task.pause_reason = ""
        task.next_check = self.next_wakeup(task, info)
        await self.failures.reset_account(account.id)
        await Store.save(
            task,
            Notice(
                subscription_id=task.subscription_id,
                account_id=account.id,
                text="token已更新，宝箱任务已自动恢复，继续剩余有效期。",
            ),
        )
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
                previous_token = account.token
                account.token = token
                account.token_state = "ready"
                account.token_expires_at = 0
                account.token_renew_after = 0
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
                    resume_note = f"登录成功，但任务尚未恢复：{detail}；可稍后用 wt恢复。"
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
                raise WuwaError("账号编号不存在，请发送 wt查看 查看当前编号。")
            target = accounts[index - 1]
            if domestic is not None and target.domestic != domestic:
                raise WuwaError("该编号的区服与指定区服不同，请按 wt查看 的编号操作。")
            return target
        matches = [a for a in accounts if a.cuid == reference and (domestic is None or a.domestic == domestic)]
        if len(matches) > 1:
            raise WuwaError("该 CUID 同时绑定了国服与海外，请使用编号或在末尾指定区服。")
        if matches:
            return matches[0]
        if domestic is None and 1 <= int(reference) <= len(accounts):
            return accounts[int(reference) - 1]
        raise WuwaError("尚未绑定该账号，请发送 wt查看 查看编号或 CUID。")

    async def view_accounts(self, event: Event) -> str:
        owner = identity(event.bot_id, event.user_id)
        async with self.user_lock(owner):
            accounts = await Store.linked_accounts(owner)
            if not accounts:
                raise WuwaError("请先发送 wt登录 或 wt添加token TOKEN。")
            lines = ["已绑定的 CUID："]
            for index, account in enumerate(accounts, 1):
                status = " · token已过期" if account.token_state == "expired" else ""
                lines.append(f"[{index}] {mask_cuid(account.cuid)}（{'国服' if account.domestic else '海外'}）{status}")
        return "\n".join(lines) + "\nwt切换 [x] 切换账号；wt删除 [x] 删除绑定。"

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
                    await self.finish(task, "stopped", "已删除绑定，自动开箱已停止")
                remaining = [a for a in accounts if a.id != target.id]
                replacement = remaining[0].id if remaining else None
                await Store.delete_link(owner, target.id, replacement)
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
            + current
        )

    async def status(self, event: Event) -> str:
        if event.user_pm > 1:
            raise WuwaError("wt状态仅限 Bot 管理员使用。")
        if not self.database_ready:
            return f"WT 已停止\n{self.database_error}\n连续失败上限：{retry_limit()} 次"
        bound_people, task_people, total_chests = await Store.statistics()
        return (
            f"WT 状态\n绑定人数：{bound_people} 人\n开启任务人数：{task_people} 人（含暂停）"
            f"\n累计成功开箱：{total_chests} 个"
            f"\n连续失败上限：{retry_limit()} 次"
        )

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
            + ("\n" + TOKEN_EXPIRED_MESSAGE if target.token_state == "expired" else "")
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
            "已开启10次后通知；失败、重新登录、暂停或任务结束时也会补发已有结果。"
            if enabled
            else "已关闭10次后通知，任务结束后汇总发图；中途失败、重新登录或暂停时仍发送已开出的结果。"
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
        for (run_id, subscription_id), records in grouped.items():
            send_rest = force or task is None or task.run_id != run_id or task.status not in ("active", "paused")
            while records and (send_rest or account.subscribed and len(records) >= 10):
                # 订阅按十件推送；未订阅保留整轮通知，绘图入口按页绘制。
                size = min(len(records), 10) if account.subscribed else len(records)
                batch, records = records[:size], records[size:]
                notice = Notice(
                    subscription_id=subscription_id,
                    account_id=key,
                    text=f"{reason + ' · ' if reason else ''}本次 {len(batch)} 个 · {date_text(batch[-1].created_at)}",
                    record_ids=json.dumps([r.id for r in batch]),
                    available_clicks=info.remaining if info else None,
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
            await self.refresh(account, fresh=True)
            raise

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
                        await self.refresh(account, fresh=True)
                    raise
                except MaintenanceError:
                    raise
                except WuwaError as error:
                    logger.warning(f"[WuwaTappoUID] 兑换结果未确认：{self.error_detail(error)}")
                    return [f"兑换结果尚未确认：{error}。本次未自动重发，请用 wt背包、wt次数 核对实际变化。"]
                # 从这里起官方已确认兑换成功，后续故障不得提示成兑换失败。
                try:
                    after_info = await self.refresh(account, fresh=True)
                    after_items = await self.read_bag(account, fresh=True)
                except SQLAlchemyError as error:
                    await self.disable_database(error)
                    return ["兑换成功，但结果刷新失败。\n" + self.database_error]
                except (DatabaseWriteTimeout, WriteGateTimeout):
                    return ["兑换成功，但数据库写入超时，结果未能完成刷新。请稍后用 wt背包、wt次数 查看。"]
                except WuwaError as error:
                    logger.warning(f"[WuwaTappoUID] 兑换成功但结果刷新失败：{self.error_detail(error)}")
                    return [f"兑换成功，但结果刷新失败：{error}。请稍后用 wt背包、wt次数 查看。"]
                except Exception as error:
                    logger.exception("[WuwaTappoUID] 兑换成功后刷新出现意外错误")
                    return [f"兑换成功，但结果刷新异常（{type(error).__name__}），已记录。请用 wt背包、wt次数 查看。"]
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
            return [summary + "\n结果图绘制失败，可用 wt背包 查看。"]
        return [summary, *images]

    async def chest_statistics(self, event: Event, period: StatisticsPeriod) -> list[str | bytes]:
        async with self.user_lock(identity(event.bot_id, event.user_id)):
            _, account = await self.bound(event, allow_expired=True)
            first_at, groups = await Store.chest_history(account.id, period.since, period.until)
        stats = await asyncio.to_thread(ChestStatistics.aggregate, groups)
        return [await self.cards.chest_statistics(account, event, period, stats, first_at)]

    async def synthesize(self, event: Event, same_level: bool = False) -> list[str | bytes]:
        results: list[SynthesisResult] = []
        upgrades = same_count = spent = 0
        mode_name = "同阶" if same_level else "升阶"
        problem = ""
        bag_current = True
        run_id = new_id()
        async with self.user_lock(identity(event.bot_id, event.user_id)):
            _, account = await self.bound(event)
            async with self.account_lock(account.id):
                await self.refresh(account)
                items = await self.read_bag(account)
                owned_before = {str(item.asset_id) for item in items if item.quantity > 0}
                highest = {} if same_level else highest_tiers(await self.api.atlas())
                while not self.closing:
                    try:
                        recipe = next_recipe(items, highest, same_level=same_level)
                    except WuwaError as error:
                        problem = str(error)
                        break
                    if recipe is None:
                        break
                    if results:
                        await asyncio.sleep(1)
                    if self.closing:
                        problem = "插件正在重新加载，本轮已停止，已确认结果保留。"
                        break
                    record = SynthesisRecord(
                        run_id=run_id,
                        account_id=account.id,
                        mode="same_level" if recipe.same_level else "level_up",
                        material_ids=json.dumps(recipe.asset_ids),
                        before_quantity=sum(item.quantity for item in items),
                    )
                    await Store.save(record)
                    try:
                        result = await self.api.synthesize(account.token, recipe.asset_ids, recipe.same_level)
                    except OperationStopped as error:
                        record.state, record.error = "rejected", str(error)
                        await Store.save(record)
                        problem = str(error)
                        break
                    except MaintenanceError as error:
                        record.state, record.error = "rejected", str(error)
                        await Store.save(record)
                        problem = str(error)
                        break
                    except ApiError as error:
                        record.state, record.error = "rejected", str(error)
                        await Store.save(record)
                        if error.is_auth:
                            logger.info(
                                f"[WuwaTappoUID] CUID {mask_cuid(account.cuid)}：合成鉴权被拒绝，{error.details}。"
                            )
                        if error.is_auth:
                            try:
                                await self.refresh(account, fresh=True)
                                problem = "合成请求被拒绝，但账号查询确认token可用；本轮已停止。"
                            except WuwaError as confirmation:
                                problem = str(confirmation)
                            break
                        problem = f"服务端拒绝本次合成：{error}，本轮已停止。"
                        break
                    except WuwaError as error:
                        record.state, record.error = "uncertain", type(error).__name__
                        bag_current = False
                        await Store.save(record)
                        problem = "最后一次合成结果未确认，已停止且不会自动重发。请先用 wt背包 检查实际结果。"
                        break
                    record.result = result.model_dump_json()
                    record.state = "success"
                    await Store.save(record)
                    results.append(result)
                    same_count += int(recipe.same_level)
                    upgrades += int(not recipe.same_level)
                    spent += len(recipe.asset_ids)
                    try:
                        items = await self.read_bag(account, fresh=True)
                    except WuwaError as error:
                        bag_current = False
                        problem = f"已保存本次合成结果，但背包回读失败：{error}。本轮已停止。"
                        break
                    record.after_quantity = sum(item.quantity for item in items)
                    await Store.save(record)
                    if record.before_quantity - record.after_quantity != len(recipe.asset_ids) - 1:
                        problem = "服务端背包数量尚未与合成消耗一致，本轮已停止；请用 wt背包 核对。"
                        break
        self.wake_notices()
        summary = f"本轮已确认：升阶 {upgrades} 次，同阶 {same_count} 次。"
        summary += f"消耗多余物品 {spent} 件，获得 {len(results)} 件。"
        if not results and not problem:
            cost = 5 if same_level else 10
            return [f"没有足够的多余物品可{mode_name}合成：已跳过收藏，每种保留 1 件，需同品类同稀有度 {cost} 件。"]
        if problem:
            summary += "\n" + problem
        if not results:
            return [summary]
        try:
            images = await self.cards.synthesis(
                account,
                results,
                items if bag_current else None,
                event,
                f"升阶 {upgrades} 次 · 同阶 {same_count} 次",
                owned_before,
            )
        except Exception:
            logger.exception("[WuwaTappoUID] 合成结果已保存，但结果图绘制失败")
            return [summary + "\n结果图绘制失败，可用 wt背包 查看。"]
        return [summary, *images]

    @staticmethod
    def check_deadline(task: ChestTask | None) -> None:
        if task is not None and task.remaining() <= 0:
            raise Expired

    async def add_clicks(
        self, account: Account, number: int, task: ChestTask | None = None
    ) -> tuple[UserInfo, int, str]:
        before = await self.refresh(account)
        self.check_deadline(task)
        if self.closing:
            return before, 0, "插件正在重新加载，本次未提交点击。"
        problem = ""
        submission_error = None
        try:
            ack = await self.api.set_clicks(
                account.token, before.total_click_count + number, task.expires_at if task is not None else None
            )
            if ack not in CODE_OK:
                problem = str(ApiError(ack))
        except OperationStopped as error:
            return before, 0, str(error)
        except ApiError as error:
            if task is not None and error.code == 10014:
                raise
            problem = str(error)
        except (NetworkError, DataError) as error:
            submission_error = error
            problem = str(error)
        # 重连只提交固定目标累计值；最终仍按服务端实际变化作答。
        after = await self.refresh(account, fresh=True)
        delta = after.total_click_count - before.total_click_count
        if task is not None and delta <= 0 and submission_error is not None:
            raise submission_error
        return after, delta, problem

    async def increase(self, event: Event, number: int) -> list[str | bytes]:
        limit = max_click_increment()
        if not 1 <= number <= limit:
            raise WuwaError(f"单次增加次数须为 1～{limit:,}，请核对输入。")
        async with self.user_lock(identity(event.bot_id, event.user_id)):
            _, account = await self.bound(event)
            async with self.account_lock(account.id):
                _, delta, problem = await self.add_clicks(account, number)
        self.wake_notices()
        image = await self.cards.click_summary(account, event, number, delta)
        hint = f"提交提示：{problem}（已回读实际累计值）" if problem else ""
        return [hint, image] if hint else [image]

    def job_id(self, key: str) -> str:
        return "WuwaTappoUID:chest:" + key

    def schedule(self, task: ChestTask) -> None:
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
        scheduler.add_job(
            self.tick,
            "date",
            run_date=datetime.fromtimestamp(
                max(time.time() + 0.1, task.expires_at if task.pause_reason in ("auth", "route") else task.next_check),
                timezone.utc,
            ),
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
        if retry:
            # 失败后不使用已经过去的提醒/到期点，避免 0.1 秒循环。
            wakeup = max(now + 60, self.failures.retry_after("check:" + task.account_id))
            return min(wakeup, task.expires_at) if task.expires_at > now else wakeup
        candidates = [task.expires_at]
        if not task.reminded:
            # 剩余不超过 1 小时进入提醒与续期窗口。
            candidates.append(task.expires_at - RENEW_WINDOW)
        if task.status == "active":
            if retry or info is None:
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
                    await self.finish(task, "expired", "本轮已到期")
                if task is not None and task.status in ("active", "paused"):
                    if task.owner_id != binding.id:
                        raise WuwaError("这个账号已有其他用户开启的任务，请由任务所属用户操作。")
                    if task.status == "paused":
                        action = "任务已暂停，可发送 wt恢复 继续本轮。"
                    else:
                        action = "任务正在运行。"
                    if 0 < task.remaining(now) <= RENEW_WINDOW:
                        task.expires_at += hours * 3600
                        task.reminded = False
                        action += f"已在原到期时间上续期 {hours} 小时。"
                    await Store.save(task)
                    self.schedule(task)
                else:
                    if await Store.unresolved(account.id):
                        assert task is not None
                        info = await self.refresh(account)
                        if not await self.reconcile(task, info):
                            raise WuwaError("上轮开箱仍未能确认，已保留流水，请管理员核对后再开启。")
                    await self.refresh(account)
                    if not event.WS_BOT_ID:
                        raise WuwaError("当前会话缺少 GS WebSocket 路由，无法保存自动推送目标。")
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
                    self.schedule(task)
                    action = f"已开启 {hours} 小时自动开箱，将立即检查左右宝箱。"
                answer = action
        self.wake_notices()
        image = await self.cards.task_summary(account, task, event)
        return [answer, image]

    async def finish(self, task: ChestTask, status: str, reason: str) -> None:
        # 先持久化结束状态并撤销调度，结果核对/汇总失败不能阻止任务停止。
        task.status = status
        task.paused_at = None
        await Store.save(task)
        self.unschedule(task.account_id)
        pending = await Store.unresolved(task.account_id)
        expired_note = ""
        if pending:
            info: UserInfo | None = None
            account = await Store.account(task.account_id)
            if account is not None and account.token_state != "expired":
                try:
                    info = await self.api.me(account.token, fresh=True)
                    if str(info.cuid) != account.cuid:
                        raise DataError("结束任务核对的CUID与账号不一致。")
                except MaintenanceError:
                    pass
                except WuwaError as error:
                    if isinstance(error, ApiError) and error.is_auth:
                        await self.expire_account(account)
                        await self.failures.success("reconcile:" + account.id)
                        expired_note = "\n" + self.expired_message(account, task.owner_id)
                    elif isinstance(error, ApiError) and error.code == 10014:
                        pass
                    else:
                        await self.failures.fail(
                            "reconcile:" + account.id,
                            "结束任务的结果核对",
                            self.error_detail(error),
                            account.id,
                            limit=1 if isinstance(error, DataError) else None,
                        )
            if info is not None:
                await self.reconcile(task, info)
            else:
                for record in pending:
                    if record.result:
                        await self.record_success(task, record)
        unresolved_count = len(await Store.unresolved(task.account_id))
        await self.queue_results(task.account_id, force=True, reason=reason)
        await Store.save(
            task,
            Notice(
                subscription_id=task.subscription_id,
                account_id=task.account_id,
                text=f"{reason}。本轮成功开箱 {task.successes} 个。"
                + (f"另有 {unresolved_count} 个请求结果待确认，尚未计入。" if unresolved_count else "")
                + expired_note,
            ),
        )

    async def stop_owned(self, binding: Binding, reason: str) -> str:
        task = await Store.task(binding.account_id)
        if task is None or task.status not in ("active", "paused"):
            return "当前没有运行中的自动开箱任务，账号绑定已保留。"
        if task.owner_id != binding.id:
            raise WuwaError("任务属于绑定此账号的另一用户，请由任务所属用户停止。")
        await self.finish(task, "stopped", reason)
        return f"自动开箱已停止，账号绑定已保留。本轮成功 {task.successes} 个。"

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
                    raise WuwaError("当前没有可暂停的任务；请用 wt宝箱 开启新任务。")
                if task.owner_id != binding.id:
                    raise WuwaError("任务属于另一用户，请由任务所属用户操作。")
                if task.remaining() <= 0:
                    await self.finish(task, "expired", "本轮已到期")
                    answer = "本轮已到期，请用 wt宝箱 开启新任务。"
                elif task.timer_paused:
                    answer = "任务和计时已经暂停。\n" + task_text(task)
                else:
                    task.status = "paused"
                    task.pause_reason = "manual"
                    task.paused_at = time.time()
                    action = "宝箱任务和计时已暂停，剩余有效期保留。"
                    task.next_check = time.time()
                    await Store.save(
                        task,
                        Notice(
                            subscription_id=task.subscription_id,
                            account_id=account.id,
                            text=action + "\n" + task_text(task),
                        ),
                    )
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
                                "新登录态已被拒绝，本次未重复登录；请稍后重新发送 wt恢复。"
                            ) from None
                    except (NetworkError, DataError):
                        if not pending:
                            raise
                        return "新登录凭据已保存，验证尚未完成；请稍后发送 wt恢复 继续验证，不会重复交换token。"
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
                                "保存的SDK凭据已被拒绝，请发送 wt登录 或 wt添加token 更新登录态。"
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
                        return "新登录凭据已取得并保存，验证尚未完成；请稍后发送 wt恢复 继续验证，不会重复交换token。"
                    except ApiError as error:
                        if error.is_auth:
                            await self.expire_account(account)
                            raise AuthenticationError(
                                "新登录态已被拒绝，本次未重复登录；请稍后重新发送 wt恢复。"
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
                    await self.finish(task, "expired", "本轮已到期")
                    answer = prefix + "原任务已到期，可发送 wt宝箱 开启新任务。"
                elif task.status == "active":
                    answer = prefix + "宝箱任务正在运行。"
                elif not await self.reconcile(task, info):
                    answer = prefix + "开箱结果仍待确认，任务保持暂停，可稍后再次 wt恢复。"
                elif task.remaining() <= 0:
                    await self.finish(task, "expired", "本轮已到期")
                    answer = prefix + "原任务已到期，可发送 wt宝箱 开启新任务。"
                else:
                    route = await Store.subscription(task.subscription_id)
                    if task.pause_reason == "route" or route is None or not route.WS_BOT_ID:
                        async with self.subscription(event, "WuwaTappo宝箱", new_id()) as subscription_id:
                            task.subscription_id = subscription_id
                            await Store.save(task)
                    if task.paused_at is not None:
                        task.expires_at += time.time() - task.paused_at
                    task.paused_at = None
                    task.status = "active"
                    task.pause_reason = ""
                    await self.failures.reset_account(account.id)
                    task.next_check = self.next_wakeup(task, info)
                    await Store.save(
                        task,
                        Notice(
                            subscription_id=task.subscription_id,
                            account_id=account.id,
                            text="登录态已确认，宝箱任务已恢复，继续原轮次。",
                        ),
                    )
                    self.schedule(task)
                    answer = prefix + "宝箱任务已恢复，继续原轮次。\n" + task_text(task)
        self.wake_notices()
        return answer

    async def record_success(self, task: ChestTask, record: ChestRecord) -> None:
        record.state = "success"
        if task.run_id == record.run_id:
            task.successes += 1
        await Store.save(record, task)
        await self.failures.success("check:" + task.account_id)
        await self.queue_results(record.account_id)

    async def reconcile(self, task: ChestTask, info: UserInfo) -> bool:
        unresolved = await Store.unresolved(task.account_id)
        clear = True
        now = time.time()
        for record in unresolved:
            before = UserInfo.model_validate_json(record.before_info)
            side: Side = "left" if record.side == "left" else "right"
            if record.result or info.cooldown(side) > before.cooldown(side):
                await self.record_success(task, record)
            elif now - record.created_at >= SETTLE_SECONDS:
                # 成功开箱一定会推进该侧冷却；早已超时仍未推进，说明请求没有生效（包括没发出去）。
                record.state = "not_applied"
                await Store.save(
                    record,
                    Notice(
                        subscription_id=record.subscription_id,
                        account_id=record.account_id,
                        text=f"{SIDE_NAMES[side]}宝箱上次请求没有生效（该侧冷却未变化）。"
                        + ("将按重试次数限制继续处理。" if task.status == "active" else "本次仅核对结果，不重新开箱。"),
                    ),
                )
            else:
                clear = False
                if record.state != "uncertain":
                    record.state = "uncertain"
                    await Store.save(
                        record,
                        Notice(
                            subscription_id=record.subscription_id,
                            account_id=record.account_id,
                            text=f"{SIDE_NAMES[side]}宝箱结果尚未确认，暂缓开箱并继续查询，避免重复请求。",
                        ),
                    )
        if unresolved:
            # 中断后迟到的成功结果也立刻补发，不等下一次十件或到期。
            await self.queue_results(task.account_id, force=True, reason="开箱请求核对结果")
        return clear

    async def open_side(self, account: Account, task: ChestTask, side: Side, info: UserInfo) -> UserInfo:
        recent = await Store.recent_chests(account.id, task.run_id, side)
        failed = 0
        for record in recent:
            if record.state != "not_applied":
                break
            failed += 1
        if failed == 4:
            task.status = "paused"
            task.pause_reason = "failure"
            # 留下终止标记，用户恢复后才开始新的重试批次；不新增数据库列。
            recent[0].state = "retry_exhausted"
            message = f"{SIDE_NAMES[side]}宝箱首次请求及 3 次重试均未生效，已暂停；检查网络后可发送 wt恢复。"
            await Store.save(
                recent[0],
                task,
                Notice(subscription_id=task.subscription_id, account_id=account.id, text=message),
            )
            raise NetworkError(message)
        if failed:
            logger.warning(f"[WuwaTappoUID] {SIDE_NAMES[side]}宝箱已确认前次未生效，开始第 {failed}/3 次重试。")
        supplement = ""
        # 最多一次自动补充、一次重试；每次真正发请求前都检查到期时间。
        for attempt in range(2):
            if self.closing:
                return info
            self.check_deadline(task)
            record = ChestRecord(
                account_id=account.id,
                run_id=task.run_id,
                subscription_id=task.subscription_id,
                side=side,
                before_info=info.model_dump_json(),
                supplement=supplement,
            )
            await Store.save(record)
            if self.closing or time.time() >= task.expires_at:
                record.state = "not_sent"
                await Store.save(record)
                if self.closing:
                    return info
                raise Expired
            try:
                result = await self.api.open(account.token, side, info.total_click_count)
            except OperationStopped:
                record.state = "not_sent"
                await Store.save(record)
                return info
            except MaintenanceError as error:
                record.state = "rejected"
                record.result = str(error)
                await Store.save(record)
                raise
            except ApiError as error:
                record.state = "rejected"
                record.result = str(error)
                await Store.save(record)
                if error.code not in (70005, 70006):
                    await self.queue_results(account.id, force=True, reason="开箱失败，汇总已开结果")
                if error.code == 70005 and attempt == 0:
                    info, delta, problem = await self.add_clicks(account, AUTO_CLICKS, task)
                    supplement = f"自动补充请求 {AUTO_CLICKS} 点击，服务端累计值实际变化 {delta:+d}。"
                    if problem:
                        supplement += f"提交提示：{problem}。"
                    record.supplement = supplement
                    await Store.save(record)
                    if delta > 0:
                        continue
                    raise ClicksUnavailable(supplement + "本次未重试开箱。")
                if error.code == 70005:
                    raise ClicksUnavailable(supplement + "再次开箱仍提示点击不足，本轮停止重试。")
                if error.is_auth:
                    # 先验证原会话，本次不再次提交开箱。
                    await self.refresh(account, fresh=True)
                    raise WuwaError("开箱请求被拒绝，但token仍可查询；本次停止开箱。") from None
                if supplement:
                    await self.account_notice(account, f"{SIDE_NAMES[side]}宝箱重试未成功：{error}。\n{supplement}")
                raise
            except (NetworkError, DataError):
                # 冷却推进就结算成功；120 秒后确认未生效才重试，累计最多初次加 3 次。
                await self.queue_results(account.id, force=True, reason="开箱响应中断")
                fresh = await self.refresh(account, fresh=True)
                await self.reconcile(task, fresh)
                return fresh
            record.result = result.model_dump_json()
            await Store.save(record)
            fresh = await self.refresh(account, fresh=True)
            await self.record_success(task, record)
            return fresh
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
                if task.remaining() <= 0:
                    await self.finish(task, "expired", "本轮自动开箱已结束")
                    return
                if await self.pause_invalid_route(task):
                    return
                if task.status == "paused" and task.pause_reason in ("auth", "route"):
                    return
                if not task.reminded and task.expires_at - time.time() <= RENEW_WINDOW:
                    task.reminded = True
                    await Store.save(
                        task,
                        Notice(
                            subscription_id=task.subscription_id,
                            account_id=key,
                            text="自动开箱剩余不超过 1 小时，可发送 wt宝箱"
                            f" 在原到期时间上续期 {chest_task_hours()} 小时。"
                            f"\n到期：{date_text(task.expires_at)}",
                        ),
                    )
                if task.status == "paused":
                    return
                if not self.failures.allows("check:" + key):
                    retry = True
                    return
                account = await Store.account(key)
                if account is None:
                    raise DataError("任务关联的账号数据缺失。")
                info = await self.refresh(account)
                if not await self.reconcile(task, info):
                    await self.failures.success("check:" + key)
                    retry = True
                    return
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
                            info = await self.refresh(account)
                            retry = True
                            continue
                        if error.code in (70004, 10014):
                            retry = True
                            break
                        raise
                    if await Store.unresolved(key):
                        retry = True
                        break
                await self.failures.success("check:" + key)
            except OperationStopped:
                return
            except Expired:
                await self.finish(task, "expired", "本轮自动开箱已结束")
            except MaintenanceError:
                retry = True
            except ClicksUnavailable as error:
                interrupted = True
                task.status = "paused"
                task.pause_reason = "failure"
                task.paused_at = None
                await self.failures.success("check:" + key)
                await Store.save(
                    task,
                    Notice(
                        subscription_id=task.subscription_id,
                        account_id=key,
                        text=f"点击不足，自动开箱已暂停。{error}\n请先补足点击，再发送 wt恢复。",
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
                            try:
                                await self.refresh(account, fresh=True)
                            except AuthenticationError:
                                task.status = "paused"
                                task.pause_reason = "auth"
                                await Store.save(task)
                                self.unschedule(key)
                            else:
                                # 业务请求拒绝但账号仍可查询，不标记token过期。
                                await self.task_failure(task, error)
                                retry = True
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
                    if self.database_ready and interrupted:
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
                        task.next_check = max(time.time() + 60, self.failures.retry_after("save:" + key))
                        self.schedule(task)

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
                task.next_check = time.time()
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
        """每分钟执行：补排丢失了下次检查的任务，再补发待提交的通知。"""
        if self.closing or not self.database_ready:
            return
        # 冷却期内不重试；达到告警阈值后冷却结束仍继续尝试，成功即恢复，不必等重启。
        if self.failures.retry_after("maintain") > time.time():
            return
        try:
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
                    task.next_check = time.time()
                    self.schedule(task)
                    await self.failures.success("restore:" + key)
            for key in await Store.unresolved_accounts():
                if self.account_lock(key).locked() or not self.failures.allows("reconcile:" + key):
                    continue
                async with self.account_lock(key):
                    task = await Store.task(key)
                    if task is None or task.status in ("active", "paused"):
                        continue
                    account = await Store.account(key)
                    if account is None or account.token_state == "expired":
                        continue
                    try:
                        # 结束后只查询原登录态，不重新登录、不提交点击或开箱。
                        info = await self.api.me(account.token, fresh=True)
                        if str(info.cuid) != account.cuid:
                            raise DataError("核对返回的 CUID 与账号不一致。")
                        account.info = info.model_dump_json()
                        account.verified_at = time.time()
                        await Store.save(account)
                        await self.reconcile(task, info)
                        await self.failures.success("reconcile:" + key)
                    except SQLAlchemyError:
                        raise
                    except MaintenanceError:
                        continue
                    except Exception as error:
                        if isinstance(error, ApiError) and error.is_auth:
                            await self.expire_account(account)
                            await self.failures.success("reconcile:" + key)
                            continue
                        if isinstance(error, ApiError) and error.code == 10014:
                            continue
                        await self.failures.fail(
                            "reconcile:" + key,
                            "已结束任务的结果核对",
                            self.error_detail(error),
                            key,
                            limit=1 if isinstance(error, DataError) else None,
                        )
            for key in await Store.result_accounts():
                if self.account_lock(key).locked() or not self.failures.allows("results:" + key):
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
                        await self.failures.fail("results:" + key, "开箱结果汇总", self.error_detail(error), key)
        self.wake_notices()

    def release_notice_image(self, key: str) -> None:
        if key in self._notice_images:
            self._notice_image_bytes -= sum(map(len, self._notice_images.pop(key).body))

    def cached_notice_image(self, key: str) -> tuple[bytes, ...] | None:
        if key not in self._notice_images:
            return None
        cached = self._notice_images[key]
        if cached.revision != self.resources.revision or cached.bot_name != bot_name():
            self.release_notice_image(key)
            return None
        self._notice_images.move_to_end(key)
        return cached.body

    def cache_notice_image(self, key: str, revision: int, name: str, body: tuple[bytes, ...]) -> None:
        self.release_notice_image(key)
        size = sum(map(len, body))
        if size > NOTICE_CACHE_BYTES:
            return
        while self._notice_images and self._notice_image_bytes + size > NOTICE_CACHE_BYTES:
            _, old = self._notice_images.popitem(last=False)
            self._notice_image_bytes -= sum(map(len, old.body))
        self._notice_images[key] = CachedNoticeImage(revision, name, body)
        self._notice_image_bytes += size

    async def notice_messages(
        self, notice: Notice, event: Event, account: Account | None, contexts: dict[str, ResultContext]
    ) -> list[Message]:
        notice_text = notice.text.replace(account.cuid, mask_cuid(account.cuid)) if account is not None else notice.text
        if notice.record_ids:
            if account is None:
                raise DataError("开箱通知对应的账号不存在。")
            rendered = self.cached_notice_image(notice.id)
            if rendered is None:
                revision = self.resources.revision
                name = bot_name()
                records = await Store.records(json.loads(notice.record_ids))
                rendered = await self.cards.results(records, notice.available_clicks, notice_text, contexts[account.id])
                self.cache_notice_image(notice.id, revision, name, rendered)
            return [MessageSegment.image(page) for page in rendered]
        label = f"CUID {mask_cuid(account.cuid)}：\n" if account is not None else ""
        messages = [MessageSegment.text(label + notice_text + "\n")]
        if notice.asset_id:
            try:
                async with asyncio.timeout(20):
                    tile = await self.resources.item_image(notice.asset_id, notice.rarity)
            except TimeoutError:
                logger.warning("[WuwaTappoUID] 物品图获取超时，本条只发文字。")
                tile = None
            if tile is not None:
                messages.append(MessageSegment.image(tile))
        return messages

    @tracked_method
    async def flush_notices(self) -> None:
        if self.closing or not self.database_ready:
            return
        # 冷却期内不重试；达到告警阈值后冷却结束仍继续尝试，成功即恢复，不必等重启。
        if self.failures.retry_after("notice_loop") > time.time():
            return
        try:
            await self._flush_notices()
            await self.failures.success("notice_loop")
        except SQLAlchemyError as error:
            await self.disable_database(error)
        except MaintenanceError:
            return
        except Exception as error:
            if self.failures.attempts("notice_loop") == 0:
                logger.exception("[WuwaTappoUID] 通知巡检出现意外错误")
            await self.failures.fail("notice_loop", "通知巡检", self.error_detail(error))

    async def _flush_notices(self) -> None:
        if self._push_lock.locked():
            self._notice_again = True
            return
        async with self._push_lock:
            self._notice_again = False
            semaphore = asyncio.Semaphore(4)

            async def prepare(key: str) -> None:
                async with semaphore:
                    await self._flush_account_notices(key)

            cursor = None
            until = time.time()
            while not self.closing:
                accounts = await Store.notice_accounts(until, after=cursor)
                if not accounts:
                    break
                outcomes = await asyncio.gather(*(prepare(key) for key, _ in accounts), return_exceptions=True)
                for (key, _), outcome in zip(accounts, outcomes, strict=True):
                    if isinstance(outcome, SQLAlchemyError):
                        await self.disable_database(outcome)
                        return
                    if isinstance(outcome, Exception):
                        await self.failures.fail(
                            "notice-account:" + key, "账号通知处理", self.error_detail(outcome), key
                        )
                last_key, last_time = accounts[-1]
                cursor = last_time, last_key
        if self._notice_again:
            self.wake_notices(5)

    async def _flush_account_notices(self, key: str) -> None:
        if self.closing or not self.database_ready:
            return
        if not self.failures.allows("notice-account:" + key):
            return
        batch = await Store.notices(key)
        groups: dict[int, list[Notice]] = {}
        for notice in batch:
            groups.setdefault(notice.subscription_id, []).append(notice)
        available: dict[int, tuple[Event, list[Notice]]] = {}
        for subscription_id, notices in groups.items():
            sub = await Store.subscription(subscription_id)
            if sub is None or not sub.WS_BOT_ID:
                ended = await Store.mark_unroutable(subscription_id)
                if ended:
                    for notice in ended:
                        self.release_notice_image(notice.id)
                    await self.failures.fail(
                        f"route:{subscription_id}",
                        "通知路由失效",
                        "通知已标记无法投递，原始结果保留至归档。",
                        key,
                        limit=1,
                    )
                if key and not self.account_lock(key).locked():
                    async with self.account_lock(key):
                        task = await Store.task(key)
                        if task is not None and task.subscription_id == subscription_id:
                            await self.pause_invalid_route(task)
                continue
            if sub.WS_BOT_ID not in gss.active_bot:
                continue
            event = Event(
                bot_id=sub.bot_id,
                user_id=sub.user_id,
                bot_self_id=sub.bot_self_id,
                user_type=sub.user_type,
                group_id=sub.group_id,
                WS_BOT_ID=sub.WS_BOT_ID,
            )
            available[subscription_id] = event, notices
        if not available:
            return
        account = await Store.account(key) if key else None
        progress = None
        unchecked = [notice for notice in batch if notice.record_ids and not notice.new_checked]
        needs_context = any(notice.record_ids for _, notices in available.values() for notice in notices)
        if needs_context:
            if account is None:
                raise DataError("开箱通知对应的账号不存在。")
            if self.account_lock(key).locked():
                self._notice_again = True
                return
            async with self.account_lock(key):
                account = await Store.account(key)
                if account is None:
                    raise DataError("开箱通知对应的账号不存在。")
                unchecked = [
                    notice for notice in await Store.notices(key) if notice.record_ids and not notice.new_checked
                ]
                try:
                    if account.token_state in ("expired", "recovering"):
                        items = None
                        await self.failures.success("notice-bag:" + key)
                    elif not self.failures.allows("notice-bag:" + key):
                        if not self.failures.blocked("notice-bag:" + key):
                            return
                        items = None
                    else:
                        async with asyncio.timeout(12):
                            items = await self.read_bag(account, fresh=True)
                    if items is not None:
                        progress = self.cards.progress(items)
                        owned = {str(item.asset_id) for item in items if item.quantity > 0}
                        if unchecked:
                            await Store.prepare_new_items(key, [notice.id for notice in unchecked], owned)
                        await self.failures.success("notice-bag:" + key)
                except MaintenanceError:
                    return
                except AuthenticationError:
                    await self.failures.success("notice-bag:" + key)
                except (WuwaError, TimeoutError) as error:
                    if isinstance(error, ApiError) and error.code == 10014:
                        return
                    failure = await self.failures.fail(
                        "notice-bag:" + key,
                        "通知背包快照",
                        self.error_detail(error),
                        key,
                    )
                    if unchecked and not failure.blocked:
                        return
        for subscription_id, (event, notices) in available.items():
            gate = self._notice_sends.setdefault(subscription_id, NoticeSendGate(asyncio.Lock()))
            gate.users += 1
            try:
                async with gate.lock:
                    await self._send_notice_group(
                        key, subscription_id, event, notices, account, progress, bool(unchecked)
                    )
            finally:
                gate.users -= 1
                if not gate.users:
                    del self._notice_sends[subscription_id]
        await self.failures.success("notice-account:" + key)

    async def _send_notice_group(
        self,
        key: str,
        subscription_id: int,
        event: Event,
        notices: list[Notice],
        account: Account | None,
        progress: list[Progress] | None,
        unchecked: bool,
    ) -> None:
        if self.closing:
            return
        send_key = f"notice:{key}:{subscription_id}"
        render_key = f"render:{key}:{subscription_id}"
        if not self.failures.allows(send_key):
            return
        if not self.failures.blocked(render_key) and not self.failures.allows(render_key):
            return
        contexts = {}
        contents: list[Message] = []
        skip_image = unchecked and (
            self.failures.blocked("notice-bag:" + key)
            or account is not None
            and account.token_state in ("expired", "recovering")
        )
        if not self.failures.blocked(render_key) and not skip_image:
            try:
                if any(notice.record_ids for notice in notices) and account is not None:
                    contexts[key] = ResultContext(await self.cards.header(account, None, event), progress)
                for notice in notices:
                    contents.extend(await self.notice_messages(notice, event, account, contexts))
                await self.failures.success(render_key)
            except SQLAlchemyError:
                raise
            except MaintenanceError:
                return
            except Exception as error:
                if self.failures.attempts(render_key) == 0:
                    logger.exception("[WuwaTappoUID] 通知图片绘制失败")
                failure = await self.failures.fail(render_key, "通知图片绘制", self.error_detail(error), key)
                if not failure.blocked:
                    return
        if not contents or self.failures.blocked(render_key):
            fallback = "结果图片暂不可用，本组改为文字通知；开箱流水已保留。\n"
            if account is not None and account.token_state == "expired":
                fallback += self.expired_message(account, identity(event.bot_id, event.user_id)) + "\n"
            contents = [MessageSegment.text(fallback)]
            for notice in notices:
                text = notice.text.replace(account.cuid, mask_cuid(account.cuid)) if account else notice.text
                contents.append(MessageSegment.text(text + "\n"))
        messages = [MessageSegment.node(contents)] if sum(m.type == "image" for m in contents) > 1 else contents
        if event.user_type == "group":
            messages = [MessageSegment.at(event.user_id), MessageSegment.text("\n"), *messages]
        ids = [notice.id for notice in notices]
        await Store.notice_attempts(ids)
        try:
            sub = await Store.subscription(subscription_id)
            if sub is None or not sub.WS_BOT_ID:
                self._notice_again = True
                return
            async with asyncio.timeout(15):
                submitted = await sub.send(messages)
            if submitted != 0:
                raise NetworkError("GS 未能提交推送，Bot可能离线。")
        except SQLAlchemyError:
            raise
        except Exception as error:
            await Store.notice_error(ids, f"推送异常：{type(error).__name__}")
            await self.failures.fail(send_key, "结果通知发送", self.error_detail(error), key)
            return
        await Store.mark_submitted(ids)
        await self.failures.success(send_key)
        for notice in notices:
            self.release_notice_image(notice.id)
