"""通知组装、缓存和发送；共享 Service 的锁与持久化状态。"""

import asyncio
import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from gsuid_core.gss import gss
from gsuid_core.logger import logger
from gsuid_core.models import Event, Message
from gsuid_core.segment import MessageSegment
from sqlalchemy.exc import SQLAlchemyError

from .cards import ResultContext
from .command_text import command_prefix
from .database import Account, Notice, Store
from .diagnostics import measure
from .formatting import mask_cuid
from .render_common import Progress
from .schema import ApiError, AuthenticationError, DataError, MaintenanceError, NetworkError, WuwaError
from .settings import bot_name

if TYPE_CHECKING:
    from .service import Service

NOTICE_CACHE_BYTES = 32 * 1024 * 1024


@dataclass
class CachedNoticeImage:
    revision: int
    bot_name: str
    prefix: str
    body: tuple[bytes, ...]


@dataclass
class NoticeSendGate:
    lock: asyncio.Lock
    users: int = 0


def release_notice_image(service: "Service", key: str) -> None:
    if key in service._notice_images:
        service._notice_image_bytes -= sum(map(len, service._notice_images.pop(key).body))


def cached_notice_image(service: "Service", key: str) -> tuple[bytes, ...] | None:
    if key not in service._notice_images:
        return None
    cached = service._notice_images[key]
    if (
        cached.revision != service.resources.revision
        or cached.bot_name != bot_name()
        or cached.prefix != command_prefix()
    ):
        service.release_notice_image(key)
        return None
    service._notice_images.move_to_end(key)
    return cached.body


def cache_notice_image(
    service: "Service", key: str, revision: int, name: str, prefix: str, body: tuple[bytes, ...]
) -> None:
    service.release_notice_image(key)
    size = sum(map(len, body))
    if size > NOTICE_CACHE_BYTES:
        return
    while service._notice_images and service._notice_image_bytes + size > NOTICE_CACHE_BYTES:
        _, old = service._notice_images.popitem(last=False)
        service._notice_image_bytes -= sum(map(len, old.body))
    service._notice_images[key] = CachedNoticeImage(revision, name, prefix, body)
    service._notice_image_bytes += size


async def notice_messages(
    service: "Service", notice: Notice, event: Event, account: Account | None, contexts: dict[str, ResultContext]
) -> list[Message]:
    notice_text = notice.text.replace(account.cuid, mask_cuid(account.cuid)) if account is not None else notice.text
    if notice.record_ids:
        if account is None:
            raise DataError("开箱通知对应的账号不存在。")
        rendered = service.cached_notice_image(notice.id)
        if rendered is None:
            revision = service.resources.revision
            name = bot_name()
            prefix = command_prefix()
            records = await Store.records(json.loads(notice.record_ids))
            rendered = await service.cards.results(records, notice.available_clicks, notice_text, contexts[account.id])
            service.cache_notice_image(notice.id, revision, name, prefix, rendered)
        return [MessageSegment.image(page) for page in rendered]
    label = f"CUID {mask_cuid(account.cuid)}：\n" if account is not None else ""
    messages = [MessageSegment.text(label + notice_text + "\n")]
    if notice.asset_id:
        try:
            async with asyncio.timeout(20):
                tile = await service.resources.item_image(notice.asset_id, notice.rarity)
        except TimeoutError:
            logger.warning("[WuwaTappoUID] 物品图获取超时，本条只发文字。")
            tile = None
        if tile is not None:
            messages.append(MessageSegment.image(tile))
    return messages


async def flush_notices(service: "Service") -> None:
    if service.closing or not service.database_ready:
        return
    # 冷却期内不重试；达到告警阈值后冷却结束仍继续尝试，成功即恢复，不必等重启。
    if service.failures.retry_after("notice_loop") > time.time():
        return
    try:
        with measure("notice-loop"):
            await service._flush_notices()
        await service.failures.success("notice_loop")
    except SQLAlchemyError as error:
        await service.disable_database(error)
    except MaintenanceError:
        return
    except Exception as error:
        if service.failures.attempts("notice_loop") == 0:
            logger.exception("[WuwaTappoUID] 通知巡检出现意外错误")
        await service.failures.fail("notice_loop", "通知巡检", service.error_detail(error), blocked_delay=900)


async def _flush_notices(service: "Service") -> None:
    if service._push_lock.locked():
        service._notice_again = True
        return
    async with service._push_lock:
        service._notice_again = False
        semaphore = asyncio.Semaphore(4)

        async def prepare(key: str) -> None:
            async with semaphore:
                await service._flush_account_notices(key)

        cursor = None
        until = time.time()
        while not service.closing:
            accounts = await Store.notice_accounts(until, after=cursor)
            if not accounts:
                break
            outcomes = await asyncio.gather(*(prepare(key) for key, _ in accounts), return_exceptions=True)
            for (key, _), outcome in zip(accounts, outcomes, strict=True):
                if isinstance(outcome, SQLAlchemyError):
                    await service.disable_database(outcome)
                    return
                if isinstance(outcome, Exception):
                    await service.failures.fail(
                        "notice-account:" + key, "账号通知处理", service.error_detail(outcome), key, blocked_delay=900
                    )
            last_key, last_time = accounts[-1]
            cursor = last_time, last_key
    if service._notice_again:
        service.wake_notices(5)


async def _flush_account_notices(service: "Service", key: str) -> None:
    if service.closing or not service.database_ready:
        return
    if not service.failures.cooldown_ready("notice-account:" + key):
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
                    service.release_notice_image(notice.id)
                await service.failures.fail(
                    f"route:{subscription_id}",
                    "通知路由失效",
                    "通知已标记无法投递，原始结果保留至归档。",
                    key,
                    limit=1,
                )
            if key and not service.account_lock(key).locked():
                async with service.account_lock(key):
                    task = await Store.task(key)
                    if task is not None and task.subscription_id == subscription_id:
                        await service.pause_invalid_route(task)
            continue
        # 重连后的旧标识交给 Subscribe.send 按平台回退并修正路由。
        if sub.WS_BOT_ID not in gss.active_bot and not any(bot.bot_id == sub.bot_id for bot in gss.active_bot.values()):
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
        if service.account_lock(key).locked():
            service._notice_again = True
            return
        async with service.account_lock(key):
            account = await Store.account(key)
            if account is None:
                raise DataError("开箱通知对应的账号不存在。")
            unchecked = [notice for notice in await Store.notices(key) if notice.record_ids and not notice.new_checked]
            try:
                if account.token_state in ("expired", "recovering"):
                    items = None
                    await service.failures.success("notice-bag:" + key)
                elif not service.failures.allows("notice-bag:" + key):
                    if not service.failures.blocked("notice-bag:" + key):
                        return
                    items = None
                else:
                    async with asyncio.timeout(12):
                        items = await service.read_bag(account, fresh=True)
                if items is not None:
                    progress = service.cards.progress(items)
                    owned = {str(item.asset_id) for item in items if item.quantity > 0}
                    if unchecked:
                        await Store.prepare_new_items(key, [notice.id for notice in unchecked], owned)
                    await service.failures.success("notice-bag:" + key)
            except MaintenanceError:
                return
            except AuthenticationError:
                await service.failures.success("notice-bag:" + key)
                # 失效通知在本轮取出通知之后才生成，稍后再巡检一次把它发出去。
                service._notice_again = True
            except (WuwaError, TimeoutError) as error:
                if isinstance(error, ApiError) and error.code == 10014:
                    return
                failure = await service.failures.fail(
                    "notice-bag:" + key,
                    "通知背包快照",
                    service.error_detail(error),
                    key,
                )
                if unchecked and not failure.blocked:
                    return
    # 登录态失效时拿不到背包快照：待判 NEW 的结果留到登录恢复后再出图，其余通知照常发送。
    waiting = account is not None and account.token_state in ("expired", "recovering")
    fallback_sent = False
    for subscription_id, (event, notices) in available.items():
        if waiting:
            notices = [notice for notice in notices if not notice.record_ids or notice.new_checked]
            if not notices:
                continue
        gate = service._notice_sends.setdefault(subscription_id, NoticeSendGate(asyncio.Lock()))
        gate.users += 1
        try:
            async with gate.lock:
                if await service._send_notice_group(
                    key, subscription_id, event, notices, account, progress, bool(unchecked) and not waiting
                ):
                    fallback_sent = True
        finally:
            gate.users -= 1
            if not gate.users:
                del service._notice_sends[subscription_id]
    if fallback_sent:
        # 文字兜底已送达：清掉背包快照的失败计数，下一组结果重新尝试出图。
        await service.failures.reset("notice-bag:" + key)
    await service.failures.success("notice-account:" + key)


async def _send_notice_group(
    service: "Service",
    key: str,
    subscription_id: int,
    event: Event,
    notices: list[Notice],
    account: Account | None,
    progress: list[Progress] | None,
    unchecked: bool,
) -> bool:
    """发送一组通知；返回本组是否以文字兜底送达。"""
    if service.closing:
        return False
    send_key = f"notice:{key}:{subscription_id}"
    render_key = f"render:{key}:{subscription_id}"
    if not service.failures.cooldown_ready(send_key):
        return False
    if not service.failures.blocked(render_key) and not service.failures.allows(render_key):
        return False
    contexts = {}
    contents: list[Message] = []
    if not service.failures.blocked(render_key) and not (unchecked and service.failures.blocked("notice-bag:" + key)):
        try:
            if any(notice.record_ids for notice in notices) and account is not None:
                contexts[key] = ResultContext(await service.cards.header(account, None, event), progress)
            for notice in notices:
                contents.extend(await service.notice_messages(notice, event, account, contexts))
            await service.failures.success(render_key)
        except SQLAlchemyError:
            raise
        except MaintenanceError:
            return False
        except Exception as error:
            if service.failures.attempts(render_key) == 0:
                logger.exception("[WuwaTappoUID] 通知图片绘制失败")
            failure = await service.failures.fail(render_key, "通知图片绘制", service.error_detail(error), key)
            if not failure.blocked:
                return False
    fallback = not contents or service.failures.blocked(render_key)
    if fallback:
        contents = [MessageSegment.text("结果图片暂不可用，本组改为文字通知；开箱流水已保留。\n")]
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
            service._notice_again = True
            return False
        with measure("notice-send", debug=True):
            async with asyncio.timeout(15):
                submitted = await sub.send(messages)
            if submitted != 0:
                raise NetworkError("GS 未能提交推送，Bot可能离线。")
    except SQLAlchemyError:
        raise
    except Exception as error:
        await Store.notice_error(ids, f"推送异常：{type(error).__name__}")
        await service.failures.fail(send_key, "结果通知发送", service.error_detail(error), key, blocked_delay=900)
        return False
    await Store.mark_submitted(ids)
    await service.failures.success(send_key)
    for notice in notices:
        service.release_notice_image(notice.id)
    if fallback:
        # 文字兜底已送达：清掉绘图的失败计数，下一组重新尝试出图。
        await service.failures.reset(render_key)
    return fallback
