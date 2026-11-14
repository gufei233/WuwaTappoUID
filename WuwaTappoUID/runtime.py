"""插件生命周期；导入本模块不会调用游戏接口。"""

import asyncio
import time

from gsuid_core.aps import scheduler
from gsuid_core.data_store import get_res_path
from gsuid_core.gss import gss
from gsuid_core.logger import logger
from gsuid_core.server import on_core_shutdown, on_core_start, on_core_start_before
from gsuid_core.shutdown import shutdown_event
from gsuid_core.utils.database.base_models import DatabaseWriteTimeout
from gsuid_core.utils.database.write_gate import WriteGateTimeout
from pydantic import TypeAdapter, ValidationError
from sqlalchemy.exc import SQLAlchemyError

from .utils.api import WuwaAPI, with_read_cache
from .utils.database import Account, AccountLink, Binding, Store, ensure_database_schema
from .utils.failures import ALERT_JOB
from .utils.lifecycle import RuntimeOwner
from .utils.login import LoginManager
from .utils.resources import Resources
from .utils.schema import DatabaseSchemaError, DataError, MaintenanceError, SessionImport, WuwaError, token_expiry
from .utils.service import Service, account_id, identity
from .utils.settings import retry_limit
from .utils.status import register_status_panel

ROOT = get_res_path("WuwaTappoUID")
api = WuwaAPI()
resources = Resources(api, ROOT)
service = Service(api, resources)
login = LoginManager(service)
owner = RuntimeOwner(service, api, resources, login)
NOTICE_JOB = "WuwaTappoUID:notice"
MAINTAIN_JOB = "WuwaTappoUID:maintain"
RESOURCE_JOB = "WuwaTappoUID:resources"
HISTORY_JOB = "WuwaTappoUID:history"
_initialize_lock = asyncio.Lock()
_initialize_attempted = False


async def import_sessions() -> None:
    path = ROOT / "session-import.json"
    if not path.exists():
        return
    try:
        entries = TypeAdapter(list[SessionImport]).validate_json(await asyncio.to_thread(path.read_text, "utf-8"))
    except (OSError, ValidationError):
        raise DataError("登录态导入文件无法读取或格式错误；请按模板填写，文件已保留。") from None
    # 先验证全部登录态；任一无效时不写入任何绑定。
    verified: list[Account] = []
    for entry in entries:
        info = await api.me(entry.token)
        if str(info.cuid) != entry.cuid:
            raise DataError("导入的登录态与 CUID 不一致；文件已保留。")
        verified.append(
            Account(
                id=account_id(entry.cuid, entry.domestic),
                cuid=entry.cuid,
                domestic=entry.domestic,
                token=entry.token,
                info=info.model_dump_json(),
                verified_at=time.time(),
                token_expires_at=token_expiry(entry.expire_time),
            )
        )
    for entry, account in zip(entries, verified, strict=True):
        owner = identity(entry.platform, entry.user_id)
        old_account = await Store.account(account.id)
        if old_account is not None:
            account.subscribed = old_account.subscribed
            if old_account.sdk_owner_id != owner:
                account.sdk_token = old_account.sdk_token
                account.sdk_openid = old_account.sdk_openid
                account.sdk_username = old_account.sdk_username
                account.sdk_expires_at = old_account.sdk_expires_at
                account.sdk_owner_id = old_account.sdk_owner_id
        await Store.save(
            account,
            Binding(id=owner, platform=entry.platform, user_id=entry.user_id, account_id=account.id),
            AccountLink(owner_id=owner, account_id=account.id),
        )
        if old_account is not None and old_account.token != account.token:
            api.retire_token(old_account.token)
        await service.failures.reset_account(account.id)
    # 成功后移除明文导入文件；token 只保存在 GS 数据库中。
    await asyncio.to_thread(path.unlink)
    logger.info(f"[WuwaTappoUID] 已导入 {len(entries)} 个登录态，导入文件已移除。")


@on_core_start_before(priority=0)
async def wt_initialize() -> None:
    await owner.activate()


async def initialize() -> None:
    global _initialize_attempted
    async with _initialize_lock:
        if _initialize_attempted:
            return
        _initialize_attempted = True
        try:
            await service.failures.load()
            logger.info(f"[WuwaTappoUID] 已加载待发送告警，后台连续异常上限 {retry_limit()} 次（本次运行计数）。")
            await ensure_database_schema()
            await login.maintain_sessions(startup=True)
            await api.start()
            await resources.load()
        except (SQLAlchemyError, DatabaseSchemaError) as error:
            await service.disable_database(error)
            return
        except Exception as error:
            logger.exception("[WuwaTappoUID] 插件初始化失败")
            service.database_error = "插件初始化失败，后台任务未启动，请检查首次异常并重启。"
            await service.failures.fail("initialize", "插件初始化", service.error_detail(error), limit=1)
            return
        service.database_ready = True
        service.database_error = ""
        await service.failures.success("database")
        await service.failures.success("initialize")
        try:
            await import_sessions()
        except SQLAlchemyError as error:
            await service.disable_database(error)
        except (WuwaError, OSError, DatabaseWriteTimeout, WriteGateTimeout) as error:
            detail = service.error_detail(error)
            logger.error(f"[WuwaTappoUID] 登录态导入失败，文件已保留，已有账号与任务继续运行：{detail}")
            await service.failures.fail("import", "登录态导入", detail, limit=1)


async def wt_update_resources() -> None:
    # 连续异常计数仅在本次运行中使用，待发送告警单独落盘。
    while not service.closing:
        if service.failures.blocked("resources"):
            return
        delay = max(0, service.failures.retry_after("resources") - time.time())
        if delay:
            await asyncio.sleep(delay)
        if service.closing:
            return
        try:
            result = await resources.update()
        except MaintenanceError:
            await asyncio.sleep(60)
            continue
        except Exception as error:
            if service.failures.attempts("resources") == 0:
                logger.exception("[WuwaTappoUID] 资源更新出现意外错误")
            await service.failures.fail("resources", "资源更新", service.error_detail(error))
        else:
            logger.info(f"[WuwaTappoUID] {result}")
            if not resources.incomplete:
                await service.failures.success("resources")
                return
            await service.failures.fail("resources", "资源更新", result)
        if service.closing:
            return


async def start_jobs() -> None:
    scheduler.add_job(
        owner.entry(service.failures.flush_alerts),
        "interval",
        seconds=60,
        id=ALERT_JOB,
        name="WuwaTappo 主人故障告警",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    if not service.database_ready:
        logger.error("[WuwaTappoUID] 初始化未完成，不恢复宝箱任务；请检查首次启动异常。")
        return
    register_status_panel(service)
    jobs = (
        (MAINTAIN_JOB, maintain, 60, "WuwaTappo 任务巡检"),
        (NOTICE_JOB, service.flush_notices, 60, "WuwaTappo 结果通知"),
        (HISTORY_JOB, service.archive_history, 60, "WuwaTappo 历史归档与清理"),
    )
    for job_id, operation, seconds, name in jobs:
        scheduler.add_job(
            owner.entry(operation),
            "interval",
            seconds=seconds,
            id=job_id,
            name=name,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
    scheduler.add_job(
        owner.entry(wt_update_resources, cancellable=True),
        "date",
        id=RESOURCE_JOB,
        name="WuwaTappo 名称与图库增量更新",
        replace_existing=True,
        misfire_grace_time=None,
    )
    try:
        await login.restore_workers()
        await service.restore()
    except SQLAlchemyError as error:
        await service.disable_database(error)
    except (DatabaseWriteTimeout, WriteGateTimeout) as error:
        await service.failures.fail("restore", "任务恢复", service.error_detail(error), limit=1)


async def maintain() -> None:
    if service.closing or not service.database_ready:
        return
    await login.maintain_sessions()
    await login.restore_workers()
    await service.maintain()


owner.initialize = initialize
owner.start_jobs = start_jobs


@on_core_start
async def wt_start() -> None:
    await owner.activate(start=True)


@gss.on_bot_connect
@with_read_cache
async def wt_bot_connected() -> None:
    if owner.active:
        service.failures.wake_alerts()
        await service.failures.wake_notifications()
        service.wake_notices()


@on_core_shutdown(priority=-10)
async def wt_shutdown() -> None:
    # GS重启前也会执行退出钩子；只有实际退出时才设置该事件。
    if not shutdown_event.is_set():
        logger.debug("[WuwaTappoUID] 核心尚未实际退出，保留网络会话与后台任务。")
        return
    await owner.drain()
