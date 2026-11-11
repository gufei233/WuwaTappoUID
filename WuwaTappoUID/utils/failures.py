"""仅监测后台持续异常并提醒主人，不限制用户命令。"""

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from gsuid_core.aps import scheduler
from gsuid_core.config import core_config
from gsuid_core.gss import gss
from gsuid_core.logger import logger
from gsuid_core.subscribe import gs_subscribe
from pydantic import BaseModel, Field

from .formatting import date_text, mask_cuid
from .settings import master_alerts_enabled, retry_limit

ALERT_JOB = "WuwaTappoUID:alerts"


class Failure(BaseModel):
    attempts: int = 0
    blocked: bool = False
    retry_after: float = 0
    account_id: str = ""


class MasterAlert(BaseModel):
    text: str
    account_id: str = ""
    attempts: int = 0
    retry_after: float = 0
    delivered: list[str] = Field(default_factory=list)


class FailureState(BaseModel):
    alerts: list[MasterAlert] = Field(default_factory=list)


class Failures:
    def __init__(self, root: Path) -> None:
        self.path = root / "failure_state.json"
        self.state = FailureState()
        self._failures: dict[str, Failure] = {}
        self._lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()

    async def load(self) -> None:
        if self.path.exists():
            raw = json.loads(await asyncio.to_thread(self.path.read_text, "utf-8"))
            # 旧命令封禁和告警ID管理状态不再使用；新状态只保存待发送提醒。
            if isinstance(raw.get("alerts"), list):
                self.state = FailureState.model_validate(raw)
        if not master_alerts_enabled():
            await self._discard_alerts()

    def allows(self, key: str) -> bool:
        item = self._failures.get(key)
        return item is None or not item.blocked and item.retry_after <= time.time()

    def attempts(self, key: str) -> int:
        item = self._failures.get(key)
        return item.attempts if item else 0

    def blocked(self, key: str) -> bool:
        item = self._failures.get(key)
        return item is not None and item.blocked

    def retry_after(self, key: str) -> float:
        item = self._failures.get(key)
        return item.retry_after if item else 0

    @staticmethod
    def delay(attempts: int) -> int:
        return (60, 300, 900)[min(max(attempts - 1, 0), 2)]

    @staticmethod
    def _write(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_bytes(data)
        temporary.replace(path)

    async def _save(self) -> None:
        async with self._write_lock:
            await asyncio.to_thread(self._write, self.path, self.state.model_dump_json().encode())

    async def _discard_alerts(self) -> None:
        if self.state.alerts:
            self.state.alerts.clear()
            await self._save()

    def wake_alerts(self) -> None:
        job = scheduler.get_job(ALERT_JOB)
        if job is not None:
            job.modify(next_run_time=datetime.now(timezone.utc))

    async def fail(
        self,
        key: str,
        phase: str,
        detail: str,
        account_id: str = "",
        *,
        limit: int | None = None,
        delay: int | None = None,
    ) -> Failure:
        async with self._lock:
            item = self._failures.setdefault(key, Failure(account_id=account_id))
            maximum = retry_limit() if limit is None else limit
            if item.blocked:
                # blocked 只表示告警已发；巡检入口仍会冷却重试，这里续上冷却时间
                item.retry_after = time.time() + self.delay(maximum)
                return item.model_copy()
            item.attempts += 1
            item.blocked = item.attempts >= maximum
            item.retry_after = time.time() + (self.delay(item.attempts) if delay is None else delay)
            target = f"CUID {mask_cuid(account_id.split(':', 1)[-1])}" if account_id else "插件"
            summary = f"{target} · {phase} · 连续后台异常 {item.attempts}/{maximum}"
            if item.blocked:
                if master_alerts_enabled():
                    self.state.alerts.append(
                        MasterAlert(
                            text=f"WT后台异常提醒\n发生时间：{date_text(time.time())}\n{summary}\n{detail}"
                            "\n请主人检查日志、接口或数据库。任务和原始结果保留。",
                            account_id=account_id,
                        )
                    )
                    await self._save()
                    logger.error(f"[WuwaTappoUID] {summary}，已达到告警阈值并提醒主人。{detail}")
                    self.wake_alerts()
                else:
                    logger.error(f"[WuwaTappoUID] {summary}，已达到异常阈值；主人告警已关闭。{detail}")
            elif item.attempts == 1:
                logger.warning(f"[WuwaTappoUID] {summary}。{detail}")
            else:
                logger.warning(f"[WuwaTappoUID] {summary}，异常仍持续。")
            return item.model_copy()

    async def success(self, key: str) -> None:
        if key in self._failures:
            del self._failures[key]
            logger.info(f"[WuwaTappoUID] 后台阶段 {key.split(':', 1)[0]} 已恢复。")

    async def reset_account(self, account_id: str) -> None:
        for key in [key for key, value in self._failures.items() if value.account_id == account_id]:
            del self._failures[key]

    async def flush_alerts(self) -> None:
        async with self._send_lock:
            if not master_alerts_enabled():
                await self._discard_alerts()
                return
            if not gss.active_bot:
                return
            for alert in list(self.state.alerts):
                if not master_alerts_enabled():
                    await self._discard_alerts()
                    return
                if alert.retry_after > time.time():
                    continue
                alert.attempts += 1
                alert.retry_after = time.time() + self.delay(alert.attempts)
                if alert.attempts > retry_limit():
                    self.state.alerts.remove(alert)
                    await self._save()
                    continue
                await self._save()
                try:
                    async with asyncio.timeout(15):
                        masters = {str(value) for value in core_config.get_config("masters")}
                        targets = {
                            f"{sub.bot_id}:{sub.user_id}": sub
                            for sub in await gs_subscribe.get_subscribe("主人用户")
                            if str(sub.user_id) in masters
                        }
                        if not targets:
                            raise RuntimeError("主人订阅不可用")
                        for target, sub in targets.items():
                            if not master_alerts_enabled():
                                await self._discard_alerts()
                                return
                            if target in alert.delivered:
                                continue
                            if await sub.send(alert.text, force_direct=True) != 0:
                                raise RuntimeError("主人消息未提交")
                            alert.delivered.append(target)
                            await self._save()
                except Exception as error:
                    if alert.attempts == 1 or alert.attempts >= retry_limit():
                        logger.warning(
                            f"[WuwaTappoUID] 主人提醒投递失败 {alert.attempts}/{retry_limit()}："
                            f"{type(error).__name__}。"
                        )
                    if alert.attempts < retry_limit():
                        continue
                    logger.error("[WuwaTappoUID] 主人提醒投递已达到上限，不再自动重发；请查看后台日志。")
                self.state.alerts.remove(alert)
                await self._save()
