"""分批归档与清理；累计入账和删除原始流水始终在同一写事务内。"""

from dataclasses import dataclass
from datetime import datetime

from gsuid_core.utils.database.base_models import with_session
from sqlalchemy import and_, delete, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col, select

from .database import ChestRecord, ChestSummary, Notice, SynthesisRecord
from .schema import DataError
from .statistics import CHINA, read_result

BATCH_SIZE = 100


@dataclass(frozen=True)
class ArchiveBatchResult:
    processed: int
    archived: int
    quarantined: tuple[str, ...]


class HistoryArchive:
    @classmethod
    @with_session
    async def archive_batch(cls, session: AsyncSession, cutoff: float, daily_cutoff: float) -> ArchiveBatchResult:
        delivered = (
            select(Notice.id)
            .where(Notice.id == ChestRecord.notice_id, col(Notice.delivery_state).in_(["submitted", "unroutable"]))
            .correlate(ChestRecord)
            .exists()
        )
        completed_notice = or_(ChestRecord.notice_id == "legacy", delivered)
        eligible = or_(
            and_(ChestRecord.state == "success", completed_notice),
            ChestRecord.state != "success",
        )
        result = await session.execute(
            select(ChestRecord)
            .where(
                ChestRecord.created_at < cutoff,
                ChestRecord.archive_error == "",
                eligible,
            )
            .order_by(col(ChestRecord.created_at), col(ChestRecord.id))
            .limit(BATCH_SIZE)
        )
        records = list(result.scalars().all())
        summaries: dict[tuple[str, int, str, str], ChestSummary] = {}
        archived = []
        quarantined = []
        for record in records:
            if record.state != "success":
                archived.append(record.id)
                continue
            try:
                item = read_result(record.result) if record.result else None
            except DataError as error:
                record.archive_error = str(error)[:160]
                session.add(record)
                quarantined.append(record.id)
                continue
            archived.append(record.id)
            asset_id = str(item.asset_id) if item is not None else ""
            rarity = str(item.rarity_type) if item is not None else ""
            clicks = item.click_consume if item is not None else 0
            day = int(
                datetime.fromtimestamp(record.created_at, CHINA)
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .timestamp()
            )
            periods = (0, day) if day >= daily_cutoff else (0,)
            for period in periods:
                key = record.account_id, period, asset_id, rarity
                if key not in summaries:
                    saved = await session.get(ChestSummary, key)
                    summaries[key] = (
                        saved
                        if saved is not None
                        else ChestSummary(
                            account_id=record.account_id,
                            day=period,
                            asset_id=asset_id,
                            rarity=rarity,
                            first_at=record.created_at,
                        )
                    )
                summary = summaries[key]
                summary.quantity += 1
                summary.clicks += clicks
                summary.first_at = min(summary.first_at, record.created_at)
                summary.result = record.result
                session.add(summary)
        # 写入汇总后再删除这批明细；失败或中断时整个事务回滚。
        await session.flush()
        if archived:
            await session.execute(
                delete(ChestRecord)
                .where(col(ChestRecord.id).in_(archived))
                .execution_options(synchronize_session=False)
            )
        return ArchiveBatchResult(len(records), len(archived), tuple(quarantined))

    @classmethod
    @with_session
    async def cleanup_batch(cls, session: AsyncSession, cutoff: float, daily_cutoff: float) -> int:
        referenced = select(ChestRecord.id).where(ChestRecord.notice_id == Notice.id).correlate(Notice).exists()
        result = await session.execute(
            select(Notice)
            .where(
                col(Notice.delivery_state).in_(["submitted", "unroutable", "superseded"]),
                Notice.finished_at < cutoff,
                ~referenced,
            )
            .order_by(col(Notice.finished_at), col(Notice.id))
            .limit(BATCH_SIZE)
        )
        notices = list(result.scalars().all())
        for notice in notices:
            await session.delete(notice)
        result = await session.execute(
            select(SynthesisRecord)
            .where(SynthesisRecord.created_at < cutoff, col(SynthesisRecord.state).in_(["success", "rejected"]))
            .order_by(col(SynthesisRecord.created_at), col(SynthesisRecord.id))
            .limit(BATCH_SIZE - len(notices))
        )
        synthesis = list(result.scalars().all())
        for record in synthesis:
            await session.delete(record)
        result = await session.execute(
            select(ChestSummary)
            .where(ChestSummary.day > 0, ChestSummary.day < daily_cutoff)
            .order_by(col(ChestSummary.day))
            .limit(BATCH_SIZE - len(notices) - len(synthesis))
        )
        daily = list(result.scalars().all())
        for summary in daily:
            await session.delete(summary)
        return len(notices) + len(synthesis) + len(daily)
