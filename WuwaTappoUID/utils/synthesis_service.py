"""合成批次与结果汇总；Service 门面负责复用同一账号和用户锁。"""

import asyncio
import json
from collections import Counter
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Literal

from gsuid_core.logger import logger
from gsuid_core.models import Event
from gsuid_core.utils.database.base_models import DatabaseWriteTimeout
from gsuid_core.utils.database.write_gate import WriteGateTimeout
from sqlalchemy.exc import SQLAlchemyError

from .command_text import command_hint
from .database import Store, SynthesisRecord, new_id
from .formatting import mask_cuid
from .schema import ApiError, MaintenanceError, OperationStopped, SynthesisResult, WuwaError
from .synthesis import SynthesisOptions, highest_tiers, next_recipe

if TYPE_CHECKING:
    from .service import Service


async def _save_or_stash(service: "Service", record: SynthesisRecord) -> Literal["saved", "timeout", "db_error"]:
    try:
        await Store.save(record)
        return "saved"
    except (DatabaseWriteTimeout, WriteGateTimeout):
        await service.stash_pending_synthesis(record)
        return "timeout"
    except SQLAlchemyError as error:
        await service.stash_pending_synthesis(record)
        await service.disable_database(error)
        return "db_error"


def _result_lines(service: "Service", results: list[SynthesisResult]) -> str:
    counts = Counter(service.cards.item_name(item.asset_id, item.asset_name) for item in results)
    return "\n".join(f"- {name} ×{count}" for name, count in counts.items())


async def synthesize(
    service: "Service",
    event: Event,
    options: SynthesisOptions,
    on_started: Callable[[], Awaitable[None]] | None = None,
    *,
    request_scope: AsyncExitStack | None = None,
) -> list[str | bytes]:
    async with AsyncExitStack() as local_scope:
        return await _synthesize(
            service, event, options, on_started, request_scope if request_scope is not None else local_scope
        )


async def _synthesize(
    service: "Service",
    event: Event,
    options: SynthesisOptions,
    on_started: Callable[[], Awaitable[None]] | None,
    request_scope: AsyncExitStack,
) -> list[str | bytes]:
    results: list[SynthesisResult] = []
    spent = 0
    problem = ""
    bag_current = True
    run_id = new_id()
    async with service.heavy_operation(event, "合成", on_started, request_scope=request_scope) as (binding, account):
        await service.refresh(account)
        items = await service.read_bag(account)
        owned_before = {str(item.asset_id) for item in items if item.quantity > 0}
        highest = {} if options.same_level else highest_tiers(await service.api.atlas())
        while not service.closing:
            try:
                recipe = next_recipe(items, highest, same_level=options.same_level, kind=options.kind)
            except WuwaError as error:
                problem = str(error)
                break
            if recipe is None:
                break
            if results:
                await asyncio.sleep(1)
            if service.closing:
                problem = "插件正在重新加载，本轮已停止，已确认结果保留。"
                break
            record = SynthesisRecord(
                run_id=run_id,
                account_id=account.id,
                mode="same_level" if recipe.same_level else "level_up",
                material_ids=json.dumps(recipe.asset_ids),
                before_quantity=sum(item.quantity for item in items),
            )
            try:
                await Store.save(record)
            except (DatabaseWriteTimeout, WriteGateTimeout):
                if not results:
                    raise
                problem = "后续合成预写超时，已停止继续合成；以下为本轮已确认结果。"
                break
            except SQLAlchemyError as error:
                await service.disable_database(error)
                if not results:
                    raise
                problem = "后续合成遇到数据库故障，已停止继续合成；以下为本轮已确认结果。"
                break
            try:
                result = await service.api.synthesize(account.token, recipe.asset_ids, recipe.same_level)
            except OperationStopped as error:
                record.state, record.error = "rejected", str(error)
                await _save_or_stash(service, record)
                problem = str(error)
                break
            except MaintenanceError as error:
                record.state, record.error = "rejected", str(error)
                await _save_or_stash(service, record)
                problem = str(error)
                break
            except ApiError as error:
                record.state, record.error = "rejected", str(error)
                await _save_or_stash(service, record)
                if error.is_auth:
                    logger.info(f"[WuwaTappoUID] CUID {mask_cuid(account.cuid)}：合成鉴权被拒绝，{error.details}。")
                    if service.database_ready:
                        try:
                            await service.expire_account(account)
                        except (DatabaseWriteTimeout, WriteGateTimeout):
                            pass
                        except SQLAlchemyError as db_error:
                            await service.disable_database(db_error)
                    problem = service.expired_message(account, binding.id)
                    break
                problem = f"服务端拒绝本次合成：{error}，本轮已停止。"
                break
            except WuwaError as error:
                record.state, record.error = "uncertain", type(error).__name__
                bag_current = False
                await _save_or_stash(service, record)
                problem = command_hint("最后一次合成结果未确认，已停止且不会自动重发。请先用 wt背包 检查实际结果。")
                break
            record.result = result.model_dump_json()
            record.state = "success"
            results.append(result)
            spent += len(recipe.asset_ids)
            saved_state = await _save_or_stash(service, record)
            if saved_state != "saved":
                bag_current = False
                problem = (
                    "已确认本次合成产物，但数据库故障，已暂存本地待恢复后补写；本轮已停止。"
                    if saved_state == "db_error"
                    else "已确认本次合成产物，但数据库写入超时，已暂存待后台补写；本轮已停止。"
                )
                break
            try:
                items = await service.read_bag(account, fresh=True)
            except WuwaError as error:
                bag_current = False
                problem = f"已保存本次合成结果，但背包回读失败：{error}。本轮已停止。"
                break
            record.after_quantity = sum(item.quantity for item in items)
            saved_after = await _save_or_stash(service, record)
            if saved_after != "saved":
                problem = (
                    "已保存本次合成结果，但背包数量回写遇到数据库故障，已暂存本地待恢复后补写；本轮已停止。"
                    if saved_after == "db_error"
                    else "已保存本次合成结果，但背包数量回写超时，已暂存待后台补写；本轮已停止。"
                )
                break
            if record.before_quantity - record.after_quantity != len(recipe.asset_ids) - 1:
                problem = command_hint("服务端背包数量尚未与合成消耗一致，本轮已停止；请用 wt背包 核对。")
                break
    service.wake_notices()
    if not results and not problem:
        cost = 5 if options.same_level else 10
        return [
            f"{options.scope}没有足够的多余物品可{options.mode_name}合成："
            f"已跳过收藏，每种保留 1 件，需同品类同稀有度 {cost} 件。"
        ]
    if not results:
        return [f"合成已停止（{options.scope}）：\n{problem}"]
    summary = (
        f"合成完成（{options.scope} · {options.mode_name} {len(results)} 次）："
        f"消耗 {spent} 件，获得 {len(results)} 件。"
    )
    if problem:
        summary += "\n" + problem
    if not service.database_ready:
        return [
            summary
            + "\n已获得产物：\n"
            + _result_lines(service, results)
            + command_hint("\n结果图暂不可用，可用 wt背包 查看。")
        ]
    try:
        images = await service.cards.synthesis(
            account,
            results,
            items if bag_current else None,
            event,
            f"{options.scope} · {options.mode_name} {len(results)} 次",
            owned_before,
        )
    except Exception:
        logger.exception("[WuwaTappoUID] 合成结果已确认，但结果图绘制失败")
        return [
            summary
            + "\n已获得产物：\n"
            + _result_lines(service, results)
            + command_hint("\n结果图暂不可用，可用 wt背包 查看。")
        ]
    return [summary, *images]
