"""合成批次与结果汇总；Service 门面负责复用同一账号和用户锁。"""

import asyncio
import json
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING

from gsuid_core.logger import logger
from gsuid_core.models import Event

from .command_text import command_hint
from .database import Store, SynthesisRecord, new_id
from .formatting import mask_cuid
from .schema import ApiError, MaintenanceError, OperationStopped, SynthesisResult, WuwaError
from .synthesis import SynthesisOptions, highest_tiers, next_recipe

if TYPE_CHECKING:
    from .service import Service


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
    upgrades = same_count = spent = 0
    problem = ""
    bag_current = True
    run_id = new_id()
    async with service.heavy_operation(event, "合成", on_started, request_scope=request_scope) as (_, account):
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
            await Store.save(record)
            try:
                result = await service.api.synthesize(account.token, recipe.asset_ids, recipe.same_level)
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
                    logger.info(f"[WuwaTappoUID] CUID {mask_cuid(account.cuid)}：合成鉴权被拒绝，{error.details}。")
                if error.is_auth:
                    try:
                        await service.refresh(account, fresh=True)
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
                problem = command_hint("最后一次合成结果未确认，已停止且不会自动重发。请先用 wt背包 检查实际结果。")
                break
            record.result = result.model_dump_json()
            record.state = "success"
            await Store.save(record)
            results.append(result)
            same_count += int(recipe.same_level)
            upgrades += int(not recipe.same_level)
            spent += len(recipe.asset_ids)
            try:
                items = await service.read_bag(account, fresh=True)
            except WuwaError as error:
                bag_current = False
                problem = f"已保存本次合成结果，但背包回读失败：{error}。本轮已停止。"
                break
            record.after_quantity = sum(item.quantity for item in items)
            await Store.save(record)
            if record.before_quantity - record.after_quantity != len(recipe.asset_ids) - 1:
                problem = command_hint("服务端背包数量尚未与合成消耗一致，本轮已停止；请用 wt背包 核对。")
                break
    service.wake_notices()
    summary = f"本轮范围：{options.scope}。已确认：升阶 {upgrades} 次，同阶 {same_count} 次。"
    summary += f"消耗多余物品 {spent} 件，获得 {len(results)} 件。"
    if not results and not problem:
        cost = 5 if options.same_level else 10
        return [
            f"{options.scope}没有足够的多余物品可{options.mode_name}合成："
            f"已跳过收藏，每种保留 1 件，需同品类同稀有度 {cost} 件。"
        ]
    if problem:
        summary += "\n" + problem
    if not results:
        return [summary]
    try:
        images = await service.cards.synthesis(
            account,
            results,
            items if bag_current else None,
            event,
            f"{options.scope} · 升阶 {upgrades} 次 · 同阶 {same_count} 次",
            owned_before,
        )
    except Exception:
        logger.exception("[WuwaTappoUID] 合成结果已保存，但结果图绘制失败")
        return [summary + command_hint("\n结果图绘制失败，可用 wt背包 查看。")]
    return [summary, *images]
