"""wt 命令；显式请求才执行，不注册 AI 工具。"""

import re
from contextlib import AsyncExitStack
from copy import deepcopy

from gsuid_core.bot import Bot
from gsuid_core.config import core_config, plugin_config_store
from gsuid_core.help.utils import register_help
from gsuid_core.logger import logger
from gsuid_core.models import Event
from gsuid_core.segment import MessageSegment
from gsuid_core.sv import SV, config_sv, get_plugin_available_prefix
from gsuid_core.utils.database.base_models import DatabaseWriteTimeout
from gsuid_core.utils.database.write_gate import WriteGateTimeout
from sqlalchemy.exc import SQLAlchemyError

from ..runtime import login, resources, service
from ..utils.api import with_read_cache
from ..utils.backpack import BagFilter
from ..utils.buttons import query_buttons
from ..utils.command_text import command_hint, command_prefix
from ..utils.help_card import help_text, render_bytes
from ..utils.lifecycle import tracked
from ..utils.render_cache import HelpImageCache
from ..utils.schema import ApiError, AuthenticationError, DataError, NetworkError, WuwaError
from ..utils.statistics import StatisticsPeriod
from ..utils.synthesis import SynthesisOptions


async def help_image() -> bytes:
    return await _help_images.get(get_plugin_available_prefix("WuwaTappoUID"), render_bytes)


SERVICE_COMMANDS = (
    ("WuwaTappo登录", 6, ("登录", "添加token")),
    ("WuwaTappo账号", 6, ("查看", "切换", "信息")),
    ("WuwaTappo账号删除", 6, ("删除",)),
    ("WuwaTappo背包", 6, ("背包",)),
    ("WuwaTappo图鉴", 6, ("图鉴",)),
    ("WuwaTappo合成", 6, ("合成",)),
    ("WuwaTappo兑换", 6, ("兑换",)),
    ("WuwaTappo次数", 6, ("次数", "增加次数")),
    ("WuwaTappo宝箱任务", 6, ("宝箱", "暂停", "恢复", "停止")),
    ("WuwaTappo开箱统计", 6, ("开箱统计",)),
    ("WuwaTappo通知订阅", 6, ("订阅", "取消订阅")),
    ("WuwaTappo资源更新", 6, ("更新资源",)),
    ("WuwaTappo强制资源更新", 1, ("强制更新资源",)),
    ("WuwaTappo状态", 1, ("状态",)),
    ("WuwaTappo帮助", 6, ("帮助",)),
)


def migrate_service_config() -> None:
    old_name = "WuwaTappo账号与宝箱"
    plugin_configs = plugin_config_store.get_all()
    services = plugin_configs["WuwaTappoUID"]["sv"] if "WuwaTappoUID" in plugin_configs else {}
    if old_name in config_sv:
        old = config_sv.pop(old_name)
        for name, pm, _ in SERVICE_COMMANDS:
            if name not in config_sv and name not in services:
                config_sv[name] = {**deepcopy(old), "pm": min(old["pm"], pm)}
        core_config.lazy_set_config("sv", config_sv)
        if old_name in services:
            del services[old_name]
            plugin_config_store.mark_dirty("WuwaTappoUID")
    elif old_name in services:
        old = services.pop(old_name)
        for name, pm, _ in SERVICE_COMMANDS:
            if name not in services and name not in config_sv:
                services[name] = {**deepcopy(old), "pm": min(old["pm"], pm)}
        plugin_config_store.mark_dirty("WuwaTappoUID")


async def wt_command(bot: Bot, event: Event) -> None:
    try:
        await execute_command(bot, event)
    except WuwaError as error:
        await bot.send(str(error))


migrate_service_config()
for name, pm, commands in SERVICE_COMMANDS:
    SV(name, pm=pm).on_command(commands, block=True)(wt_command)
register_help("WuwaTappoUID", f"{get_plugin_available_prefix('WuwaTappoUID')}帮助")
_help_images = HelpImageCache()


@tracked(lambda: service.runtime)
@with_read_cache
async def execute_command(bot: Bot, event: Event) -> None:
    async with AsyncExitStack() as request_scope:
        await _execute_command(bot, event, request_scope)


async def _execute_command(bot: Bot, event: Event, request_scope: AsyncExitStack) -> None:
    # GS 会把命令本身放在 command，把剥离命令后的参数放在 text。
    command = event.command.strip()
    argument = event.text.strip()
    if command == "帮助":
        await bot.send(await help_image())
        return
    try:
        if command != "状态":
            service.require_database()
        if command == "登录":
            if argument:
                raise WuwaError(command_hint("请直接发送 wt登录；海外账号请用 wt添加token TOKEN 海外。"))
            replies = await login.create(event)
        elif command == "添加token":
            if event.user_type == "group":
                raise WuwaError(command_hint("请撤回群内 token，并在私聊中使用 wt添加token。"))
            match = re.fullmatch(r"(\S+)(?:\s+(国服|海外))?", argument)
            if match is None:
                raise WuwaError(command_hint("用法：wt添加token TOKEN，可在末尾加国服或海外；token 前的空格可省。"))
            domestic = None if match[2] is None else 0 if match[2] == "海外" else 1
            replies = await service.bind_token(event, match[1], domestic)
        elif command == "切换":
            if argument:
                match = re.fullmatch(r"([0-9]+|\[[0-9]+\])(?:\s+(国服|海外))?", argument)
                if match is None:
                    raise WuwaError(command_hint("用法：wt切换 [编号] 或 wt切换 CUID；不带参数则切换到下一个账号。"))
                domestic = None if match[2] is None else 0 if match[2] == "海外" else 1
                replies = [await service.switch(event, match[1], domestic)]
            else:
                replies = [await service.switch(event)]
        elif command == "删除":
            if argument:
                match = re.fullmatch(r"([0-9]+|\[[0-9]+\])(?:\s+(国服|海外))?", argument)
                if match is None:
                    raise WuwaError(command_hint("用法：wt删除 [编号] 或 wt删除 CUID；不带参数删除当前绑定。"))
                domestic = None if match[2] is None else 0 if match[2] == "海外" else 1
                replies = [await service.delete_account(event, match[1], domestic)]
            else:
                replies = [await service.delete_account(event)]
        elif command == "增加次数":
            if re.fullmatch(r"[0-9]+", argument) is None or int(argument) <= 0:
                raise WuwaError(command_hint("请输入正整数，例如 wt增加次数1000 或 wt增加次数 1000。"))

            async def increase_started() -> None:
                await bot.send("开始增加次数，将先核对账号和累计值，请等待结果。")

            replies = await service.increase(event, int(argument), increase_started, request_scope=request_scope)
        elif command == "合成":
            options = SynthesisOptions.parse(argument)

            async def synthesis_started() -> None:
                await bot.send(f"开始{options.mode_name}合成多余物品：本轮{options.scope}，跳过收藏，每种保留 1 件。")

            replies = await service.synthesize(event, options, synthesis_started, request_scope=request_scope)
        elif command == "兑换":
            if not argument or len(argument.split()) != 1:
                raise WuwaError(command_hint("用法：wt兑换 CODE，为当前账号使用一个兑换码。"))
            replies = await service.redeem(event, argument)
        elif command == "开箱统计":
            replies = await service.chest_statistics(event, StatisticsPeriod.parse(argument))
        elif command == "背包":
            replies = await service.query(event, bag=True, filters=BagFilter.parse(argument))
        elif command == "图鉴":
            kinds = {"角色": "character", "配件": "accessory", "表情": "emote"}
            if argument and argument not in kinds:
                raise WuwaError(f"用法：{command_prefix()}{command} [角色/配件/表情]；不带参数查看全部分类。")
            kind = kinds[argument] if argument else None
            replies = await service.atlas(event, kind)
        elif argument:
            raise WuwaError(command_hint("这条命令不需要参数，可发送 wt帮助 查看用法。"))
        elif command == "查看":
            replies = [await service.view_accounts(event)]
        elif command in ("订阅", "取消订阅"):
            replies = [await service.subscribe(event, command == "订阅")]
        elif command == "信息":
            replies = await service.info(event)
        elif command == "次数":
            replies = await service.query(event)
        elif command == "宝箱":
            replies = await service.start_or_renew(event)
        elif command == "暂停":
            replies = [await service.pause(event)]
        elif command == "恢复":
            replies = [await service.recover(event)]
        elif command == "停止":
            replies = [await service.stop(event)]
        elif command == "状态":
            replies = [await service.status(event)]
        elif command in ("更新资源", "强制更新资源"):
            force = command == "强制更新资源"
            if force and event.user_pm > 1:
                raise WuwaError("强制更新资源仅限 Bot 管理员使用。")
            await bot.send("开始更新中文名称与物品图库，完成后会回复统计结果。")
            await service.failures.success("resources")
            replies = [await resources.update(force=force)]
            if resources.incomplete:
                await service.failures.fail("resources", "资源更新", replies[0])
        else:
            replies = [help_text(get_plugin_available_prefix("WuwaTappoUID"))]
    except AuthenticationError as error:
        replies = [str(error) if command == "恢复" else await service.authentication_message(event, error)]
    except (ApiError, DataError, NetworkError) as error:
        replies = [str(error)]
    except WuwaError as error:
        replies = [str(error)]
    except SQLAlchemyError as error:
        await service.disable_database(error)
        replies = [service.database_error]
    except (DatabaseWriteTimeout, WriteGateTimeout):
        replies = [command_hint("数据库未能完成本次操作。请稍后用 wt次数 或 wt宝箱 确认实际状态后再操作。")]
    except Exception as error:
        logger.exception("[WuwaTappoUID] 命令出现意外错误")
        replies = [f"命令异常（{type(error).__name__}），已记录；请检查实际状态后再操作。"]
    segments = [
        MessageSegment.image(reply) if isinstance(reply, bytes) else MessageSegment.text(reply) for reply in replies
    ]
    messages = [MessageSegment.node(segments)] if sum(isinstance(reply, bytes) for reply in replies) > 1 else segments
    if event.user_type == "group" and command not in ("背包", "图鉴", "查看"):
        messages = [MessageSegment.at(event.user_id), MessageSegment.text("\n"), *messages]
    if command in ("信息", "次数", "宝箱") and any(isinstance(reply, bytes) for reply in replies):
        await bot.send_option(messages, query_buttons(), unsuported_platform=True)
    else:
        await bot.send(messages)
