"""仅格式化代码中的命令提示模板，不替换用户输入或已保存的历史文本。"""

import re
from contextvars import ContextVar

from gsuid_core.sv import get_plugin_available_prefix

_COMMAND = re.compile(
    r"wt(?=强制更新资源|更新资源|开箱统计|添加token|增加次数|取消订阅|登录|查看|切换|删除|信息|次数|"
    r"背包|图鉴|宝箱|暂停|恢复|停止|订阅|合成|兑换|帮助|状态)"
)
current_prefix: ContextVar[str | None] = ContextVar("wt_command_prefix", default=None)


def command_prefix() -> str:
    snapshot = current_prefix.get()
    return snapshot if snapshot is not None else get_plugin_available_prefix("WuwaTappoUID")


def command_text(name: str, argument: str = "") -> str:
    return command_prefix() + name + (" " + argument if argument else "")


def command_hint(template: str) -> str:
    return _COMMAND.sub(lambda _: command_prefix(), template)
