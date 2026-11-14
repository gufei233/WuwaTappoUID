"""通过 GS 普通命令入口提供查询按钮。"""

from gsuid_core.message_models import Button

from .command_text import command_text


def query_buttons() -> list[Button]:
    return [Button(name, command_text(name)) for name in ("背包", "图鉴", "次数", "开箱统计")]
