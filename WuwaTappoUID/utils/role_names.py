"""背包角色筛选：明确别名直接解析，模糊候选仅作提示。"""

import difflib
import unicodedata

from .command_text import command_text
from .schema import WuwaError

# 使用官方角色 ID；只在当前目录中确实存在时启用。
ALIASES = {
    "男漂": "1501",
    "漂泊者男": "1501",
    "女漂": "1502",
    "漂泊者女": "1502",
    "维奶": "1503",
    "守岸": "1505",
    "相里": "1305",
    "卡提": "1409",
    "坎特": "1607",
}


def normalize(name: str) -> str:
    return unicodedata.normalize("NFKC", name).strip().casefold()


def resolve_roles(name: str, roles: dict[str, str]) -> set[str]:
    exact = {str(key) for key, value in roles.items() if value == name}
    if exact:
        return exact
    normalized = normalize(name)
    exact = {str(key) for key, value in roles.items() if normalize(value) == normalized}
    if exact:
        return exact
    alias = ALIASES.get(normalized)
    if alias is not None and alias in roles:
        return {alias}
    candidates = difflib.get_close_matches(normalized, sorted({normalize(value) for value in roles.values()}), n=3)
    suggestions = [next(value for value in roles.values() if normalize(value) == candidate) for candidate in candidates]
    commands = "\n".join(command_text("背包", f"角色={candidate}") for candidate in suggestions)
    raise WuwaError(
        "未找到指定角色。" + ("你可能想查询：\n" + commands if commands else "请使用完整角色名，例如 角色=今汐。")
    )
