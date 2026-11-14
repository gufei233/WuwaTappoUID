"""官方客户端的合成材料规则：每种留一件，升阶十件、同阶五件。"""

from dataclasses import dataclass

from .command_text import command_hint
from .schema import DataError, Item, WuwaError

KINDS = ("character_skin", "accessory", "emote")
RARITIES = ("common", "uncommon", "rare", "epic")
KIND_NAMES = {"character_skin": "角色", "accessory": "配件", "emote": "表情"}
KIND_ARGUMENTS = {name: kind for kind, name in KIND_NAMES.items()}
USAGE = "用法：wt合成 [升阶/同阶] [角色/配件/表情]，参数顺序不限；默认全部品类升阶。"


@dataclass
class SynthesisOptions:
    same_level: bool = False
    kind: str | None = None

    @classmethod
    def parse(cls, argument: str) -> "SynthesisOptions":
        result = cls()
        mode = ""
        for word in argument.split():
            if word in ("升阶", "同阶") and not mode:
                mode = word
                result.same_level = word == "同阶"
            elif word in KIND_ARGUMENTS and result.kind is None:
                result.kind = KIND_ARGUMENTS[word]
            else:
                raise WuwaError(command_hint(USAGE))
        return result

    @property
    def mode_name(self) -> str:
        return "同阶" if self.same_level else "升阶"

    @property
    def scope(self) -> str:
        return f"仅{KIND_NAMES[self.kind]}" if self.kind is not None else "全部品类"


@dataclass
class Recipe:
    same_level: bool
    asset_ids: list[int]


def category(item: Item) -> tuple[str, str]:
    kind = item.asset_type.removesuffix("_atlas")
    if kind == "character":
        kind = "character_skin"
    rarity = str(item.rarity or "").strip().lower()
    if kind not in KINDS or rarity not in RARITIES:
        raise DataError("背包或图鉴包含未识别的品类/稀有度，已停止合成。")
    return kind, rarity


def highest_tiers(atlas: list[Item]) -> dict[str, str]:
    highest: dict[str, str] = {}
    for item in atlas:
        kind, rarity = category(item)
        if kind not in highest or RARITIES.index(rarity) > RARITIES.index(highest[kind]):
            highest[kind] = rarity
    if any(kind not in highest for kind in KINDS):
        raise DataError("图鉴缺少品类，无法确定升阶上限。")
    return highest


def next_recipe(
    items: list[Item], highest: dict[str, str], *, same_level: bool = False, kind: str | None = None
) -> Recipe | None:
    groups: dict[tuple[str, str], list[Item]] = {}
    for item in items:
        if item.quantity > 1 and not item.is_collect:
            groups.setdefault(category(item), []).append(item)
    cost = 5 if same_level else 10
    selected_kinds = (kind,) if kind is not None else KINDS
    for selected_kind in selected_kinds:
        for rarity in RARITIES:
            if not same_level and RARITIES.index(rarity) >= RARITIES.index(highest[selected_kind]):
                continue
            group = groups.get((selected_kind, rarity), [])
            if sum(item.quantity - 1 for item in group) < cost:
                continue
            material_ids: list[int] = []
            for item in sorted(group, key=lambda entry: int(entry.asset_id)):
                count = min(item.quantity - 1, cost - len(material_ids))
                material_ids.extend([int(item.asset_id)] * count)
                if len(material_ids) == cost:
                    return Recipe(same_level, material_ids)
    return None
