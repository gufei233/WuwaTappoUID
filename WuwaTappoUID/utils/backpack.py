"""背包组合筛选参数；不改变原始背包及合成材料。"""

from dataclasses import dataclass

from .command_text import command_hint
from .schema import WuwaError

KINDS = {"角色": "character", "配件": "accessory", "表情": "emote"}
RARITIES = {"普通": "common", "珍稀": "uncommon", "稀有": "rare", "史诗": "epic"}
USAGE = "用法：wt背包 [角色/配件/表情] [普通/珍稀/稀有/史诗] [角色=今汐] [重复]，条件可组合。"


@dataclass
class BagFilter:
    kind: str | None = None
    rarity: str | None = None
    role: str = ""
    duplicates: bool = False

    @classmethod
    def parse(cls, argument: str) -> "BagFilter":
        result = cls()
        for word in argument.split():
            if word in KINDS and result.kind is None:
                result.kind = KINDS[word]
            elif word in RARITIES and result.rarity is None:
                result.rarity = RARITIES[word]
            elif word.startswith("角色=") and word[3:] and not result.role:
                result.role = word[3:]
            elif word == "重复" and not result.duplicates:
                result.duplicates = True
            else:
                raise WuwaError(command_hint(USAGE))
        return result

    @property
    def description(self) -> str:
        parts = [name for name, value in KINDS.items() if value == self.kind]
        parts += [name for name, value in RARITIES.items() if value == self.rarity]
        if self.role:
            parts.append(f"角色={self.role}")
        if self.duplicates:
            parts.append("重复")
        return " / ".join(parts)
