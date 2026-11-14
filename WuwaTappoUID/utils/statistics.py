"""按北京时间划分查询范围，汇总已确认成功的本地开箱流水。"""

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from pydantic import ValidationError

from .command_text import command_hint
from .schema import DataError, OpenResult, WuwaError

CHINA = timezone(timedelta(hours=8))
RARITIES = ("epic", "rare", "uncommon", "common")


def read_result(payload: str) -> OpenResult:
    try:
        result = OpenResult.model_validate_json(payload)
    except ValidationError:
        raise DataError("历史开箱流水的物品详情格式异常，请管理员检查记录。") from None
    rarity = str(result.rarity_type).strip().lower()
    if rarity not in RARITIES:
        raise DataError("历史开箱流水含未识别的稀有度，请管理员检查记录。")
    result.rarity_type = rarity
    return result


@dataclass(frozen=True)
class StatisticsPeriod:
    label: str
    since: float
    until: float

    @classmethod
    def parse(cls, argument: str) -> "StatisticsPeriod":
        value = argument or "7天"
        if value not in ("今天", "7天", "30天", "全部"):
            raise WuwaError(command_hint("用法：wt开箱统计 [今天/7天/30天/全部]；不带参数查询最近7天。"))
        now = datetime.now(CHINA)
        if value == "全部":
            return cls("全部", 0, now.timestamp())
        days = {"今天": 1, "7天": 7, "30天": 30}[value]
        start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days - 1)
        return cls("今天" if days == 1 else f"最近{days}天", start.timestamp(), now.timestamp())

    def caption(self, first_at: float | None) -> str:
        end = datetime.fromtimestamp(self.until, CHINA).strftime("%Y.%m.%d")
        start = self.since or first_at
        if start is None:
            return f"{self.label} · 截至 {end}"
        begin = datetime.fromtimestamp(start, CHINA).strftime("%Y.%m.%d")
        return f"{self.label} · {begin}" if begin == end else f"{self.label} · {begin} — {end}"


@dataclass
class ChestStatistics:
    total: int
    missing: int
    invalid: int
    counts: dict[str, int]
    clicks: int
    top: list[tuple[OpenResult, int]]

    @property
    def known(self) -> int:
        return self.total - self.missing - self.invalid

    @classmethod
    def aggregate(cls, groups: list[tuple[str, int, int | None]]) -> "ChestStatistics":
        counts = dict.fromkeys(RARITIES, 0)
        quantities: Counter[str] = Counter()
        items: dict[str, OpenResult] = {}
        total = missing = invalid = clicks = 0
        for payload, count, consumed in groups:
            total += count
            if not payload:
                missing += count
                continue
            try:
                result = read_result(payload)
            except DataError:
                invalid += count
                continue
            rarity = str(result.rarity_type)
            counts[rarity] += count
            clicks += result.click_consume * count if consumed is None else consumed
            key = str(result.asset_id)
            quantities[key] += count
            items[key] = result
        top = [(items[key], count) for key, count in quantities.most_common(5)]
        return cls(total, missing, invalid, counts, clicks, top)
