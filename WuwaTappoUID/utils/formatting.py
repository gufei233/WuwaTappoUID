"""纯文字消息。"""

import math
import time
from datetime import datetime, timedelta, timezone

from .command_text import command_hint
from .database import ChestTask
from .schema import SIDE_NAMES, UserInfo

CHINA = timezone(timedelta(hours=8))
PAUSE_REASONS = {"manual": "手动暂停", "auth": "登录态失效", "failure": "连续失败", "route": "通知路由失效"}


def mask_cuid(cuid: str | int) -> str:
    value = str(cuid)
    if len(value) <= 2:
        return "*" * len(value)
    visible = 3 if len(value) > 6 else 1
    return value[:visible] + "*" * (len(value) - visible * 2) + value[-visible:]


def date_text(timestamp: float) -> str:
    if timestamp <= 0:
        return "已可开启"
    return datetime.fromtimestamp(timestamp, CHINA).strftime("%m-%d %H:%M:%S（北京时间）")


def remaining_text(seconds: float) -> str:
    seconds = max(0, math.ceil(seconds))
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return f"{hours}小时{minutes}分{seconds}秒"


def counts(info: UserInfo) -> str:
    return f"累计点击：{info.total_click_count}\n已消耗点击：{info.total_click_consume}\n剩余可用点击：{info.remaining}"


def account_text(info: UserInfo, domestic: int) -> str:
    now = time.time()
    cooldowns = []
    for side in ("left", "right"):
        stamp = info.cooldown(side)
        cooldowns.append(f"{SIDE_NAMES[side]}宝箱：" + (date_text(stamp) if stamp > now else "已可开启"))
    region = f"CUID：{mask_cuid(info.cuid)}（{'国服' if domestic else '海外'}）"
    uid = [f"UID：{info.player_id}"] if str(info.player_id) not in ("", "0") else []
    return "\n".join([f"账号昵称：{info.display_name}", *uid, region, counts(info), *cooldowns])


def task_text(task: ChestTask | None) -> str:
    now = time.time()
    if task is None or task.status not in ("active", "paused"):
        return "自动开箱：未开启"
    if task.remaining(now) <= 0:
        return command_hint("自动开箱：本轮已到期，发送 wt宝箱 开启新一轮")
    state = "运行中" if task.status == "active" else f"已暂停（{PAUSE_REASONS.get(task.pause_reason, '已暂停')}）"
    deadline = "恢复后重新计算" if task.timer_paused else date_text(task.expires_at)
    return (
        f"自动开箱：{state}\n剩余：{remaining_text(task.remaining(now))}\n"
        f"到期：{deadline}\n本轮成功：{task.successes} 个"
    )
