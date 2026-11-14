"""账号、点击和宝箱任务图片；只绘制调用方提供的数据快照。"""

import io
import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from PIL import Image, ImageDraw

from .command_text import command_hint
from .render_common import (
    ACCENT,
    BG,
    BROWN,
    SCALE,
    SOFT,
    Color,
    Header,
    background,
    draw_text,
    fit,
    header,
    heading,
    rounded_mask,
    runs,
    s,
    shadow,
    width_of,
)
from .render_common import (
    footer as draw_footer,
)
from .schema import UserInfo

GREEN = (107, 142, 106)
PALE_GREEN = (231, 239, 216)
LINE = (235, 214, 185)
CREAM = (255, 250, 239)
CHINA = timezone(timedelta(hours=8))
PAUSE_LABELS = {"manual": "手动暂停", "auth": "登录态失效", "failure": "连续失败", "route": "通知路由失效"}


@dataclass
class TaskSnapshot:
    status: str
    pause_reason: str
    started_at: float
    expires_at: float
    paused_at: float | None
    successes: int
    subscribed: bool

    def remaining(self, now: float) -> float:
        return self.expires_at - (self.paused_at if self.paused_at is not None else now)

    def state(self, now: float) -> str:
        if self.status not in ("active", "paused"):
            return "未开启"
        if self.remaining(now) <= 0:
            return "已到期"
        return "运行中" if self.status == "active" else "已暂停"

    def label(self, now: float) -> str:
        state = self.state(now)
        if state == "已暂停" and self.pause_reason:
            reason = PAUSE_LABELS[self.pause_reason] if self.pause_reason in PAUSE_LABELS else self.pause_reason
            return f"{state} · {reason}"
        return state


def duration(seconds: float, *, seconds_in_hours: bool = False) -> str:
    hours, rest = divmod(max(0, math.ceil(seconds)), 3600)
    minutes, sec = divmod(rest, 60)
    if hours:
        return f"{hours}小时{minutes:02d}分" + (f"{sec:02d}秒" if seconds_in_hours else "")
    return f"{minutes}分{sec:02d}秒" if minutes else f"{sec}秒"


def stamp(value: float, *, clock: bool = False) -> str:
    return datetime.fromtimestamp(value, CHINA).strftime("%H:%M:%S" if clock else "%m-%d %H:%M")


def text(
    draw: ImageDraw.ImageDraw,
    x: float,
    y: float,
    value: str,
    size: int = 18,
    color: Color = BROWN,
    *,
    line: float | None = None,
    align: str = "left",
    weight: int = 520,
    rounded: bool = True,
    limit: float | None = None,
) -> float:
    if limit is not None:
        while size > 11 and width_of(runs(value, size, rounded=rounded, weight=weight)) > s(limit):
            size -= 1
    return draw_text(
        draw,
        x,
        y,
        line if line is not None else size * 1.45,
        value,
        size,
        color,
        align=align,
        rounded=rounded,
        weight=weight,
    )


def panel(canvas: Image.Image, x: float, y: float, width: float, height: float, fill: Color = CREAM) -> None:
    mask = rounded_mask((s(width), s(height)), s(22))
    shadow(canvas, mask, (s(x), s(y + 2)), (156, 104, 69, 14), 5)
    border = Image.new("RGBA", mask.size, LINE + (255,))
    border.putalpha(mask)
    canvas.alpha_composite(border, (s(x), s(y)))
    inner = rounded_mask((s(width - 2), s(height - 2)), s(21))
    layer = Image.new("RGBA", inner.size, fill)
    layer.putalpha(inner)
    canvas.alpha_composite(layer, (s(x + 1), s(y + 1)))


def pill(canvas: Image.Image, draw: ImageDraw.ImageDraw, x: float, y: float, value: str, active: bool = True) -> None:
    width = width_of(runs(value, 14)) / SCALE + 26
    draw.rounded_rectangle((s(x), s(y), s(x + width), s(y + 29)), s(14.5), fill=PALE_GREEN if active else LINE)
    text(draw, x + width / 2, y + 3, value, 14, GREEN if active else BROWN, line=23, align="center")


def rule(draw: ImageDraw.ImageDraw, x1: float, y1: float, x2: float, y2: float) -> None:
    draw.line((s(x1), s(y1), s(x2), s(y2)), fill=LINE, width=s(1))


def base(info: Header, title: str, height: float) -> tuple[Image.Image, ImageDraw.ImageDraw]:
    canvas = Image.new("RGBA", (s(900), s(height)), BG + (255,))
    background(canvas)
    draw = ImageDraw.Draw(canvas)
    header(canvas, draw, info)
    heading(canvas, draw, 153, title)
    return canvas, draw


def footer(draw: ImageDraw.ImageDraw, y: float) -> None:
    rule(draw, 36, y - 10, 864, y - 10)
    draw_footer(draw, y)


def finish(canvas: Image.Image) -> bytes:
    output = io.BytesIO()
    canvas.convert("RGB").save(output, "JPEG", quality=90, optimize=True)
    return output.getvalue()


def stats(canvas: Image.Image, draw: ImageDraw.ImageDraw, y: float, info: UserInfo) -> None:
    panel(canvas, 36, y, 828, 126)
    text(draw, 64, y + 14, "剩余可用点击", 15, SOFT)
    text(draw, 62, y + 40, f"{info.remaining:,}", 44, ACCENT, line=61, limit=272)
    rule(draw, 364, y + 25, 364, y + 101)
    for x, label, value in ((398, "累计点击", info.total_click_count), (644, "已消耗点击", info.total_click_consume)):
        text(draw, x, y + 19, label, 15, SOFT)
        text(draw, x, y + 54, f"{value:,}", 29, BROWN, line=40, limit=194)


def render_account(
    profile: Header,
    info: UserInfo,
    cuid: str,
    domestic: int,
    task: TaskSnapshot | None,
    subscribed: bool,
    desktop_avatar: bytes | None,
) -> bytes:
    now = time.time()
    canvas, draw = base(profile, "账号信息", 757)
    panel(canvas, 36, 222, 828, 94)
    if desktop_avatar is not None:
        with Image.open(io.BytesIO(desktop_avatar)) as opened, opened.convert("RGBA") as picture:
            with picture.crop(picture.getbbox()) as cropped, fit(cropped, s(76), s(76)) as icon:
                canvas.alpha_composite(icon, (s(60) + (s(76) - icon.width) // 2, s(231) + (s(76) - icon.height) // 2))
    else:
        text(draw, 98, 256, "头像暂不可用" if info.character is not None else "未设置", 13, SOFT, align="center")
    text(draw, 154, 249, info.display_name, 25, line=36, rounded=False, weight=700, limit=270)
    text(draw, 840, 245, f"CUID {cuid}", 18, align="right", limit=278)
    text(draw, 840, 278, "国服" if domestic else "海外", 14, SOFT, align="right")
    stats(canvas, draw, 332, info)
    for index, side in enumerate(("left", "right")):
        x, top = 36 + 422 * index, 474
        panel(canvas, x, top, 406, 106)
        text(draw, x + 24, top + 12, "左侧宝箱" if side == "left" else "右侧宝箱", 15, SOFT)
        available_at = info.cooldown(side)
        if available_at > now:
            text(draw, x + 24, top + 42, duration(available_at - now, seconds_in_hours=True), 28, line=38, limit=240)
            text(draw, x + 380, top + 54, stamp(available_at, clock=True) + "可开", 13, SOFT, align="right")
        else:
            text(draw, x + 24, top + 42, "已可开启", 28, GREEN, line=38)
            draw.ellipse((s(x + 362), s(top + 51), s(x + 372), s(top + 61)), fill=GREEN)
    top = 596
    panel(canvas, 36, top, 828, 89)
    text(draw, 61, top + 13, "自动开箱", 16)
    state = task.state(now) if task is not None else "未开启"
    label = task.label(now) if task is not None else state
    pill(canvas, draw, 158, top + 10, label, state == "运行中")
    text(draw, 838, top + 15, "10次后通知" if subscribed else "任务结束汇总", 14, SOFT, align="right")
    if task is not None and state in ("运行中", "已暂停"):
        text(draw, 61, top + 52, f"剩余 {duration(task.remaining(now))} · 本轮成功 {task.successes}个", 16, limit=494)
        deadline = "恢复后计算到期时间" if task.paused_at is not None else stamp(task.expires_at) + " 到期"
        text(draw, 838, top + 52, deadline, 15, SOFT, align="right", limit=236)
    else:
        text(draw, 61, top + 52, command_hint("发送 wt宝箱 开启新任务"), 16, SOFT)
    footer(draw, 705)
    return finish(canvas)


def render_clicks(
    profile: Header,
    info: UserInfo,
    requested: int | None = None,
    delta: int = 0,
) -> bytes:
    changing = requested is not None
    canvas, draw = base(profile, "点击统计", 620 if changing else 435)
    top = 407 if changing else 222
    if requested is not None:
        panel(canvas, 36, 222, 828, 168, (251, 250, 234))
        text(draw, 63, 240, "本次实际增加" if delta >= 0 else "本次实际变化", 16, SOFT)
        right = text(draw, 60, 271, f"{delta:+,}", 50, GREEN if delta > 0 else BROWN, line=70, limit=580)
        text(draw, right + 15, 301, "次点击", 18, SOFT)
        pill(canvas, draw, 729, 243, "增加成功" if delta == requested else "已回读", delta > 0)
        text(draw, 65, 355, f"请求增加 {requested:,} 次", 15, SOFT)
    stats(canvas, draw, top, info)
    footer(draw, top + 161)
    return finish(canvas)


def render_task(profile: Header, task: TaskSnapshot) -> bytes:
    now = time.time()
    state = task.state(now)
    paused = task.paused_at is not None
    canvas, draw = base(profile, "宝箱任务", 600)
    panel(canvas, 36, 222, 828, 170)
    pill(canvas, draw, 62, 241, task.label(now), state == "运行中")
    text(draw, 836, 245, "10次后通知" if task.subscribed else "任务结束汇总", 15, SOFT, align="right")
    text(draw, 63, 284, "剩余有效期" + (" · 已冻结" if paused else ""), 15, SOFT)
    text(draw, 60, 312, duration(task.remaining(now)), 40, line=57, limit=456)
    rule(draw, 556, 291, 556, 366)
    text(draw, 604, 283, "本轮成功", 15, SOFT)
    right = text(draw, 602, 311, str(task.successes), 43, ACCENT, line=57, limit=154)
    text(draw, right + 18, 332, "个宝箱", 16, SOFT)
    panel(canvas, 36, 408, 828, 110)
    text(draw, 63, 424, stamp(task.started_at) + " 开始", 15, SOFT)
    text(draw, 837, 424, "恢复后计算到期时间" if paused else stamp(task.expires_at) + " 到期", 15, align="right")
    clock = task.paused_at if task.paused_at is not None else now
    elapsed = max(0, clock - task.started_at)
    total = task.expires_at - task.started_at
    ratio = min(1, max(0, elapsed / total)) if total > 0 else 0
    draw.rounded_rectangle((s(64), s(463), s(836), s(473)), s(5), fill=(241, 227, 205))
    if ratio > 0:
        draw.rounded_rectangle((s(64), s(463), s(64 + 772 * ratio), s(473)), s(5), fill=(234, 164, 107))
    text(draw, 64, 483, "计时已暂停" if paused else f"本轮已持续 {duration(elapsed)}", 13, SOFT)
    text(draw, 836, 483, duration(total).removesuffix("00分") + "任务", 13, SOFT, align="right")
    footer(draw, 548)
    return finish(canvas)
