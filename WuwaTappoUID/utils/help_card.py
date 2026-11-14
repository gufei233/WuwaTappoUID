"""wt帮助：背包同款底图，分类栏目与史诗角色指令图标。"""

import io
import re
from functools import cache
from pathlib import Path

from gsuid_core.utils.fonts.fonts import core_font
from PIL import Image, ImageDraw, ImageFont

from .render_cache import current_config
from .render_common import BG, BROWN, SOFT, asset, background, draw_text, footer, paste_scaled, rounded_mask, s, shadow
from .settings import chest_task_hours, max_click_increment

# 图片与文字帮助共享指令和说明。
COMMANDS: list[tuple[str, str, str]] = [
    ("登录", "", "登录国服并保存SDK恢复凭据"),
    ("添加token", "TOKEN [海外]", "仅私聊，用 token 获取 CUID 并绑定账号"),
    ("查看", "", "查看账号编号、CUID 和区服"),
    ("删除", "[编号/CUID]", "按编号删除绑定，本人任务一并停止"),
    ("切换", "[编号/CUID]", "按编号切换；省略参数切到下一个"),
    ("信息", "", "查看角色、点击、冷却和任务状态"),
    ("背包", "[组合条件]", "分类、稀有度、角色、重复可组合；角色支持明确别名"),
    ("图鉴", "[角色/配件/表情]", "不带参数查看全部，可指定分类"),
    ("合成", "[模式] [分类]", "升阶/同阶与角色/配件/表情可组合；默认全部升阶，跳过收藏，每种留1件"),
    ("兑换", "CODE", "兑换当前账号的兑换码并展示背包变化"),
    ("次数", "", "查看累计、已消耗和可用点击"),
    ("增加次数", "N", "增加 N 次点击，单次最多{click_limit:,}"),
    ("宝箱", "", "开启{task_hours}小时任务；剩余1小时可续期"),
    ("开箱统计", "[今天/7天/30天/全部]", "默认7天，查看本插件开箱分布与掉落排名"),
    ("订阅", "", "10次后通知，中断或结束补发"),
    ("取消订阅", "", "任务结束汇总；中断仍发已有结果"),
    ("暂停", "", "暂停开箱和计时，恢复后继续"),
    ("恢复", "", "恢复登录及暂停任务，重新登录会顶掉其他客户端"),
    ("停止", "", "结束任务并汇总，保留账号绑定"),
    ("更新资源", "", "补齐新增、变化、缺失和损坏的图库"),
    ("强制更新资源", "", "覆盖更新名称和图库，仅限管理员"),
    ("状态", "", "账号、任务、通知积压与阶段状态，仅管理员"),
    ("帮助", "", "查看全部指令与说明"),
]

# 每条指令图标唯一：最高品质角色（Epic）优先，次高品质角色（Rare）补齐。
ICON_IDS = {
    "登录": 836,
    "添加token": 842,
    "查看": 848,
    "删除": 854,
    "切换": 860,
    "信息": 866,
    "背包": 872,
    "图鉴": 878,
    "合成": 884,
    "兑换": 194,
    "次数": 890,
    "增加次数": 128,
    "宝箱": 134,
    "开箱统计": 200,
    "订阅": 140,
    "取消订阅": 146,
    "暂停": 152,
    "恢复": 158,
    "停止": 164,
    "更新资源": 170,
    "强制更新资源": 176,
    "状态": 182,
    "帮助": 188,
}

GROUPS = (
    ("账号管理", ("登录", "添加token", "查看", "删除", "切换", "信息"), (248, 221, 181)),
    ("背包与合成", ("背包", "图鉴", "合成", "兑换", "次数", "增加次数"), (229, 236, 206)),
    ("自动开箱", ("宝箱", "开箱统计", "订阅", "取消订阅", "暂停", "恢复", "停止"), (250, 214, 192)),
    ("资源与帮助", ("更新资源", "强制更新资源", "状态", "帮助"), (237, 224, 230)),
)

WIDTH, PAD, GAP = 900, 36, 14
COLUMN_W = (WIDTH - PAD * 2 - GAP) / 2
ROW_PAD, ICON_W, TEXT_GAP = 18, 64, 12
DETAIL_SIZE, DETAIL_STEP = 15, 22
ICON_ROOT = Path(__file__).parent.parent / "assets" / "help-icons"
LEADING_PUNCT = "，。、；：！？）》」”’"
UNIT = re.compile(r"[A-Za-z0-9]+|「[^」]{1,8}」|.", re.S)


def commands() -> list[tuple[str, str, str]]:
    config = current_config.get()
    limit, hours = (
        (config.click_limit, config.task_hours) if config is not None else (max_click_increment(), chest_task_hours())
    )
    return [(name, arg, detail.format(click_limit=limit, task_hours=hours)) for name, arg, detail in COMMANDS]


def help_text(prefix: str) -> str:
    return "\n".join(f"{prefix}{name}{' ' + arg if arg else ''}：{detail}" for name, arg, detail in commands())


def wrap(text: str, font: ImageFont.FreeTypeFont, width: int) -> list[str]:
    lines, line = [], ""
    for unit in UNIT.findall(text):
        if line and font.getlength(line + unit) > width and unit not in LEADING_PUNCT:
            lines.append(line.rstrip())
            line = unit.lstrip()
        else:
            line += unit
    if line:
        lines.append(line)
    return lines


@cache
def command_icon(asset_id: int) -> Image.Image:
    path = ICON_ROOT / f"{asset_id}.png"
    with Image.open(path) as image:
        return image.convert("RGBA")


def panel(canvas: Image.Image, x: float, y: float, width: float, height: float) -> None:
    shape = rounded_mask((s(width), s(height)), s(16))
    shadow(canvas, shape, (s(x), s(y + 2)), (155, 103, 65, 18), 5)
    border = Image.new("RGBA", shape.size, (237, 217, 187, 255))
    border.putalpha(shape)
    canvas.alpha_composite(border, (s(x), s(y)))
    inner = rounded_mask((s(width) - 4, s(height) - 4), s(15))
    fill = Image.new("RGBA", inner.size, (255, 251, 242, 246))
    fill.putalpha(inner.point(lambda value: value * 246 // 255))
    canvas.alpha_composite(fill, (s(x) + 2, s(y) + 2))


def icon(canvas: Image.Image, asset_id: int, x: float, y: float) -> None:
    image = command_icon(asset_id)
    bounds = image.getbbox()
    if bounds is not None:
        image = image.crop(bounds)
    size = s(ICON_W)
    ratio = min(size / image.width, size / image.height)
    image = image.resize((round(image.width * ratio), round(image.height * ratio)), Image.LANCZOS)
    canvas.alpha_composite(image, (s(x + ICON_W / 2) - image.width // 2, s(y + ICON_W / 2) - image.height // 2))


def render(prefix: str) -> Image.Image:
    details = core_font(s(DETAIL_SIZE), 430)
    detail_width = s(COLUMN_W - ROW_PAD * 2 - ICON_W - TEXT_GAP)
    by_name = {name: (arg, detail) for name, arg, detail in commands()}
    prepared = []
    cursor = 166
    for index, (title, names, color) in enumerate(GROUPS, 1):
        rows = []
        section_y = cursor
        cursor += 46
        for start in range(0, len(names), 2):
            row = []
            for name in names[start : start + 2]:
                arg, detail = by_name[name]
                width = COLUMN_W
                available = s(width - ROW_PAD * 2 - ICON_W - TEXT_GAP)
                lines = wrap(detail, details, available if width != COLUMN_W else detail_width)
                height = ROW_PAD * 2 + 26 + 7 + DETAIL_STEP * len(lines)
                row.append((name, arg, lines, width, height))
            row_h = max(item[4] for item in row)
            rows.append((cursor, row, row_h))
            cursor += row_h + 12
        prepared.append((index, title, color, section_y, rows))
        cursor += 12
    height = cursor + 44 + 28
    canvas = Image.new("RGBA", (s(WIDTH), s(height)), BG + (255,))
    background(canvas)
    draw = ImageDraw.Draw(canvas)

    paste_scaled(canvas, asset("logo.webp"), PAD, 26, 190)
    draw_text(draw, WIDTH - PAD, 43, 46, f"{prefix}帮助", 34, BROWN, align="right")
    draw_text(draw, WIDTH - PAD, 100, 23, "WuwaTappo 指令手册", 16, SOFT, align="right", rounded=False, weight=500)
    draw.rounded_rectangle((s(PAD), s(143), s(WIDTH - PAD), s(145)), s(1), fill=(238, 215, 182))

    for index, title, color, section_y, rows in prepared:
        draw.rounded_rectangle((s(PAD), s(section_y), s(WIDTH - PAD), s(section_y + 34)), s(17), fill=color)
        draw.rounded_rectangle(
            (s(PAD + 7), s(section_y + 5), s(PAD + 43), s(section_y + 29)), s(12), fill=(255, 251, 242)
        )
        draw_text(
            draw, PAD + 25, section_y + 5, 24, f"{index:02d}", 12, BROWN, align="center", rounded=False, weight=630
        )
        draw_text(draw, PAD + 57, section_y + 3, 28, title, 18, BROWN)
        count = sum(len(row) for _, row, _ in rows)
        draw_text(draw, WIDTH - PAD - 18, section_y + 6, 22, f"{count} 条指令", 12, BROWN, align="right", rounded=False)
        for y, row, row_h in rows:
            for column, (name, arg, lines, width, _) in enumerate(row):
                x = PAD + column * (COLUMN_W + GAP)
                panel(canvas, x, y, width, row_h)
                icon(canvas, ICON_IDS[name], x + ROW_PAD, y + (row_h - ICON_W) / 2)
                text_x = x + ROW_PAD + ICON_W + TEXT_GAP
                right = draw_text(draw, text_x, y + ROW_PAD, 26, prefix + name, 19, BROWN, rounded=False, weight=660)
                if arg:
                    draw_text(draw, right + 8, y + ROW_PAD + 4, 20, arg, 13, (181, 112, 73), rounded=False, weight=520)
                text_y = y + ROW_PAD + 26 + 7
                for line in lines:
                    draw_text(draw, text_x, text_y, DETAIL_STEP, line, DETAIL_SIZE, SOFT, rounded=False, weight=430)
                    text_y += DETAIL_STEP

    footer(draw, cursor + 8, WIDTH)
    return canvas.convert("RGB")


def render_bytes(prefix: str) -> bytes:
    from gsuid_core.utils.image.convert import pic_quality

    with render(prefix) as image:
        output = io.BytesIO()
        image.save(output, "JPEG", quality=pic_quality)
        return output.getvalue()
