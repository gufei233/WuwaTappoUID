"""按 XWUIDCollection wwtappo 图鉴的小图格样式绘制图鉴与背包。"""

import io
import math
from dataclasses import dataclass
from functools import cache

from gsuid_core.logger import logger
from PIL import Image, ImageChops, ImageDraw, ImageEnhance, ImageOps

from .command_text import command_text
from .render_cache import LocalImage, images
from .render_common import (
    BG,
    BROWN,
    SOFT,
    Header,
    Progress,
    asset,
    background,
    composite,
    draw_text,
    ellipsize,
    footer,
    header,
    heading,
    image_key,
    prepared_sprite,
    progress_box,
    s,
)

CELL_W = 94
ROW_H = 122
RARITIES = ("epic", "rare", "uncommon", "common")
RARITY_LABELS = {"epic": "史诗", "rare": "稀有", "uncommon": "珍稀", "common": "普通"}
DOT_COLORS = {
    "epic": (240, 148, 96),
    "rare": (199, 138, 224),
    "uncommon": (127, 159, 224),
    "common": (156, 199, 122),
}


@dataclass
class AtlasItem:
    name: str
    rarity: str
    image: bytes | LocalImage | None
    quantity: int = 0


@dataclass
class AtlasGroup:
    rarity: str
    items: list[AtlasItem]
    owned: int
    total: int


@cache
def tile_background(rarity: str) -> Image.Image:
    return asset(f"tile_{rarity}.webp").resize((s(86), s(76)), Image.Resampling.LANCZOS)


@cache
def tile_mask() -> Image.Image:
    return asset("tile_mask.png").getchannel("A").resize((s(86), s(76)), Image.Resampling.LANCZOS)


def composed_tile(item: AtlasItem, rarity: str) -> Image.Image:
    key = ("tile", image_key(item.image) if item.image is not None else None, s(86), s(76), rarity, item.quantity > 0)
    cached = images.get(key)
    if cached is not None:
        return cached
    tile = tile_background(rarity).copy()
    if item.image:
        try:
            # 图鉴保留最终小图即可，避免中间缩放图与最终图重复占用预算。
            with prepared_sprite(item.image, s(92), s(92), retain=False) as picture:
                tile.alpha_composite(
                    picture, ((tile.width - picture.width) // 2, s(-4) + (s(92) - picture.height) // 2)
                )
        except OSError as error:
            logger.warning(
                f"[WuwaTappoUID] 图鉴物品图无法读取（{type(error).__name__}），"
                f"使用占位图；请发送 {command_text('更新资源')} 修复。"
            )
    tile.putalpha(ImageChops.multiply(tile.getchannel("A"), tile_mask()))
    if item.quantity == 0:
        gray = ImageOps.grayscale(tile).convert("RGBA")
        gray.putalpha(tile.getchannel("A"))
        tile.close()
        tile = ImageEnhance.Brightness(gray).enhance(1.12)
        gray.close()
        tile.putalpha(tile.getchannel("A").point(lambda alpha: round(alpha * 0.38)))
    return images.put(key, tile)


def draw_tile(canvas: Image.Image, draw: ImageDraw.ImageDraw, item: AtlasItem, x: float, y: float) -> None:
    rarity = item.rarity if item.rarity in RARITIES else "common"
    tile = composed_tile(item, rarity)
    composite(canvas, tile, (s(x + 4), s(y)))

    head, _, tail = item.name.partition(" · ")
    name_color = BROWN if item.quantity else (201, 183, 164)
    tail_color = SOFT if item.quantity else (214, 199, 182)
    draw_text(draw, x + CELL_W / 2, y + 80, 17, ellipsize(head, 13, 90), 13, name_color, align="center")
    draw_text(draw, x + CELL_W / 2, y + 97, 15, ellipsize(tail, 11, 90), 11, tail_color, align="center")
    if item.quantity > 1:
        label = str(item.quantity)
        badge_w = max(24, len(label) * 9 + 10)
        left, top = x + CELL_W - 4 - badge_w, y + 56
        draw.rounded_rectangle(
            (s(left), s(top), s(left + badge_w), s(top + 18)),
            radius=s(9),
            fill=(253, 249, 237),
        )
        draw_text(draw, left + badge_w / 2, top, 18, label, 13, (142, 123, 106), align="center")


def render_atlas_card(
    info: Header, title: str, groups: list[AtlasGroup], progress: list[Progress], note: str = ""
) -> bytes:
    """绘制一张按稀有度分组的 JPEG；未拥有项保留位置并灰化。"""
    columns = 12 if sum(len(group.items) for group in groups) > 300 else 9
    width = columns * CELL_W + 72
    heading_top = 257
    cursor = heading_top + 41 + 18 + (32 if note else 0)
    for group in groups:
        cursor += 36 + math.ceil(len(group.items) / columns) * ROW_H + 18
    if not groups:
        cursor += 70
    footer_top = cursor + 8
    height = footer_top + 26 + 24
    canvas = Image.new("RGB", (s(width), s(height)), BG)
    background(canvas, width)
    draw = ImageDraw.Draw(canvas)
    header(canvas, draw, info, width)
    progress_box(canvas, draw, 156, progress, width)
    heading(canvas, draw, heading_top, title, width)
    cursor = heading_top + 41 + 18
    if note:
        draw_text(draw, width / 2, cursor, 20, note, 15, SOFT, align="center")
        cursor += 32
    if not groups:
        draw_text(draw, width / 2, cursor + 16, 28, "尚未收集", 18, SOFT, align="center")
    for group in groups:
        dot = DOT_COLORS.get(group.rarity, DOT_COLORS["common"])
        draw.ellipse((s(40), s(cursor + 7), s(50), s(cursor + 17)), fill=dot)
        label = RARITY_LABELS.get(group.rarity, group.rarity)
        x = draw_text(draw, 56, cursor, 24, label, 20, BROWN)
        draw_text(draw, x + 10, cursor + 4, 20, f"{group.owned}/{group.total}", 15, SOFT)
        grid_top = cursor + 36
        for index, item in enumerate(group.items):
            row, col = divmod(index, columns)
            draw_tile(canvas, draw, item, 36 + col * CELL_W, grid_top + row * ROW_H)
        cursor = grid_top + math.ceil(len(group.items) / columns) * ROW_H + 18
    footer(draw, footer_top, width)
    output = io.BytesIO()
    canvas.save(output, "JPEG", quality=90, optimize=True)
    canvas.close()
    return output.getvalue()
