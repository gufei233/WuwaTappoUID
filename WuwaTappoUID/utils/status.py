"""GS 状态面板使用本地聚合统计，不请求游戏接口。"""

from pathlib import Path
from typing import TYPE_CHECKING

from gsuid_core.status.plugin_status import register_status
from PIL import Image
from sqlalchemy.exc import SQLAlchemyError

from .database import Store
from .schema import WuwaError

if TYPE_CHECKING:
    from .service import Service


def register_status_panel(service: "Service") -> None:
    async def metric(key: str) -> str | int:
        async def read() -> int:
            if key == "bindings":
                return (await Store.statistics())[0]
            return int((await Store.operational_statistics())[key] or 0)

        if not service.database_ready:
            return "数据库不可用"
        if not service.runtime.active:
            return "正在重载"
        try:
            return await service.runtime.run(read)
        except SQLAlchemyError as error:
            await service.disable_database(error)
            return "数据库不可用"
        except WuwaError:
            return "正在重载"

    async def bindings() -> str | int:
        return await metric("bindings")

    async def active() -> str | int:
        return await metric("active")

    async def pending() -> str | int:
        return await metric("pending")

    with Image.open(Path(__file__).parent.parent / "assets" / "tappo" / "logo.webp") as opened:
        icon = opened.convert("RGBA")
    register_status(icon, "WuwaTappoUID", {"绑定用户": bindings, "运行任务": active, "待发通知": pending})
