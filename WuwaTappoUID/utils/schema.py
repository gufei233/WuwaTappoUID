"""游戏接口的数据结构；缺失必要字段属于接口错误。"""

from datetime import datetime, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, Field, JsonValue

from .command_text import command_hint

BASE = "https://api-mc-wuwatappo.kurogames-global.com"
ORIGIN = "http://tauri.localhost"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 WuwaTappo/1.0"
CODE_OK = (0, 200)
CODE_TOKEN_INVALID = (10002, 10003, 90002)
TOKEN_LIFETIME = 7 * 24 * 3600
TOKEN_RENEW_WINDOW = 2 * 24 * 3600
TOKEN_RENEW_RETRY = 5 * 60
TOKEN_EXPIRED_MESSAGE = "token已过期，请发送 wt登录，或使用 wt添加token TOKEN 更新。"
RENEW_WINDOW = 60 * 60
AUTO_CLICKS = 10_000
# 请求发出这么久后该侧冷却仍未推进，就判定那次开箱没有生效（HTTP 超时为 25 秒）。
SETTLE_SECONDS = 120
SIDE_NAMES = {"left": "左", "right": "右"}
Side = Literal["left", "right"]


class WuwaError(Exception):
    """可向用户展示的中文错误，不携带 token、URL 查询串或原始响应。"""


class OperationStopped(WuwaError):
    """实例交接已开始，本次修改请求尚未发送。"""

    def __init__(self) -> None:
        super().__init__("插件正在重新加载，本次修改请求未发送，请稍后重试。")


class ApiError(WuwaError):
    def __init__(self, code: int | None, *, http_status: int | None = None, path: str = "", request_id: str = ""):
        self.code = code
        self.http_status = http_status
        self.path = path
        self.request_id = request_id
        messages = {
            10002: "登录鉴权被拒绝（10002）",
            10003: "登录鉴权被拒绝（10003）",
            90002: "账号已在其他设备登录（90002）",
            10024: command_hint("库洛登录凭据校验失败，请重新发送 wt登录（10024）"),
            10014: "服务端正在处理另一项操作（10014）",
            70004: "当前没有可用宝箱（70004）",
            70005: "点击不足（70005）",
            70006: "宝箱仍在冷却（70006）",
            50002: "合成失败（50002）",
            50006: "未拥有该物品（50006）",
            60002: "兑换失败（60002）",
            60003: "已使用过该兑换码（60003）",
            60004: "该兑换码无效（60004）",
        }
        message = (
            f"请求鉴权被拒绝（HTTP {http_status}，无业务错误码）"
            if code is None
            else messages.get(code, f"游戏接口返回错误（{code}）")
        )
        super().__init__(message)

    @property
    def is_auth(self) -> bool:
        return self.code in CODE_TOKEN_INVALID or self.code is None and self.http_status in (401, 403)

    @property
    def details(self) -> str:
        return f"接口 {self.path}，HTTP {self.http_status}，业务码 {self.code}，请求 ID {self.request_id or '未提供'}"


class NetworkError(WuwaError):
    """网络失败或服务端暂不可用；修改请求可能已经到达服务端。"""

    def __init__(self, message: str, details: str = ""):
        super().__init__(message)
        self.details = details or message


class MaintenanceError(WuwaError):
    """官方维护中，暂缓请求，不计入账号或通知失败预算。"""

    def __init__(self, details: str = ""):
        super().__init__("官方服务正在维护，已暂缓操作，请稍后再试。")
        self.details = details


class DataError(WuwaError):
    """响应数据不符合当前接口结构。"""


class DatabaseSchemaError(WuwaError):
    """插件表结构不完整，不能继续账号操作或恢复后台任务。"""


class AuthenticationError(WuwaError):
    """登录态失效或尚未验证，停止当前操作。"""


class ClicksUnavailable(WuwaError):
    """自动补充点击后仍无法开箱，交给用户处理。"""


class Expired(Exception):
    """本轮已到期，不再提交操作。"""


class Envelope(BaseModel):
    code: int
    data: JsonValue = None
    result: JsonValue = None
    request_id: str = ""

    def payload(self) -> JsonValue:
        if not ({"data", "result"} & self.model_fields_set):
            raise DataError("游戏接口响应缺少 data/result，请管理员检查接口。")
        return self.data if self.data is not None else self.result


class LoginData(BaseModel):
    token: str = Field(min_length=1, repr=False)
    expire_time: int | float | str | None = None


class SdkLogin(BaseModel):
    token: str = Field(min_length=1, max_length=4096, repr=False)
    cuid: str = Field(pattern=r"^[0-9]{6,16}$")
    sdk_openid: str = Field(min_length=1, max_length=128)
    username: str = Field(min_length=1, max_length=128)
    expires_in: int = Field(default=0, ge=0)


def token_expiry(value: int | float | str | None) -> float:
    if value is None or value == "":
        return 0
    if isinstance(value, str) and not value.replace(".", "", 1).isdigit():
        try:
            stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise DataError("登录响应的 expire_time 格式无法识别。") from None
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone(timedelta(hours=8)))
        return stamp.timestamp()
    number = float(value)
    return number / 1000 if number >= 1_000_000_000_000 else number


class DesktopCharacter(BaseModel):
    """user/me 当前桌宠外观；字段与官方客户端 appearanceAssetId/Name 一致。"""

    id: int | str
    name: str = ""


class UserInfo(BaseModel):
    id: int
    cuid: str | int
    nickname: str | None = None
    cname: str | None = None
    player_id: str | int
    server_id: str | int
    is_domestic: Literal[0, 1] | None = None
    room_no: str | int | None = None
    character: DesktopCharacter | None = None
    total_click_count: int
    total_click_consume: int
    left_treasure_chest: int
    right_treasure_chest: int

    @property
    def display_name(self) -> str:
        return (self.nickname or "").strip() or "漂泊者"

    @property
    def remaining(self) -> int:
        return self.total_click_count - self.total_click_consume

    def cooldown(self, side: Side) -> float:
        ms = self.left_treasure_chest if side == "left" else self.right_treasure_chest
        return ms / 1000


class Item(BaseModel):
    asset_id: int | str
    asset_name: str | None = None
    asset_type: str = "其他"
    quantity: int = 1
    rarity: str | int | None = None
    role_id: str | int = 0
    is_collect: Literal[0, 1] = 0


class OpenResult(BaseModel):
    asset_id: int | str
    asset_name: str | None = None
    rarity_type: str | int
    click_consume: int


class SynthesisResult(BaseModel):
    asset_id: int | str
    asset_name: str | None = None
    asset_type: str
    rarity_type: str | int


class Dictionary(BaseModel):
    character: dict[str, str]
    series: dict[str, str] = {}
    accessory: dict[str, str] = {}
    emote: dict[str, str] = {}


class RoleInfo(BaseModel):
    """登录前角色查询接口的一项。"""

    playerId: int | str
    playerName: str = ""
    level: int | None = None
    serverId: str = ""
    headPhoto: str = ""


class RoleQuery(BaseModel):
    items: list[RoleInfo] = []
    channelId: int | str | None = None


# 鸣潮服务器编号，取自 XutheringWavesUID 的 SERVER_ID / NET_SERVER_ID_MAP。
CN_SERVER_ID = "76402e5b20be2c39f095a152090afddc"
NET_SERVER_IDS = {
    "591d6af3a3090d8ea00d8f86cf6d7501",
    "6eb2a235b30d05efd77bedb5cf60999e",
    "86d52186155b148b5c138ceb41be9650",
    "919752ae5ea09c1ced910dd668a63ffb",
}


class AssetLayer(BaseModel):
    url: str


class AssetEntry(BaseModel):
    """外观清单中的主图、物品映射和桌宠 head1 头像层。"""

    url: str
    height_light_id: int | None = None
    head1: AssetLayer | None = None


class AssetManifest(BaseModel):
    character_skin_atlas: dict[str, AssetEntry] = {}
    accessory_atlas: dict[str, AssetEntry] = {}
    emote_atlas: dict[str, AssetEntry] = {}
    character_skin: dict[str, AssetEntry] = {}
    accessory: dict[str, AssetEntry] = {}
    emote: dict[str, AssetEntry] = {}

    def images(self) -> dict[str, str]:
        return {
            asset_id: entry.url
            for group in (self.character_skin, self.accessory, self.emote)
            for asset_id, entry in group.items()
        }

    def head_images(self) -> dict[str, str]:
        return {asset_id: entry.head1.url for asset_id, entry in self.character_skin.items() if entry.head1 is not None}

    def lookup(self, asset_id: str) -> str:
        for group in (self.character_skin, self.accessory, self.emote):
            entry = group.get(asset_id)
            if entry is not None:
                return entry.url
        return ""


class SessionImport(BaseModel):
    platform: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    cuid: str = Field(pattern=r"^[0-9]+$")
    domestic: Literal[0, 1] = 1
    token: str = Field(min_length=1, repr=False)
    expire_time: int | float | str | None = None
