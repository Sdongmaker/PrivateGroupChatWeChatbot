import asyncio
import json
import os
import random
import shutil
import stat
import time
import hashlib

import jwt as pyjwt
from aiohttp import web as aio_web

try:
    from PIL import Image as PILImage
    HAS_PIL = True
except ImportError:
    HAS_PIL = False
    # 尝试自动安装
    try:
        import subprocess, sys
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "Pillow>=9.0.0", "-q"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        from PIL import Image as PILImage
        HAS_PIL = True
    except Exception:
        pass

import astrbot.api.message_components as Comp
from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.api import logger

EMOJI_POOL = [
    "🦊", "🐼", "🦁", "🐨", "🐯", "🦄", "🐸", "🐧", "🦋", "🐝",
    "🦉", "🐬", "🐙", "🦈", "🐢", "🐾", "🦩", "🐻", "🐮", "🐷",
    "🐰", "🐶", "🐱", "🐵", "🐺", "🦆", "🦅", "🐴", "🦎", "🐘",
    "🦒", "🦇", "🐿️", "🦔", "🐞", "🦜", "🐡", "🦑", "🐠", "🦨",
    "🌻", "🌸", "🌺", "🍄", "🌵", "🎃", "⭐", "🌙", "🌈", "❄️",
]


class MemberRegistry:
    """管理匿名群组成员的注册信息，持久化到 JSON 文件。"""

    def __init__(self, data_path: str):
        self.data_path = data_path
        self.members: dict = {}
        self._load()

    def _load(self):
        if os.path.exists(self.data_path):
            try:
                with open(self.data_path, "r", encoding="utf-8") as f:
                    self.members = json.load(f)
            except (json.JSONDecodeError, IOError):
                logger.warning("成员数据文件损坏，将使用空数据")
                self.members = {}

    def _save(self):
        os.makedirs(os.path.dirname(self.data_path), exist_ok=True)
        with open(self.data_path, "w", encoding="utf-8") as f:
            json.dump(self.members, f, ensure_ascii=False, indent=2)

    def join(self, umo: str) -> str:
        if umo in self.members:
            return self.members[umo]["emoji"]
        used_emojis = {m["emoji"] for m in self.members.values()}
        available = [e for e in EMOJI_POOL if e not in used_emojis]
        emoji = random.choice(available) if available else random.choice(EMOJI_POOL)
        self.members[umo] = {"emoji": emoji, "joined_at": int(time.time())}
        self._save()
        return emoji

    def leave(self, umo: str) -> bool:
        if umo in self.members:
            del self.members[umo]
            self._save()
            return True
        return False

    def get_emoji(self, umo: str) -> str:
        return self.members[umo]["emoji"] if umo in self.members else "❓"

    def is_member(self, umo: str) -> bool:
        return umo in self.members

    def get_all_members(self) -> dict:
        return self.members

    def get_other_umos(self, umo: str) -> list[str]:
        return [m for m in self.members if m != umo]


class SessionModeRegistry:
    """管理每个会话的模式：relay 为虚拟群聊，private 为普通私聊。"""

    DEFAULT_MODE = "relay"
    VALID_MODES = {"relay", "private"}

    def __init__(self, data_path: str):
        self.data_path = data_path
        self.modes: dict[str, str] = {}
        self._load()

    def _load(self):
        if os.path.exists(self.data_path):
            try:
                with open(self.data_path, "r", encoding="utf-8") as f:
                    self.modes = json.load(f)
            except (json.JSONDecodeError, IOError):
                logger.warning("模式数据文件损坏，将使用默认模式")
                self.modes = {}

    def _save(self):
        os.makedirs(os.path.dirname(self.data_path), exist_ok=True)
        with open(self.data_path, "w", encoding="utf-8") as f:
            json.dump(self.modes, f, ensure_ascii=False, indent=2)

    def get_mode(self, umo: str) -> str:
        mode = self.modes.get(umo, self.DEFAULT_MODE)
        return mode if mode in self.VALID_MODES else self.DEFAULT_MODE

    def set_mode(self, umo: str, mode: str):
        normalized = mode if mode in self.VALID_MODES else self.DEFAULT_MODE
        self.modes[umo] = normalized
        self._save()

    def is_relay_mode(self, umo: str) -> bool:
        return self.get_mode(umo) == "relay"


class ManagedBotsRegistry:
    """追踪由 WebBridge API 创建的 bot 实例。"""

    def __init__(self, data_path: str):
        self.data_path = data_path
        self.bots: dict = {}
        self._load()

    def _load(self):
        if os.path.exists(self.data_path):
            try:
                with open(self.data_path, "r", encoding="utf-8") as f:
                    self.bots = json.load(f)
            except (json.JSONDecodeError, IOError):
                logger.warning("managed_bots 数据文件损坏，将使用空数据")
                self.bots = {}

    def _save(self):
        os.makedirs(os.path.dirname(self.data_path), exist_ok=True)
        with open(self.data_path, "w", encoding="utf-8") as f:
            json.dump(self.bots, f, ensure_ascii=False, indent=2)

    def add(self, platform_id: str, created_by: str):
        self.bots[platform_id] = {
            "created_at": int(time.time()),
            "created_by": created_by,
            "status": "active",
        }
        self._save()

    def remove(self, platform_id: str) -> bool:
        if platform_id in self.bots:
            del self.bots[platform_id]
            self._save()
            return True
        return False

    def contains(self, platform_id: str) -> bool:
        return platform_id in self.bots

    def get_all(self) -> dict:
        return self.bots


# ── WebBridge: 独立 HTTP Server ─────────────────────────


class WebBridge:
    """管理 aiohttp HTTP server 的完整生命周期、JWT 认证、API 路由。"""

    VERSION = "1.4.0"
    SYSTEM_EMOJI = "🤖"

    def __init__(self, plugin, data_dir: str, port: int = 6196, jwt_secret: str = "",
                 video_serve_base_url: str = ""):
        self.plugin = plugin
        self._data_dir = data_dir
        self._port = port
        self._video_serve_base_url = video_serve_base_url.rstrip("/") if video_serve_base_url else ""
        self._app: aio_web.Application | None = None
        self._runner: aio_web.AppRunner | None = None
        self._site: aio_web.TCPSite | None = None
        self._jwt_secret = jwt_secret if jwt_secret else self._init_jwt_secret()
        self._managed_bots = ManagedBotsRegistry(
            os.path.join(data_dir, "managed_bots.json")
        )

    # ── JWT secret ───────────────────────────────────────

    def _init_jwt_secret(self) -> str:
        secret_path = os.path.join(self._data_dir, "jwt_secret.txt")
        if os.path.exists(secret_path):
            with open(secret_path, "r", encoding="utf-8") as f:
                return f.read().strip()
        secret = os.urandom(32).hex()
        with open(secret_path, "w", encoding="utf-8") as f:
            f.write(secret)
        try:
            os.chmod(secret_path, stat.S_IRUSR | stat.S_IWUSR)
        except OSError:
            pass
        logger.info(
            f"[匿名树洞] WebBridge JWT secret 已生成并保存到 {secret_path} "
            f"(secret={secret}) — 请复制给外部 Web 项目"
        )
        return secret

    # ── Middleware ────────────────────────────────────────

    @aio_web.middleware
    async def _auth_middleware(self, request: aio_web.Request, handler):
        if request.path == "/api/health":
            return await handler(request)
        if request.path.startswith("/temp/"):
            return await handler(request)
        if not request.path.startswith("/api/"):
            return await handler(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header:
            return aio_web.json_response(
                {"error": "Missing Authorization header"}, status=401
            )
        if not auth_header.startswith("Bearer "):
            return aio_web.json_response(
                {"error": "Invalid Authorization format"}, status=401
            )

        token = auth_header.removeprefix("Bearer ").strip()
        try:
            payload = pyjwt.decode(
                token, self._jwt_secret, algorithms=["HS256"],
                options={"require": ["exp"]},
            )
            request["jwt_sub"] = payload.get("sub", "unknown")
        except pyjwt.ExpiredSignatureError:
            return aio_web.json_response(
                {"error": "Token expired"}, status=401
            )
        except pyjwt.InvalidTokenError:
            return aio_web.json_response(
                {"error": "Invalid token"}, status=401
            )

        return await handler(request)

    # ── Lifecycle ────────────────────────────────────────

    def _register_routes(self):
        self._app.router.add_get("/api/health", self._handle_health)
        self._app.router.add_post("/api/bot/create", self._handle_bot_create)
        self._app.router.add_get(
            "/api/bot/{platform_id}/qr", self._handle_bot_qr
        )
        self._app.router.add_get("/api/bot/list", self._handle_bot_list)
        self._app.router.add_delete(
            "/api/bot/{platform_id}", self._handle_bot_delete
        )
        self._app.router.add_post("/api/group/send", self._handle_group_send)
        self._app.router.add_get(
            "/api/group/status", self._handle_group_status
        )
        self._app.router.add_get("/temp/{filename}", self._handle_temp_file)

    async def start(self):
        self._app = aio_web.Application(middlewares=[self._auth_middleware])
        self._register_routes()
        self._runner = aio_web.AppRunner(self._app)
        await self._runner.setup()
        self._site = aio_web.TCPSite(self._runner, "0.0.0.0", self._port)
        await self._site.start()

    async def stop(self):
        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()

    # ── Helpers ──────────────────────────────────────────

    def _find_platform(self, platform_id: str):
        for inst in self.plugin.context.platform_manager.platform_insts:
            if inst.meta().id == platform_id:
                return inst
        return None

    def _log(self, level: str, action: str, **fields):
        self.plugin._log_behavior(level, action, **fields)

    def get_video_url(self, file_path: str) -> str | None:
        """给定本地文件路径，返回通过 WebBridge HTTP 可访问的 URL。
        需要 video_serve_base_url 已配置。"""
        if not self._video_serve_base_url:
            return None
        filename = os.path.basename(file_path)
        return f"{self._video_serve_base_url}/temp/{filename}"

    async def _handle_temp_file(self, request: aio_web.Request):
        """提供 data/temp 目录下的文件，用于跨容器视频中转。"""
        filename = request.match_info["filename"]
        # 安全检查：只允许文件名，不允许路径穿越
        if "/" in filename or "\\" in filename or ".." in filename:
            return aio_web.Response(status=403, text="Forbidden")
        # 在 AstrBot temp 目录查找
        temp_dir = os.path.join("data", "temp")
        file_path = os.path.join(temp_dir, filename)
        if not os.path.isfile(file_path):
            return aio_web.Response(status=404, text="File not found")
        return aio_web.FileResponse(file_path)

    # ── Handlers ─────────────────────────────────────────

    async def _handle_health(self, request: aio_web.Request):
        return aio_web.json_response({
            "status": "ok",
            "version": self.VERSION,
            "member_count": len(self.plugin.registry.get_all_members()),
            "managed_bots_count": len(self._managed_bots.get_all()),
        })

    async def _handle_bot_create(self, request: aio_web.Request):
        platform_id = f"weixin_oc_ext_{int(time.time())}"
        config = {
            "id": platform_id,
            "type": "weixin_oc",
            "enable": True,
            "weixin_oc_bot_type": "3",
            "weixin_oc_base_url": "https://ilinkai.weixin.qq.com",
            "weixin_oc_token": "",
            "weixin_oc_account_id": "",
            "weixin_oc_qr_poll_interval": 1,
        }

        ctx = self.plugin.context
        try:
            ctx._config["platform"].append(config)
            ctx._config.save_config()
            await ctx.platform_manager.load_platform(config)
        except Exception as e:
            # rollback
            ctx._config["platform"] = [
                p for p in ctx._config["platform"] if p.get("id") != platform_id
            ]
            try:
                ctx._config.save_config()
            except Exception:
                pass
            import traceback
            err_detail = traceback.format_exc()
            self._log("error", "bot_create_failed", platform_id=platform_id, error=err_detail)
            return aio_web.json_response(
                {"error": f"Platform load failed: {e}", "detail": err_detail}, status=500
            )

        sub = request.get("jwt_sub", "unknown")
        self._managed_bots.add(platform_id, sub)
        self._log("info", "bot_created", platform_id=platform_id, created_by=sub)
        return aio_web.json_response({
            "platform_id": platform_id,
            "status": "qr_pending",
        })

    async def _handle_bot_qr(self, request: aio_web.Request):
        platform_id = request.match_info["platform_id"]
        if not self._managed_bots.contains(platform_id):
            return aio_web.json_response(
                {"error": "Bot not found or not managed by this plugin"},
                status=404,
            )

        inst = self._find_platform(platform_id)
        if inst is None:
            return aio_web.json_response(
                {"error": "Bot not found or not managed by this plugin"},
                status=404,
            )

        stats = inst.get_stats()
        wx_info = stats.get("weixin_oc", {})

        qr_status = wx_info.get("qr_status")
        qr_url = wx_info.get("qrcode_img_content")
        qr_error = wx_info.get("qr_error")

        if qr_status is None and not wx_info.get("configured"):
            qr_status = "initializing"

        return aio_web.json_response({
            "platform_id": platform_id,
            "qr_url": qr_url,
            "status": qr_status,
            "error": qr_error,
        })

    async def _handle_bot_list(self, request: aio_web.Request):
        bots_data = self._managed_bots.get_all()
        result = []
        for pid, info in bots_data.items():
            inst = self._find_platform(pid)
            runtime_status = "unknown"
            qr_status = None
            configured = False
            if inst is not None:
                try:
                    runtime_status = inst.status.value
                except Exception:
                    runtime_status = "unknown"
                stats = inst.get_stats()
                wx_info = stats.get("weixin_oc", {})
                qr_status = wx_info.get("qr_status")
                configured = bool(wx_info.get("configured", False))
            result.append({
                "platform_id": pid,
                "created_at": info.get("created_at"),
                "created_by": info.get("created_by"),
                "runtime_status": runtime_status,
                "qr_status": qr_status,
                "configured": configured,
            })
        return aio_web.json_response({"bots": result})

    async def _handle_bot_delete(self, request: aio_web.Request):
        platform_id = request.match_info["platform_id"]
        if not self._managed_bots.contains(platform_id):
            return aio_web.json_response(
                {"error": "Bot not found or not managed by this plugin"},
                status=404,
            )

        ctx = self.plugin.context
        try:
            await ctx.platform_manager.terminate_platform(platform_id)
        except Exception as e:
            self._log("error", "bot_delete_terminate_failed",
                       platform_id=platform_id, error=str(e))
            return aio_web.json_response(
                {"error": f"Failed to terminate platform: {e}"}, status=500
            )

        ctx._config["platform"] = [
            p for p in ctx._config["platform"] if p.get("id") != platform_id
        ]
        try:
            ctx._config.save_config()
        except Exception as e:
            self._log("warning", "bot_delete_config_save_failed",
                       platform_id=platform_id, error=str(e))

        # 清理注册在该平台上的成员
        all_members = list(self.plugin.registry.get_all_members().keys())
        removed_members = []
        for umo in all_members:
            if umo.startswith(platform_id + ":"):
                emoji_rm = self.plugin.registry.get_emoji(umo)
                self.plugin.registry.leave(umo)
                self.plugin.mode_registry.set_mode(umo, "private")
                removed_members.append(emoji_rm)

        self._managed_bots.remove(platform_id)
        self._log("info", "bot_deleted", platform_id=platform_id,
                  members_removed=len(removed_members),
                  removed_emojis=removed_members)
        return aio_web.json_response({
            "platform_id": platform_id,
            "deleted": True,
            "members_removed": len(removed_members),
        })

    async def _handle_group_send(self, request: aio_web.Request):
        try:
            body = await request.json()
        except Exception:
            return aio_web.json_response(
                {"error": "Invalid JSON body"}, status=400
            )

        text = (body.get("text") or "").strip()
        if not text or len(text) > 2000:
            return aio_web.json_response(
                {"error": "Text is required and must be 1-2000 characters"},
                status=400,
            )

        members = self.plugin.registry.get_all_members()

        delivered = 0
        failed = 0
        for umo in members:
            try:
                mc = MessageChain(chain=[Comp.Plain(f"{self.SYSTEM_EMOJI} | {text}")])
                await self.plugin.context.send_message(umo, mc)
                delivered += 1
            except Exception as e:
                failed += 1
                self._log("error", "system_broadcast_delivery_failed",
                           target=self.plugin._mask_umo(umo), error=str(e))

        self._log("info", "system_broadcast",
                   delivered=delivered, failed=failed, total=len(members))
        return aio_web.json_response({
            "delivered": delivered,
            "failed": failed,
            "total": len(members),
        })

    async def _handle_group_status(self, request: aio_web.Request):
        members = self.plugin.registry.get_all_members()
        member_list = [
            {"emoji": info["emoji"], "joined_at": info.get("joined_at")}
            for info in members.values()
        ]

        managed = self._managed_bots.get_all()
        running = 0
        for pid in managed:
            inst = self._find_platform(pid)
            if inst is not None:
                try:
                    if inst.status.value == "running":
                        running += 1
                except Exception:
                    pass

        return aio_web.json_response({
            "member_count": len(members),
            "members": member_list,
            "managed_bots_total": len(managed),
            "managed_bots_running": running,
        })


@register(
    "PrivateGroupChatWeChatbot",
    "笨笨",
    "跨微信号匿名群聊插件：给 Bot 发私聊，自动广播给所有成员；支持外部 Web 项目通过 API 接入",
    "1.4.0",
    "https://github.com/Sdongmaker/PrivateGroupChatWeChatbot",
)
class AnonymousGroupPlugin(Star):
    LOG_PREFIX = "[匿名树洞]"

    def __init__(self, context: Context, config=None):
        super().__init__(context)
        self._config = config
        data_dir = os.path.join("data", "astrbot_plugin_PrivateGroupChatWeChatbot")
        os.makedirs(data_dir, exist_ok=True)
        self.registry = MemberRegistry(os.path.join(data_dir, "members.json"))
        self.mode_registry = SessionModeRegistry(os.path.join(data_dir, "modes.json"))

        # 从 schema 配置面板读取值
        cfg_port = 6196
        cfg_secret = ""
        cfg_video_url = ""
        self._group_push_targets: list[dict] = []
        if config:
            cfg_port = config.get("web_bridge_port", 6196)
            cfg_secret = config.get("jwt_secret", "")
            cfg_video_url = config.get("video_serve_base_url", "")
            raw_targets = config.get("group_push_targets", "")
            self._group_push_targets = self._parse_group_push_targets(raw_targets)

        self.web_bridge = WebBridge(
            self, data_dir, port=cfg_port, jwt_secret=cfg_secret,
            video_serve_base_url=cfg_video_url,
        )

        # 如果面板里 jwt_secret 留空，将自动生成的值回写到配置面板
        if config and not cfg_secret:
            config["jwt_secret"] = self.web_bridge._jwt_secret
            try:
                config.save_config()
            except Exception:
                pass

        self._server_task: asyncio.Task | None = None
        self._log_behavior(
            "info",
            "plugin_loaded",
            member_count=len(self.registry.get_all_members()),
        )

    async def initialize(self):
        """插件激活时启动 WebBridge HTTP server。"""
        if not HAS_PIL:
            self._log_behavior(
                "warning", "pillow_not_available",
                hint="Pillow 库未安装，Telegram 贴纸（WebP）将无法自动转换为 PNG。"
                     "请运行: pip install Pillow>=9.0.0",
            )
        if not self.web_bridge._video_serve_base_url:
            self._log_behavior(
                "warning", "video_serve_not_configured",
                hint="video_serve_base_url 未配置，跨容器视频中转可能失败。"
                     "请在插件配置中设置（如 http://astrbot:6196）",
            )
        self._server_task = asyncio.create_task(self._run_web_bridge())

    async def _run_web_bridge(self):
        try:
            await self.web_bridge.start()
            self._log_behavior(
                "info", "web_bridge_started", port=self.web_bridge._port
            )
        except Exception as e:
            self._log_behavior(
                "error", "web_bridge_start_failed", error=str(e)
            )

    @staticmethod
    def _parse_group_push_targets(raw: str) -> list[dict]:
        """解析群推送配置，格式: platform_id:group_id,platform_id:group_id,..."""
        targets = []
        if not raw or not raw.strip():
            return targets
        for item in raw.split(","):
            item = item.strip()
            if ":" not in item:
                continue
            platform_id, group_id = item.split(":", 1)
            platform_id, group_id = platform_id.strip(), group_id.strip()
            if platform_id and group_id:
                targets.append({"platform_id": platform_id, "group_id": group_id})
        return targets

    def _get_platform_adapter_name(self, platform_id: str) -> str:
        """获取平台适配器类型名，如 'aiocqhttp'、'telegram' 等。"""
        for inst in self.context.platform_manager.platform_insts:
            if inst.meta().id == platform_id:
                return inst.meta().name
        return "unknown"

    def _extract_platform_id(self, umo: str) -> str:
        """从 unified_msg_origin 中提取平台 ID（第一个 ':' 之前的部分）。"""
        return umo.split(":", 1)[0] if ":" in umo else umo

    def _is_platform_alive(self, umo: str) -> bool:
        """检查 UMO 对应的平台是否仍然存在。"""
        platform_id = self._extract_platform_id(umo)
        for inst in self.context.platform_manager.platform_insts:
            if inst.meta().id == platform_id:
                return True
        return False

    def _mask_umo(self, umo: str) -> str:
        """对用户来源做稳定脱敏，避免在日志中直接暴露原始标识。"""
        digest = hashlib.sha256(umo.encode("utf-8")).hexdigest()[:12]
        return f"user:{digest}"

    def _summarize_components(
        self, components: list[Comp.BaseMessageComponent]
    ) -> dict[str, int]:
        summary: dict[str, int] = {}
        for comp in components:
            component_type = "other"
            if isinstance(comp, Comp.Plain):
                if not comp.text.strip():
                    continue
                component_type = "text"
            elif isinstance(comp, Comp.Image):
                component_type = "image"
            elif isinstance(comp, Comp.Record):
                component_type = "record"
            elif isinstance(comp, Comp.Video):
                component_type = "video"
            elif isinstance(comp, Comp.File):
                component_type = "file"
            elif isinstance(comp, Comp.Face):
                component_type = "face"
            elif isinstance(comp, Comp.Reply):
                component_type = "reply"

            summary[component_type] = summary.get(component_type, 0) + 1
        return summary or {"empty": 1}

    def _log_behavior(self, level: str, action: str, **fields):
        parts = [self.LOG_PREFIX, f"action={action}"]
        for key, value in fields.items():
            if value is None:
                continue
            rendered = json.dumps(value, ensure_ascii=False, default=str)
            parts.append(f"{key}={rendered}")
        message = " ".join(parts)

        if level == "warning":
            logger.warning(message)
        elif level == "error":
            logger.error(message)
        else:
            logger.info(message)

    def _mode_label(self, mode: str) -> str:
        return "虚拟群聊模式" if mode == "relay" else "私聊模式"

    # ── 指令 ──────────────────────────────────────────────

    @filter.command("join", alias={"加入", "加入群组", "群聊模式", "树洞模式"}, priority=10)
    @filter.event_message_type(filter.EventMessageType.PRIVATE_MESSAGE)
    async def join_group(self, event: AstrMessageEvent):
        """切换到匿名群聊模式"""
        umo = event.unified_msg_origin
        sender_id = self._mask_umo(umo)
        current_mode = self.mode_registry.get_mode(umo)
        already_member = self.registry.is_member(umo)
        self.mode_registry.set_mode(umo, "relay")
        emoji = self.registry.join(umo)
        count = len(self.registry.get_all_members())

        if already_member and current_mode == "relay":
            self._log_behavior(
                "info",
                "join_repeat",
                sender=sender_id,
                emoji=emoji,
                member_count=count,
            )
            yield event.plain_result(
                f"ℹ️ 你当前已经处于虚拟群聊模式\n"
                f"你的身份标识: {emoji}\n"
                f"当前群组人数: {count}\n\n"
                f"直接发消息即可匿名群聊\n"
                f"/leave  切换到私聊模式\n"
                f"/anon_status  查看当前状态"
            )
            event.stop_event()
            return

        self._log_behavior(
            "info",
            "switch_to_relay",
            sender=sender_id,
            emoji=emoji,
            member_count=count,
        )
        yield event.plain_result(
            f"✅ 已切换到虚拟群聊模式\n"
            f"你的身份标识: {emoji}\n"
            f"当前群组人数: {count}\n\n"
            f"现在开始，你发给 Bot 的私聊消息会匿名转发给其他成员\n"
            f"/leave  切换到私聊模式\n"
            f"/members  查看成员"
        )
        if not already_member:
            await self._notify_others(
                umo, f"📢 新成员 {emoji} 加入了群组！（当前 {count} 人）"
            )
        event.stop_event()

    @filter.command("leave", alias={"退出", "离开", "私聊模式"}, priority=10)
    @filter.event_message_type(filter.EventMessageType.PRIVATE_MESSAGE)
    async def leave_group(self, event: AstrMessageEvent):
        """切换到私聊模式"""
        umo = event.unified_msg_origin
        sender_id = self._mask_umo(umo)
        emoji = self.registry.get_emoji(umo)
        was_member = self.registry.leave(umo)
        self.mode_registry.set_mode(umo, "private")
        count = len(self.registry.get_all_members())
        self._log_behavior(
            "info",
            "switch_to_private",
            sender=sender_id,
            emoji=emoji if was_member else None,
            was_member=was_member,
            member_count=count,
        )
        yield event.plain_result(
            "🛌 已切换到私聊模式\n"
            "后续消息将不再进入匿名群聊，而是交给 AstrBot 默认流程处理\n"
            "发送 /join 可重新回到匿名群聊模式"
        )
        if was_member:
            await self._notify_others(
                umo, f"📢 成员 {emoji} 离开了群组（当前 {count} 人）"
            )
        event.stop_event()

    @filter.command("members", alias={"成员", "成员列表"}, priority=10)
    @filter.event_message_type(filter.EventMessageType.PRIVATE_MESSAGE)
    async def list_members(self, event: AstrMessageEvent):
        """查看群组成员列表"""
        members = self.registry.get_all_members()
        sender_id = self._mask_umo(event.unified_msg_origin)
        if not members:
            self._log_behavior("info", "members_view_empty", sender=sender_id)
            yield event.plain_result("群组暂无成员，发送 /join 加入")
            event.stop_event()
            return
        umo = event.unified_msg_origin
        self._log_behavior(
            "info",
            "members_view",
            sender=sender_id,
            member_count=len(members),
        )
        lines = [f"👥 匿名群组成员（{len(members)} 人）：\n"]
        for m_umo, info in members.items():
            marker = " ← 你" if m_umo == umo else ""
            lines.append(f"  {info['emoji']}{marker}")
        yield event.plain_result("\n".join(lines))
        event.stop_event()

    @filter.command("anon_status", alias={"树洞状态", "群聊状态", "当前状态"}, priority=10)
    @filter.event_message_type(filter.EventMessageType.PRIVATE_MESSAGE)
    async def status_cmd(self, event: AstrMessageEvent):
        """查看当前会话是虚拟群聊模式还是私聊模式"""
        umo = event.unified_msg_origin
        mode = self.mode_registry.get_mode(umo)
        is_member = self.registry.is_member(umo)
        emoji = self.registry.get_emoji(umo) if is_member else "未分配"
        count = len(self.registry.get_all_members())
        self._log_behavior(
            "info",
            "status_view",
            sender=self._mask_umo(umo),
            mode=mode,
            member_count=count,
        )
        yield event.plain_result(
            f"当前模式: {self._mode_label(mode)}\n"
            f"是否在匿名群组中: {'是' if is_member else '否'}\n"
            f"身份标识: {emoji}\n"
            f"当前群组人数: {count}\n\n"
            f"/join 切换到虚拟群聊模式\n"
            f"/leave 切换到私聊模式"
        )
        event.stop_event()

    @filter.command("anon_help", alias={"树洞帮助"}, priority=10)
    @filter.event_message_type(filter.EventMessageType.PRIVATE_MESSAGE)
    async def help_cmd(self, event: AstrMessageEvent):
        """查看匿名群组帮助"""
        self._log_behavior(
            "info",
            "help_view",
            sender=self._mask_umo(event.unified_msg_origin),
        )
        yield event.plain_result(
            "🌲 匿名树洞 使用指南\n\n"
            "默认模式：虚拟群聊模式\n"
            "首次私聊发送普通消息时，会自动加入匿名群组并中转\n\n"
            "/join — 切换到虚拟群聊模式\n"
            "/leave — 切换到私聊模式\n"
            "/members — 查看成员列表\n"
            "/anon_status — 查看当前模式\n"
            "/anon_help — 查看本帮助\n\n"
            "处于虚拟群聊模式时，私聊消息会匿名广播给其他成员，默认不走 LLM。\n"
            "切换到私聊模式后，消息会回到 AstrBot 默认处理流程。\n"
            "支持文字、图片、语音、视频、文件。"
        )
        event.stop_event()

    # ── 消息广播 ─────────────────────────────────────────

    @filter.event_message_type(filter.EventMessageType.PRIVATE_MESSAGE)
    async def on_private_message(self, event: AstrMessageEvent):
        """监听私聊消息并广播给匿名群组其他成员"""
        umo = event.unified_msg_origin
        if not self.mode_registry.is_relay_mode(umo):
            return
        # 跳过指令消息（保险起见）
        msg_str = (event.message_str or "").strip()
        if msg_str.startswith("/"):
            return

        sender_id = self._mask_umo(umo)
        auto_joined = False
        if not self.registry.is_member(umo):
            auto_joined = True
            emoji = self.registry.join(umo)
            count = len(self.registry.get_all_members())
            self._log_behavior(
                "info",
                "auto_join",
                sender=sender_id,
                emoji=emoji,
                member_count=count,
            )
            await self._notify_others(
                umo, f"📢 新成员 {emoji} 加入了群组！（当前 {count} 人）"
            )
        else:
            emoji = self.registry.get_emoji(umo)

        components = event.get_messages()
        content_summary = self._summarize_components(components)
        others = self.registry.get_other_umos(umo)
        if not others:
            self._log_behavior(
                "warning",
                "broadcast_skipped",
                sender=sender_id,
                emoji=emoji,
                reason="no_receivers",
                content=content_summary,
            )
            if auto_joined:
                yield event.plain_result(
                    f"✅ 已自动进入虚拟群聊模式\n"
                    f"你的身份标识: {emoji}\n"
                    f"当前群组中只有你一人，等待其他成员加入吧～\n"
                    f"发送 /leave 可切回私聊模式"
                )
            else:
                yield event.plain_result("群组中只有你一人，等待其他成员加入吧～")
            event.stop_event()
            return

        # 构建广播消息链列表（文字一条，每个媒体各一条）
        broadcast_chains = await self._build_broadcast_chains(
            emoji, components, sender_id
        )
        self._log_behavior(
            "info",
            "broadcast_chains_built",
            sender=sender_id,
            chain_count=len(broadcast_chains),
        )

        # 广播给所有其他成员（并发发送给不同用户）
        fail_count = 0
        stale_umos = []
        active_umos = []
        for other_umo in others:
            if not self._is_platform_alive(other_umo):
                stale_umos.append(other_umo)
                self._log_behavior(
                    "warning",
                    "broadcast_skip_dead_platform",
                    sender=sender_id,
                    target=self._mask_umo(other_umo),
                    platform=self._extract_platform_id(other_umo),
                )
            else:
                active_umos.append(other_umo)

        async def _send_to_user(target_umo: str):
            target_id = self._mask_umo(target_umo)
            platform = self._extract_platform_id(target_umo)
            for idx, chain in enumerate(broadcast_chains):
                try:
                    mc = MessageChain(chain=list(chain))
                    await self.context.send_message(target_umo, mc)
                    self._log_behavior(
                        "info",
                        "chain_sent",
                        target=target_id,
                        platform=platform,
                        index=idx + 1,
                        total=len(broadcast_chains),
                    )
                except Exception as e:
                    self._log_behavior(
                        "error",
                        "chain_send_failed",
                        target=target_id,
                        platform=platform,
                        index=idx + 1,
                        total=len(broadcast_chains),
                        error=str(e),
                    )
                    raise
                # 微信平台对同一用户连续发送有频率限制，加延迟避免丢消息
                if "weixin" in platform and idx < len(broadcast_chains) - 1:
                    await asyncio.sleep(1.0)

        results = await asyncio.gather(
            *[_send_to_user(umo) for umo in active_umos],
            return_exceptions=True,
        )
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self._log_behavior(
                    "error",
                    "broadcast_delivery_failed",
                    sender=sender_id,
                    target=self._mask_umo(active_umos[i]),
                    error=str(result),
                )
                fail_count += 1

        # 群聊推送：将广播消息同步推送到配置的群聊
        group_push_ok = 0
        group_push_fail = 0
        for target in self._group_push_targets:
            group_umo = f"{target['platform_id']}:GroupMessage:{target['group_id']}"
            try:
                for idx, chain in enumerate(broadcast_chains):
                    mc = MessageChain(chain=list(chain))
                    await self.context.send_message(group_umo, mc)
                group_push_ok += 1
                self._log_behavior(
                    "info",
                    "group_push_sent",
                    sender=sender_id,
                    platform=target["platform_id"],
                    group_id=target["group_id"],
                )
            except Exception as e:
                group_push_fail += 1
                self._log_behavior(
                    "error",
                    "group_push_failed",
                    sender=sender_id,
                    platform=target["platform_id"],
                    group_id=target["group_id"],
                    error=str(e),
                )
        if self._group_push_targets:
            self._log_behavior(
                "info" if group_push_fail == 0 else "warning",
                "group_push_summary",
                sender=sender_id,
                total=len(self._group_push_targets),
                ok=group_push_ok,
                failed=group_push_fail,
            )

        # 自动清理已失效平台的成员
        for stale in stale_umos:
            emoji_removed = self.registry.get_emoji(stale)
            self.registry.leave(stale)
            self.mode_registry.set_mode(stale, "private")
            self._log_behavior(
                "info",
                "auto_remove_stale_member",
                target=self._mask_umo(stale),
                emoji=emoji_removed,
                platform=self._extract_platform_id(stale),
            )

        active_recipients = len(others) - len(stale_umos)
        self._log_behavior(
            "info" if fail_count == 0 else "warning",
            "broadcast",
            sender=sender_id,
            emoji=emoji,
            recipients=active_recipients,
            delivered=active_recipients - fail_count,
            failed=fail_count,
            stale_removed=len(stale_umos),
            content=content_summary,
        )

        if auto_joined and fail_count == 0:
            yield event.plain_result(
                f"✅ 已自动进入虚拟群聊模式\n"
                f"你的身份标识: {emoji}\n"
                f"本条消息已匿名广播给 {active_recipients} 人\n"
                f"发送 /leave 可切回私聊模式"
            )
        elif fail_count > 0:
            yield event.plain_result(f"⚠️ 消息已发送，但有 {fail_count} 人未收到")
        if stale_umos:
            self._log_behavior(
                "info",
                "stale_cleanup_summary",
                removed=len(stale_umos),
                remaining=len(self.registry.get_all_members()),
            )
        event.stop_event()

    # ── 内部方法 ─────────────────────────────────────────

    async def _build_broadcast_chains(
        self,
        emoji: str,
        components: list[Comp.BaseMessageComponent],
        sender_id: str,
    ) -> list[list[Comp.BaseMessageComponent]]:
        """根据收到的消息构建广播消息链列表：文字一条，每个媒体各一条。"""
        chains: list[list[Comp.BaseMessageComponent]] = []

        # 检查是否有引用回复
        reply_prefix = ""
        for comp in components:
            if isinstance(comp, Comp.Reply):
                reply_prefix = "[回复消息] "
                break

        # 收集文本
        text_parts: list[str] = []
        for comp in components:
            if isinstance(comp, Comp.Plain):
                t = comp.text.strip()
                if t:
                    text_parts.append(t)

        # 第一条：emoji 前缀 + 文本
        prefix = f"{emoji} | {reply_prefix}"
        if text_parts:
            chains.append([Comp.Plain(f"{prefix}{' '.join(text_parts)}")])
        else:
            # 没有文本时先占位，后面可能改写
            chains.append([Comp.Plain(prefix)])

        has_media = False

        # 每个媒体组件拆成独立消息
        img_idx = 0
        for comp in components:
            if isinstance(comp, Comp.Image):
                has_media = True
                img_idx += 1
                try:
                    file_path = await comp.convert_to_file_path()
                    # WebP 贴纸转换为 PNG（兼容不支持 WebP 的平台）
                    file_path = self._convert_webp_if_needed(file_path, sender_id, img_idx)
                    self._log_behavior(
                        "info",
                        "image_resolved",
                        sender=sender_id,
                        index=img_idx,
                        method="file",
                        path=str(file_path),
                    )
                    chains.append([Comp.Image.fromFileSystem(file_path)])
                except Exception as e:
                    url = getattr(comp, "url", None) or getattr(comp, "file", "")
                    if url and str(url).startswith("http"):
                        self._log_behavior(
                            "info",
                            "image_resolved",
                            sender=sender_id,
                            index=img_idx,
                            method="url_fallback",
                            url=str(url)[:120],
                        )
                        chains.append([Comp.Image.fromURL(str(url))])
                    else:
                        self._log_behavior(
                            "warning",
                            "media_unavailable",
                            sender=sender_id,
                            component="image",
                            index=img_idx,
                            error=str(e),
                        )
                        chains.append([Comp.Plain("[图片无法转发]")])
                        
            elif isinstance(comp, Comp.Record):
                has_media = True
                try:
                    file_path = await comp.convert_to_file_path()
                    chains.append([Comp.Record(file=file_path, url=file_path)])
                except Exception as e:
                    self._log_behavior(
                        "warning",
                        "media_unavailable",
                        sender=sender_id,
                        component="record",
                        error=str(e),
                    )
                    chains.append([Comp.Plain("[语音无法转发]")])
            elif isinstance(comp, Comp.Video):
                has_media = True
                try:
                    file_path = await comp.convert_to_file_path()
                    # 确保视频文件有 .mp4 扩展名（NapCat 等 OB11 实现要求）
                    file_path = self._ensure_video_extension(file_path, sender_id)
                    self._log_behavior(
                        "info",
                        "video_file_resolved",
                        sender=sender_id,
                        file_path=str(file_path),
                    )
                    # 优先通过 HTTP URL 提供视频（解决跨容器路径不可达问题）
                    video_url = self.web_bridge.get_video_url(file_path)
                    if video_url:
                        self._log_behavior(
                            "info",
                            "video_served_via_http",
                            sender=sender_id,
                            url=video_url,
                        )
                        chains.append([Comp.Video(file=video_url)])
                    else:
                        chains.append([Comp.Video.fromFileSystem(path=file_path)])
                except Exception as e:
                    # 尝试在 AstrBot temp 目录搜索 OB11 裸文件名
                    resolved = self._try_resolve_ob11_video(
                        getattr(comp, "file", ""), sender_id
                    )
                    if resolved:
                        video_url = self.web_bridge.get_video_url(resolved)
                        if video_url:
                            chains.append([Comp.Video(file=video_url)])
                        else:
                            chains.append([Comp.Video.fromFileSystem(path=resolved)])
                    else:
                        url = getattr(comp, "url", None) or getattr(comp, "file", "")
                        if url and str(url).startswith("http"):
                            self._log_behavior(
                                "info",
                                "video_url_fallback",
                                sender=sender_id,
                                url=str(url)[:120],
                            )
                            chains.append([Comp.Video(file=str(url))])
                        else:
                            self._log_behavior(
                                "warning",
                                "media_unavailable",
                                sender=sender_id,
                                component="video",
                                error=str(e),
                                raw_attrs={
                                    "url": str(getattr(comp, "url", None)),
                                    "file": str(getattr(comp, "file", None)),
                                },
                            )
                            chains.append([Comp.Plain("[视频无法转发]")])
            elif isinstance(comp, Comp.File):
                has_media = True
                try:
                    local = await comp.get_file(allow_return_url=False)
                    name = getattr(comp, "name", "file")
                    chains.append([Comp.File(name=name, file=local)])
                except Exception as e:
                    self._log_behavior(
                        "warning",
                        "media_unavailable",
                        sender=sender_id,
                        component="file",
                        error=str(e),
                    )
                    chains.append([Comp.Plain("[文件无法转发]")])
            elif isinstance(comp, Comp.Face):
                has_media = True

        # 如果没有文本也没有媒体（纯表情等），改写占位文本
        if not text_parts and not has_media:
            chains[0] = [Comp.Plain(f"{prefix}[表情]")]

        return chains

    def _ensure_video_extension(self, file_path: str, sender_id: str) -> str:
        """确保视频文件路径有 .mp4 扩展名（NapCat 等 OB11 实现要求）。"""
        file_path = os.path.abspath(file_path)
        if os.path.splitext(file_path)[1].lower() in (".mp4", ".avi", ".mkv", ".mov", ".flv"):
            return file_path
        dest = file_path + ".mp4"
        try:
            shutil.copy2(file_path, dest)
            self._log_behavior(
                "info",
                "video_ext_fixed",
                sender=sender_id,
                original=file_path,
                renamed=dest,
            )
            return dest
        except Exception as e:
            self._log_behavior(
                "warning",
                "video_ext_fix_failed",
                sender=sender_id,
                error=str(e),
            )
            return file_path

    def _try_resolve_ob11_video(self, raw_file: str, sender_id: str) -> str | None:
        """尝试在 AstrBot temp 目录下查找 OB11 传来的裸文件名视频。"""
        if not raw_file or raw_file.startswith("http") or raw_file.startswith("file:///"):
            return None
        # 尝试在常见 temp 目录搜索
        search_dirs = [
            os.path.join("data", "temp"),
            "/tmp",
        ]
        basename = os.path.basename(raw_file)
        for d in search_dirs:
            candidate = os.path.join(d, basename)
            if os.path.isfile(candidate):
                resolved = os.path.abspath(candidate)
                resolved = self._ensure_video_extension(resolved, sender_id)
                self._log_behavior(
                    "info",
                    "video_ob11_resolved",
                    sender=sender_id,
                    raw=raw_file,
                    resolved=resolved,
                )
                return resolved
        self._log_behavior(
            "warning",
            "video_ob11_not_found",
            sender=sender_id,
            raw=raw_file,
            searched=search_dirs,
        )
        return None

    def _convert_webp_if_needed(
        self, file_path: str, sender_id: str, img_idx: int
    ) -> str:
        """如果图片是 WebP 格式，转换为 PNG（兼容不支持 WebP 的平台如 QQ）。"""
        if not HAS_PIL:
            return file_path
        try:
            is_webp = False
            # 先检查扩展名
            if file_path.lower().endswith(".webp"):
                is_webp = True
            else:
                # 检查文件头 (RIFF....WEBP)
                with open(file_path, "rb") as f:
                    header = f.read(12)
                if header[:4] == b"RIFF" and header[8:12] == b"WEBP":
                    is_webp = True
            if not is_webp:
                return file_path
            png_path = os.path.splitext(file_path)[0] + ".png"
            with PILImage.open(file_path) as img:
                img.save(png_path, "PNG")
            self._log_behavior(
                "info",
                "webp_converted",
                sender=sender_id,
                index=img_idx,
                original=file_path,
                converted=png_path,
            )
            return png_path
        except Exception as e:
            self._log_behavior(
                "warning",
                "webp_convert_failed",
                sender=sender_id,
                index=img_idx,
                error=str(e),
            )
            return file_path

    async def _notify_others(self, sender_umo: str, text: str):
        """向除 sender 外的所有成员发送通知消息。"""
        others = self.registry.get_other_umos(sender_umo)
        sender_id = self._mask_umo(sender_umo)
        for other_umo in others:
            try:
                mc = MessageChain(chain=[Comp.Plain(text)])
                await self.context.send_message(other_umo, mc)
            except Exception as e:
                self._log_behavior(
                    "error",
                    "notification_failed",
                    sender=sender_id,
                    target=self._mask_umo(other_umo),
                    error=str(e),
                )

    async def terminate(self):
        """插件卸载时的清理。"""
        if self.web_bridge:
            await self.web_bridge.stop()
            self._log_behavior("info", "web_bridge_stopped")
        if self._server_task and not self._server_task.done():
            self._server_task.cancel()
        self._log_behavior(
            "info",
            "plugin_unloaded",
            member_count=len(self.registry.get_all_members()),
        )
