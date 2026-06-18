from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import mimetypes
import os
import secrets
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote

from core.chat import KiraIMSentResult, KiraMessageBatchEvent, MessageChain
from core.chat.message_elements import BaseMessageElement, File, Video
from core.logging_manager import get_logger
from core.plugin import BasePlugin, Priority, on, register
from core.prompt_manager import Prompt
from core.provider import LLMRequest
from core.utils.path_utils import get_data_path


PLUGIN_ID = "napcat_large_file_transport"
DEFAULT_USAGE_PROMPT = (
    "当你需要通过 QQ/NapCat 发送文件时：普通小文件可以使用 `<file>path_or_url</file>`；"
    "明确是大文件、视频、压缩包或可能超过数 MB 的文件时，优先使用 "
    "`<napcat_file type=\"file\">path_or_url</napcat_file>`，视频可使用 "
    "`<napcat_file type=\"video\">path_or_url</napcat_file>`。不要把大文件内容转换成 base64 文本输出，"
    "也不要把文件内容直接粘贴到回复里。文件路径可以是绝对路径、`data/files/...`、`data/temp/...` 或 HTTP/HTTPS URL。"
)
FORMAT_SAFETY_PROMPT = (
    "发送文件时，每个 `<file>` 或 `<napcat_file>` 必须单独放在一个 `<msg>` 中；"
    "不要在同一个 `<msg>` 里混入解释文字、emoji、多个文件标签或其他标签。"
)

logger = get_logger(PLUGIN_ID, "orange")


class StreamUnsupportedError(RuntimeError):
    pass


@dataclass
class DownloadToken:
    path: Path
    name: str
    expires_at: float
    cleanup: bool = False


class NapCatLargeFileTransportPlugin(BasePlugin):
    """Large file transport shim for KiraAI QQ/NapCat adapters."""

    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)
        self.enabled = True
        self.intercept_existing_file_tag = True
        self.stream_enabled = True
        self.http_fallback_enabled = True
        self.public_base_url = ""
        self.stream_threshold_mb = 8
        self.base64_max_mb = 20
        self.chunk_size_kb = 512
        self.transfer_timeout_sec = 300
        self.download_token_ttl_sec = 600
        self.qq_adapter_names: set[str] = set()
        self.debug_log = False

        self._default_usage_prompt = self._load_default_usage_prompt()
        self._original_methods: dict[tuple[str, str], Callable] = {}
        self._download_tokens: dict[str, DownloadToken] = {}
        self._cleanup_task: Optional[asyncio.Task] = None
        self._stream_available: Optional[bool] = None

    async def initialize(self):
        self._load_config()
        if not self.enabled:
            logger.info("NapCat large file transport plugin disabled")
            return

        if self.intercept_existing_file_tag:
            self._patch_available_adapters()

        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("NapCat large file transport plugin initialized")

    async def terminate(self):
        self._restore_adapters()
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

        for token, entry in list(self._download_tokens.items()):
            self._remove_token(token, entry)
        self._download_tokens.clear()

    def _load_config(self):
        cfg = self.plugin_cfg if isinstance(self.plugin_cfg, dict) else {}
        self.enabled = bool(cfg.get("enabled", True))
        self.intercept_existing_file_tag = bool(cfg.get("intercept_existing_file_tag", True))
        self.stream_enabled = bool(cfg.get("stream_enabled", True))
        self.http_fallback_enabled = bool(cfg.get("http_fallback_enabled", True))
        self.public_base_url = str(cfg.get("public_base_url") or "").strip().rstrip("/")
        self.stream_threshold_mb = max(1, int(cfg.get("stream_threshold_mb", 8) or 8))
        self.base64_max_mb = max(1, int(cfg.get("base64_max_mb", 20) or 20))
        self.chunk_size_kb = max(64, int(cfg.get("chunk_size_kb", 512) or 512))
        self.transfer_timeout_sec = max(10, int(cfg.get("transfer_timeout_sec", 300) or 300))
        self.download_token_ttl_sec = max(60, int(cfg.get("download_token_ttl_sec", 600) or 600))
        names = cfg.get("qq_adapter_names") or []
        self.qq_adapter_names = {str(name).strip() for name in names if str(name).strip()}
        self.debug_log = bool(cfg.get("debug_log", False))

    def _patch_available_adapters(self):
        adapter_mgr = getattr(self.ctx, "adapter_mgr", None)
        if not adapter_mgr:
            logger.warning("Adapter manager is not available; skip NapCat adapter patching")
            return

        adapters = {}
        if hasattr(adapter_mgr, "get_adapters"):
            try:
                adapters = adapter_mgr.get_adapters() or {}
            except Exception as e:
                logger.warning(f"Failed to read adapters from adapter manager: {e}")
        if not adapters:
            adapters = getattr(adapter_mgr, "_adapters", {}) or {}

        patched = []
        for adapter_name, adapter in list(adapters.items()):
            if not self._is_target_adapter(str(adapter_name), adapter):
                continue
            self._patch_adapter(str(adapter_name), adapter)
            patched.append(str(adapter_name))

        if patched:
            logger.info(f"NapCat large file transport patched adapters: {patched}")
        else:
            logger.info("No matching QQ/NapCat adapter found for large file transport")

    def _is_target_adapter(self, adapter_name: str, adapter: Any) -> bool:
        if self.qq_adapter_names and adapter_name not in self.qq_adapter_names:
            return False
        if (adapter_name, "group") in self._original_methods:
            return False
        bot = getattr(adapter, "bot", None)
        if not bot or not hasattr(bot, "send_action"):
            return False
        return hasattr(adapter, "send_group_message") and hasattr(adapter, "send_direct_message")

    def _patch_adapter(self, adapter_name: str, adapter: Any):
        existing_originals = getattr(adapter, "_napcat_large_file_transport_originals", None)
        if isinstance(existing_originals, dict) and "group" in existing_originals and "direct" in existing_originals:
            original_group = existing_originals["group"]
            original_direct = existing_originals["direct"]
        else:
            original_group = adapter.send_group_message
            original_direct = adapter.send_direct_message
            setattr(adapter, "_napcat_large_file_transport_originals", {"group": original_group, "direct": original_direct})
        self._original_methods[(adapter_name, "group")] = original_group
        self._original_methods[(adapter_name, "direct")] = original_direct

        async def group_wrapper(group_id, send_message_obj, _adapter=adapter, _original=original_group):
            return await self._wrapped_send(_adapter, "group", group_id, send_message_obj, _original)

        async def direct_wrapper(user_id, send_message_obj, _adapter=adapter, _original=original_direct):
            return await self._wrapped_send(_adapter, "direct", user_id, send_message_obj, _original)

        adapter.send_group_message = group_wrapper
        adapter.send_direct_message = direct_wrapper

    def _restore_adapters(self):
        adapter_mgr = getattr(self.ctx, "adapter_mgr", None)
        adapters = {}
        if adapter_mgr and hasattr(adapter_mgr, "get_adapters"):
            try:
                adapters = adapter_mgr.get_adapters() or {}
            except Exception:
                adapters = {}
        if not adapters and adapter_mgr:
            adapters = getattr(adapter_mgr, "_adapters", {}) or {}

        for (adapter_name, kind), original in list(self._original_methods.items()):
            adapter = adapters.get(adapter_name)
            if not adapter:
                continue
            if kind == "group":
                adapter.send_group_message = original
            elif kind == "direct":
                adapter.send_direct_message = original
            if hasattr(adapter, "_napcat_large_file_transport_originals"):
                try:
                    delattr(adapter, "_napcat_large_file_transport_originals")
                except Exception:
                    pass
        self._original_methods.clear()

    async def _wrapped_send(self, adapter: Any, chat_type: str, target_id: Any, send_message_obj: MessageChain, original: Callable):
        if not self.enabled or not self.intercept_existing_file_tag:
            return await original(target_id, send_message_obj)

        try:
            chain = await self._maybe_process_chain(adapter, send_message_obj)
            media = self._single_media_element(chain)
            if not media:
                return await original(target_id, send_message_obj)

            decision = self._should_take_over(media)
            if not decision:
                return await original(target_id, send_message_obj)

            return await self._send_large_media(adapter, chat_type, target_id, media)
        except Exception as e:
            logger.exception(f"NapCat large file transport failed: {e}")
            media = None
            try:
                media = self._single_media_element(send_message_obj)
            except Exception:
                pass
            if media and (self._is_dangerous_base64(media) or self._should_take_over(media)):
                return KiraIMSentResult(None, ok=False, err=f"NapCat large file transport failed and blocked unsafe fallback: {e}")
            return await original(target_id, send_message_obj)

    async def _maybe_process_chain(self, adapter: Any, send_message_obj: MessageChain):
        processor = getattr(adapter, "_process_outgoing_message", None)
        if callable(processor):
            try:
                return await processor(send_message_obj)
            except Exception as e:
                self._debug(f"Adapter outgoing preprocessing failed, using original chain: {e}")
        return send_message_obj

    @staticmethod
    def _single_media_element(chain: MessageChain) -> Optional[BaseMessageElement]:
        try:
            items = list(chain)
        except TypeError:
            return None
        if len(items) != 1:
            return None
        media = [item for item in items if isinstance(item, (File, Video))]
        if len(media) != 1:
            return None
        return media[0]

    def _should_take_over(self, media: BaseMessageElement) -> bool:
        if getattr(media, "_napcat_large_file_force", False):
            return True
        if getattr(media, "file_type", "") == "url":
            return False

        size = self._media_size(media)
        if size is not None and size >= self._mb(self.stream_threshold_mb):
            return True
        if self._is_dangerous_base64(media):
            return True
        return False

    def _is_dangerous_base64(self, media: BaseMessageElement) -> bool:
        if getattr(media, "file_type", "") not in ("base64", "data_url"):
            return False
        size = self._media_size(media)
        return size is not None and size > self._mb(self.base64_max_mb)

    def _media_size(self, media: BaseMessageElement) -> Optional[int]:
        raw_size = getattr(media, "size", None)
        try:
            if raw_size:
                return int(raw_size)
        except (TypeError, ValueError):
            pass

        file_type = getattr(media, "file_type", "")
        value = getattr(media, "file", "") or ""
        if file_type == "path":
            try:
                return os.path.getsize(value)
            except OSError:
                return None
        if file_type == "base64":
            return self._estimate_base64_size(value)
        if file_type == "data_url":
            try:
                return self._estimate_base64_size(value.split(",", 1)[1])
            except IndexError:
                return None
        return None

    @staticmethod
    def _estimate_base64_size(value: str) -> Optional[int]:
        if not value:
            return 0
        text = value.strip()
        if text.startswith("base64://"):
            text = text.removeprefix("base64://")
        text = "".join(text.split())
        padding = len(text) - len(text.rstrip("="))
        try:
            return max(0, (len(text) * 3) // 4 - padding)
        except Exception:
            return None

    async def _send_large_media(self, adapter: Any, chat_type: str, target_id: Any, media: BaseMessageElement) -> KiraIMSentResult:
        start = time.monotonic()
        filename = self._media_name(media)
        file_type = getattr(media, "file_type", "")
        is_video = isinstance(media, Video)

        try:
            if file_type == "url":
                file_ref = getattr(media, "file", "")
                self._debug(f"Direct NapCat upload for URL media name={filename}")
                return await self._upload_reference(adapter, chat_type, target_id, file_ref, filename, is_video)

            path = Path(await media.to_path()).resolve()
            if not path.is_file():
                return KiraIMSentResult(None, ok=False, err=f"File not found: {path}")

            file_size = path.stat().st_size
            cleanup_temp = self._is_media_temp_path(media, path)
            self._debug(
                f"Large media strategy start name={filename} size={file_size} "
                f"type={file_type} chat={chat_type} media={'video' if is_video else 'file'}"
            )

            stream_error = ""
            if self.stream_enabled and self._stream_available is not False:
                try:
                    napcat_path = await self._upload_file_stream(adapter, path, filename)
                    result = await self._upload_reference(adapter, chat_type, target_id, napcat_path, filename, is_video)
                    if cleanup_temp:
                        self._remove_local_temp(path)
                    self._debug_result("stream", filename, result, time.monotonic() - start)
                    return result
                except StreamUnsupportedError as e:
                    self._stream_available = False
                    stream_error = str(e)
                    self._debug(f"upload_file_stream unsupported, fallback to HTTP: {stream_error}")
                except Exception as e:
                    stream_error = str(e)
                    self._debug(f"upload_file_stream failed, fallback to HTTP: {stream_error}")

            if self.http_fallback_enabled:
                url_created = False
                try:
                    url = self._create_download_url(path, filename, cleanup=cleanup_temp)
                    url_created = True
                    result = await self._upload_reference(adapter, chat_type, target_id, url, filename, is_video)
                    self._debug_result("http", filename, result, time.monotonic() - start)
                    return result
                except Exception as e:
                    if cleanup_temp and not url_created:
                        self._remove_local_temp(path)
                    detail = f"HTTP fallback failed: {e}"
                    if stream_error:
                        detail = f"stream failed: {stream_error}; {detail}"
                    return KiraIMSentResult(None, ok=False, err=detail)

            if stream_error:
                if cleanup_temp:
                    self._remove_local_temp(path)
                return KiraIMSentResult(None, ok=False, err=f"upload_file_stream failed and HTTP fallback is disabled: {stream_error}")
            if cleanup_temp:
                self._remove_local_temp(path)
            return KiraIMSentResult(None, ok=False, err="No large-file transport strategy is enabled")
        except Exception as e:
            return KiraIMSentResult(None, ok=False, err=f"NapCat large file transport failed: {e}")

    async def _upload_file_stream(self, adapter: Any, path: Path, filename: str) -> str:
        bot = getattr(adapter, "bot", None)
        if not bot or not hasattr(bot, "send_action"):
            raise RuntimeError("Adapter has no NapCat send_action client")

        size = path.stat().st_size
        chunk_size = self.chunk_size_kb * 1024
        total_chunks = max(1, math.ceil(size / chunk_size))
        stream_id = uuid.uuid4().hex
        expected_sha256 = self._sha256(path)
        file_retention = max(self.download_token_ttl_sec, 600) * 1000
        self._debug(
            f"upload_file_stream start name={filename} size={size} chunks={total_chunks} "
            f"chunk_size={chunk_size} stream_id={self._mask_token(stream_id)}"
        )

        with path.open("rb") as f:
            for chunk_index in range(total_chunks):
                chunk = f.read(chunk_size)
                if not chunk and size > 0:
                    break
                params = {
                    "stream_id": stream_id,
                    "chunk_index": chunk_index,
                    "total_chunks": total_chunks,
                    "file_size": size,
                    "filename": filename,
                    "expected_sha256": expected_sha256,
                    "file_retention": file_retention,
                    "chunk_data": base64.b64encode(chunk).decode("ascii"),
                }
                resp = await bot.send_action("upload_file_stream", params, timeout=self.transfer_timeout_sec)
                self._raise_for_stream_response(resp)

        complete_params = {
            "stream_id": stream_id,
            "is_complete": True,
            "total_chunks": total_chunks,
            "file_size": size,
            "filename": filename,
            "expected_sha256": expected_sha256,
            "file_retention": file_retention,
        }
        resp = await bot.send_action("upload_file_stream", complete_params, timeout=self.transfer_timeout_sec)
        self._raise_for_stream_response(resp)
        file_path = self._extract_stream_file_path(resp)
        if not file_path:
            raise RuntimeError(f"upload_file_stream completed without file_path: {self._summarize_response(resp)}")
        self._stream_available = True
        self._debug(
            f"upload_file_stream complete name={filename} chunks={total_chunks} "
            f"napcat_ref={self._describe_reference(file_path)}"
        )
        return file_path

    def _raise_for_stream_response(self, resp: Any):
        if not isinstance(resp, dict):
            raise RuntimeError(f"invalid stream response: {resp!r}")
        if resp.get("status") == "ok":
            return
        retcode = resp.get("retcode")
        message = self._summarize_response(resp)
        if retcode == 1404 or "unsupported" in message.lower() or "not found" in message.lower() or "unknown action" in message.lower():
            raise StreamUnsupportedError(message)
        raise RuntimeError(message)

    @staticmethod
    def _extract_stream_file_path(resp: dict) -> Optional[str]:
        data = resp.get("data")
        if isinstance(data, dict):
            for key in ("file_path", "path", "file"):
                value = data.get(key)
                if value:
                    return str(value)
            nested = data.get("data")
            if isinstance(nested, dict):
                for key in ("file_path", "path", "file"):
                    value = nested.get(key)
                    if value:
                        return str(value)
        for key in ("file_path", "path", "file"):
            value = resp.get(key)
            if value:
                return str(value)
        return None

    async def _upload_reference(self, adapter: Any, chat_type: str, target_id: Any, file_ref: str, filename: str, is_video: bool) -> KiraIMSentResult:
        bot = getattr(adapter, "bot", None)
        if not bot or not hasattr(bot, "send_action"):
            return KiraIMSentResult(None, ok=False, err="Adapter has no NapCat send_action client")

        if is_video:
            if chat_type == "group":
                action = "send_group_msg"
                params = {
                    "group_id": target_id,
                    "message": [{"type": "video", "data": {"name": filename, "file": file_ref}}],
                }
            else:
                action = "send_private_msg"
                params = {
                    "user_id": target_id,
                    "message": [{"type": "video", "data": {"name": filename, "file": file_ref}}],
                }
        elif chat_type == "group":
            action = "upload_group_file"
            params = {"group_id": str(target_id), "file": file_ref, "name": filename}
        else:
            action = "upload_private_file"
            params = {"user_id": str(target_id), "file": file_ref, "name": filename}

        self._debug(
            f"NapCat final upload action={action} chat={chat_type} media={'video' if is_video else 'file'} "
            f"name={filename} ref={self._describe_reference(file_ref)}"
        )
        resp = await bot.send_action(action, params, timeout=self.transfer_timeout_sec)
        return self._result_from_response(resp, f"Failed to send {'video' if is_video else 'file'}")

    @staticmethod
    def _result_from_response(resp: Any, prefix: str) -> KiraIMSentResult:
        result = KiraIMSentResult(None)
        if not isinstance(resp, dict):
            result.ok = False
            result.err = f"{prefix}: invalid response {resp!r}"
            return result
        if resp.get("status") != "ok":
            result.ok = False
            result.err = f"{prefix}: {NapCatLargeFileTransportPlugin._summarize_response(resp)}"
            return result
        data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
        msg_id = data.get("message_id") or data.get("file_id") or data.get("id")
        if msg_id is not None:
            result.message_id = str(msg_id)
        return result

    def _create_download_url(self, path: Path, filename: str, cleanup: bool = False) -> str:
        if not self.public_base_url:
            raise RuntimeError("HTTP fallback requires public_base_url that NapCat can access")
        resolved = path.resolve()
        if not resolved.is_file():
            raise RuntimeError(f"File not found: {resolved}")

        token = secrets.token_urlsafe(32)
        self._download_tokens[token] = DownloadToken(
            path=resolved,
            name=filename,
            expires_at=time.time() + self.download_token_ttl_sec,
            cleanup=cleanup,
        )
        return f"{self.public_base_url}/api/plugin/{PLUGIN_ID}/download/{quote(token)}"

    @register.api("GET", "/download/{token}", auth=False)
    async def download(self, token: str):
        from fastapi.responses import FileResponse, PlainTextResponse

        entry = self._download_tokens.get(token)
        if not entry:
            return PlainTextResponse("download token not found", status_code=404)
        if entry.expires_at <= time.time():
            self._remove_token(token, entry)
            return PlainTextResponse("download token expired", status_code=410)
        if not entry.path.is_file():
            self._remove_token(token, entry)
            return PlainTextResponse("file not found", status_code=404)

        media_type = mimetypes.guess_type(entry.name)[0] or "application/octet-stream"
        return FileResponse(str(entry.path), filename=entry.name, media_type=media_type)

    @register.tag("napcat_file", "Send a file through the NapCat large-file transport. Usage: <napcat_file type=\"file|video\">path_or_url</napcat_file>", parent="msg")
    async def napcat_file_tag(self, value: str, **kwargs) -> list[BaseMessageElement]:
        file_type = str(kwargs.get("type") or "file").lower()
        if file_type not in ("file", "video"):
            file_type = "file"

        file_string, name = self._resolve_tag_file(value)
        if not file_string:
            logger.warning(f"napcat_file tag skipped missing file: {value}")
            return []

        element: BaseMessageElement
        if file_type == "video":
            element = Video(file=file_string, name=name)
        else:
            element = File(file=file_string, name=name)
        setattr(element, "_napcat_large_file_force", True)
        return [element]

    @on.llm_request(priority=Priority.HIGH)
    async def inject_usage_prompt(self, _event: KiraMessageBatchEvent, req: LLMRequest, *_):
        if not self.enabled:
            return
        usage_prompt = self._get_usage_prompt()
        if not usage_prompt:
            return
        prompt = Prompt(usage_prompt, name=f"{PLUGIN_ID}_usage_prompt", source=PLUGIN_ID)
        self._insert_prompt_after(req.system_prompt, prompt, after_name="tools")

    def _resolve_tag_file(self, value: str) -> tuple[Optional[str], Optional[str]]:
        raw = (value or "").strip().replace("\\", "/")
        if not raw:
            return None, None
        if raw.startswith(("http://", "https://")):
            return raw, None
        path = Path(raw)
        if path.is_file():
            return str(path.resolve()), path.name
        if raw.startswith("data/"):
            data_path = get_data_path() / raw.removeprefix("data/")
            if data_path.is_file():
                return str(data_path.resolve()), data_path.name
        return None, None

    @staticmethod
    def _insert_prompt_after(prompts: list[Prompt], prompt: Prompt, after_name: str):
        for idx, item in enumerate(prompts):
            if isinstance(item, Prompt) and item.name == after_name:
                prompts.insert(idx + 1, prompt)
                return
        for idx, item in enumerate(prompts):
            if isinstance(item, Prompt) and item.name == "output":
                prompts.insert(idx + 1, prompt)
                return
        prompts.append(prompt)

    def _get_usage_prompt(self) -> str:
        cfg = self.plugin_cfg if isinstance(self.plugin_cfg, dict) else {}
        if "usage_prompt" in cfg:
            usage_prompt = str(cfg.get("usage_prompt") or "").strip()
        else:
            usage_prompt = str(self._default_usage_prompt or DEFAULT_USAGE_PROMPT).strip()
        if usage_prompt and "单独放在一个 `<msg>`" not in usage_prompt:
            usage_prompt = f"{usage_prompt}\n\n{FORMAT_SAFETY_PROMPT}"
        return usage_prompt

    @staticmethod
    def _load_default_usage_prompt() -> str:
        schema_path = Path(__file__).with_name("schema.json")
        try:
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
            return str(schema.get("usage_prompt", {}).get("default") or "").strip()
        except Exception as e:
            logger.warning(f"Failed to load default usage prompt: {e}")
            return DEFAULT_USAGE_PROMPT

    async def _cleanup_loop(self):
        while True:
            await asyncio.sleep(30)
            now = time.time()
            for token, entry in list(self._download_tokens.items()):
                if entry.expires_at <= now:
                    self._remove_token(token, entry)

    def _remove_token(self, token: str, entry: DownloadToken):
        self._download_tokens.pop(token, None)
        if entry.cleanup:
            self._remove_local_temp(entry.path)

    @staticmethod
    def _is_media_temp_path(media: BaseMessageElement, path: Path) -> bool:
        temp_path = getattr(media, "_temp_path", None)
        if not temp_path:
            return False
        try:
            return Path(temp_path).resolve() == path.resolve()
        except Exception:
            return str(temp_path) == str(path)

    def _remove_local_temp(self, path: Path):
        try:
            path.unlink(missing_ok=True)
        except Exception as e:
            self._debug(f"Failed to remove temporary file {path}: {e}")

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _media_name(media: BaseMessageElement) -> str:
        name = getattr(media, "name", None)
        if name:
            return os.path.basename(str(name))
        guess_name = getattr(media, "guess_name", None)
        if callable(guess_name):
            guessed = guess_name()
            if guessed:
                return os.path.basename(str(guessed))
        return uuid.uuid4().hex

    @staticmethod
    def _mb(value: int) -> int:
        return int(value) * 1024 * 1024

    @staticmethod
    def _summarize_response(resp: Any) -> str:
        try:
            text = json.dumps(resp, ensure_ascii=False)
        except (TypeError, ValueError):
            text = repr(resp)
        return text[:800]

    @staticmethod
    def _mask_token(value: str) -> str:
        if len(value) <= 10:
            return "***"
        return f"{value[:4]}...{value[-4:]}"

    def _describe_reference(self, file_ref: str) -> str:
        if not file_ref:
            return "empty"
        if file_ref.startswith(("http://", "https://")):
            marker = f"/api/plugin/{PLUGIN_ID}/download/"
            if marker in file_ref:
                token = file_ref.rsplit("/", 1)[-1]
                return f"http-token:{self._mask_token(token)}"
            return "url"
        if file_ref.startswith("base64://"):
            return "base64-blocked"
        if os.path.isabs(file_ref) or "\\" in file_ref or "/" in file_ref:
            return "path"
        return "napcat-ref"

    def _debug_result(self, strategy: str, filename: str, result: KiraIMSentResult, elapsed: float):
        if result.ok:
            self._debug(
                f"{strategy} transport finished name={filename} ok=True "
                f"message_id={result.message_id or ''} elapsed={elapsed:.2f}s"
            )
        else:
            self._debug(
                f"{strategy} transport finished name={filename} ok=False "
                f"err={result.err[:300]} elapsed={elapsed:.2f}s"
            )

    def _debug(self, message: str):
        if self.debug_log:
            logger.info(message)
