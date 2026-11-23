"""
diagnostic_logger.py - Dedicated JSON Lines Diagnostic Logging for StreamVault.

Features:
- Configured via DIAGNOSTICS_ENABLED environment variable (default: False).
- Outputs to /app/logs/diagnostics.jsonl (or local ./logs/diagnostics.jsonl).
- Latest-video-only isolation: tracks active_job_id, atomically truncating previous
  diagnostic logs upon activating a new job, and discarding events from older jobs.
- Thread-safe atomic file writing under threading.Lock.
- Zero file I/O overhead when DIAGNOSTICS_ENABLED=false.
- Strict sanitization: never logs credentials, cookies, tokens, or full private URLs.
"""

import os
import json
import threading
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from urllib.parse import urlparse, parse_qs


def sanitize_url_for_logging(url: Optional[str]) -> str:
    """
    Strips query parameters (except standard video IDs), authentication, tokens,
    cookies, fragments, and credentials.
    """
    if not url or not isinstance(url, str):
        return ""
    try:
        raw = url.strip()
        parsed = urlparse(raw)
        scheme = parsed.scheme.lower() if parsed.scheme else "https"
        netloc = parsed.netloc.lower()
        if not netloc:
            if "/" in raw:
                parsed = urlparse("https://" + raw)
                scheme = "https"
                netloc = parsed.netloc.lower()
            else:
                return "[SANITIZED_URL]"

        if "youtube.com" in netloc and parsed.path == "/watch":
            qs = parse_qs(parsed.query)
            v_val = qs.get("v", [""])[0]
            if v_val:
                return f"{scheme}://{netloc}/watch?v={v_val}"
            return f"{scheme}://{netloc}/watch"

        if "youtu.be" in netloc or ("youtube.com" in netloc and (parsed.path.startswith("/shorts/") or parsed.path.startswith("/embed/"))):
            clean_path = parsed.path.rstrip("/")
            return f"{scheme}://{netloc}{clean_path}"

        clean_path = parsed.path.rstrip("/")
        if clean_path:
            return f"{scheme}://{netloc}{clean_path}"
        return f"{scheme}://{netloc}"
    except Exception:
        return "[SANITIZED_URL]"


class DiagnosticLogger:
    def __init__(self):
        self._lock = threading.Lock()
        self._active_job_id: Optional[str] = None
        self._active_file_id: Optional[str] = None

    def is_enabled(self) -> bool:
        """Returns True only when DIAGNOSTICS_ENABLED is explicitly enabled."""
        return os.getenv("DIAGNOSTICS_ENABLED", "false").lower() in ("true", "1", "yes")

    def get_log_path(self) -> str:
        """Determines the output log path for diagnostics.jsonl."""
        env_path = os.getenv("DIAGNOSTICS_LOG_PATH")
        if env_path:
            return env_path
        if os.path.exists("/app") and os.path.isdir("/app"):
            return "/app/logs/diagnostics.jsonl"
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "diagnostics.jsonl")

    def activate_job(self, job_id: str, url: Optional[str] = None) -> None:
        """
        Atomically replaces the active diagnostic job ID and clears previous logs.
        Records the new submission event right after activating the job.
        """
        if not self.is_enabled():
            return

        with self._lock:
            self._active_job_id = job_id
            self._active_file_id = None

            log_path = self.get_log_path()
            try:
                log_dir = os.path.dirname(log_path)
                if log_dir:
                    os.makedirs(log_dir, exist_ok=True)
                with open(log_path, "w", encoding="utf-8"):
                    pass
            except Exception as e:
                print(f"[DIAGNOSTICS] Warning: Failed to reset diagnostic log at {log_path}: {e}")

        # Record submission event after activating its job ID
        self.log_event(
            event="job_activated",
            job_id=job_id,
            details={"url": sanitize_url_for_logging(url)} if url else {}
        )

    def associate_file_id(self, job_id: str, file_id: str) -> None:
        """Associates file_id with the active job if job_id matches active job."""
        if not self.is_enabled():
            return
        with self._lock:
            if self._active_job_id == job_id:
                self._active_file_id = file_id

    def get_active_job_id(self) -> Optional[str]:
        with self._lock:
            return self._active_job_id

    def get_active_file_id(self) -> Optional[str]:
        with self._lock:
            return self._active_file_id

    def is_active_job(self, job_id: Optional[str]) -> bool:
        if not job_id:
            return False
        with self._lock:
            return self._active_job_id is not None and self._active_job_id == job_id

    def is_active_file(self, file_id: Optional[str]) -> bool:
        if not file_id:
            return False
        with self._lock:
            return self._active_file_id is not None and self._active_file_id == file_id

    def log_event(
        self,
        event: str,
        job_id: Optional[str] = None,
        file_id: Optional[str] = None,
        request_id: Optional[str] = None,
        level: str = "INFO",
        details: Optional[Dict[str, Any]] = None,
        duration_ms: Optional[float] = None,
        error: Optional[str] = None,
        traceback_str: Optional[str] = None,
    ) -> None:
        """
        Records a JSON Lines entry if diagnostics is enabled and the event belongs
        to the currently active diagnostic job. Excludes older concurrent jobs,
        background tasks, and delayed cleanup.
        """
        if not self.is_enabled():
            return

        with self._lock:
            if not self._active_job_id:
                return

            # Check latest-video isolation:
            # Must match active job_id or associated active file_id
            if job_id:
                if job_id != self._active_job_id:
                    return
                effective_job_id = job_id
            elif file_id and self._active_file_id and file_id == self._active_file_id:
                effective_job_id = self._active_job_id
            else:
                return

            effective_file_id = file_id or (self._active_file_id if effective_job_id == self._active_job_id else None)

            record: Dict[str, Any] = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event": event,
                "level": level,
                "job_id": effective_job_id,
            }
            if effective_file_id:
                record["file_id"] = effective_file_id
            if request_id:
                record["request_id"] = request_id
            if duration_ms is not None:
                record["duration_ms"] = round(duration_ms, 2)
            if details:
                safe_details = {}
                for k, v in details.items():
                    if isinstance(v, str) and (v.startswith("http://") or v.startswith("https://")):
                        safe_details[k] = sanitize_url_for_logging(v)
                    else:
                        safe_details[k] = v
                record["details"] = safe_details
            if error:
                record["error"] = error
            if traceback_str:
                record["traceback"] = traceback_str

            log_path = self.get_log_path()
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
            except Exception as write_err:
                print(f"[DIAGNOSTICS] Warning: Failed to write to {log_path}: {write_err}")


# Global singleton instance
logger = DiagnosticLogger()

def is_enabled() -> bool:
    return logger.is_enabled()

def activate_job(job_id: str, url: Optional[str] = None) -> None:
    logger.activate_job(job_id, url)

def associate_file_id(job_id: str, file_id: str) -> None:
    logger.associate_file_id(job_id, file_id)

def get_active_job_id() -> Optional[str]:
    return logger.get_active_job_id()

def get_active_file_id() -> Optional[str]:
    return logger.get_active_file_id()

def is_active_job(job_id: Optional[str]) -> bool:
    return logger.is_active_job(job_id)

def is_active_file(file_id: Optional[str]) -> bool:
    return logger.is_active_file(file_id)

def log_event(*args, **kwargs) -> None:
    logger.log_event(*args, **kwargs)
