### --- SECTION 1: IMPORTS & ENVIRONMENT SETUP --- ###
import os
import sys
import stat
import uuid
import yt_dlp
import re
import json
import asyncio
import hmac
import time
import socket
import ipaddress
import mimetypes
import threading
import unicodedata
import shutil
import typing
import diagnostic_timing
import diagnostic_logger
import job_manager
import traceback
import subprocess
import errno
from contextlib import asynccontextmanager
from urllib.parse import urlparse, parse_qs, quote
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles


def load_env_file(filepath=".env"):
    if not os.path.isfile(filepath):
        raise RuntimeError(f"Environment file not found: {filepath}")

    with open(filepath, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()

            if value.startswith(("'", '"')) and value.endswith(("'", '"')):
                value = value[1:-1]

            if key:
                os.environ.setdefault(key, value)

load_env_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))


### --- SECTION 2: ORIGIN NORMALIZATION --- ###
def normalize_origin(url_str: str, strict_startup: bool = False) -> str:
    if not url_str or not str(url_str).strip():
        raise ValueError("URL string is empty")

    url_str = str(url_str).strip()
    if re.search(r'\s', url_str):
        raise ValueError("URL contains invalid whitespace")

    parsed = urlparse(url_str)
    if parsed.scheme not in ("http", "https"):
        raise ValueError("Scheme must be http or https")

    if not parsed.hostname:
        raise ValueError("Missing or invalid hostname")

    if parsed.netloc.endswith(":"):
        raise ValueError("Empty port specified")

    try:
        port = parsed.port
    except ValueError:
        raise ValueError("Invalid port number")

    if strict_startup:
        if parsed.path not in ("", "/") or parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise ValueError("Origin must contain only scheme, hostname, and optional port")

    scheme = parsed.scheme.lower()
    hostname = parsed.hostname.lower()

    if ":" in hostname:
        try:
            ipaddress.IPv6Address(hostname)
        except ValueError:
            raise ValueError("Invalid IPv6 address")
        hostname = f"[{hostname}]"
    else:
        try:
            ipaddress.IPv4Address(hostname)
        except ValueError:
            if re.match(r'^[\d.]+$', hostname):
                raise ValueError("Invalid IPv4 address format")

            if len(hostname) > 253:
                raise ValueError("Hostname exceeds 253 characters")

            labels = hostname.split(".")

            for label in labels:
                if not (1 <= len(label) <= 63):
                    raise ValueError("DNS label length must be between 1 and 63 characters")
                if not re.match(r'^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$', label):
                    raise ValueError("Invalid DNS label format")

    if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        port = None

    if port:
        return f"{scheme}://{hostname}:{port}"
    return f"{scheme}://{hostname}"


### --- SECTION 3: STARTUP CONFIGURATION & STATE --- ###
AUTH_USERNAME = os.getenv("AUTH_USERNAME")
AUTH_PASSWORD = os.getenv("AUTH_PASSWORD")
SESSION_SECRET = os.getenv("SESSION_SECRET")
CANONICAL_ORIGIN_RAW = os.getenv("CANONICAL_ORIGIN")
TRUST_REVERSE_PROXY = os.getenv("TRUST_REVERSE_PROXY", "false").lower() == "true"
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "true").lower() == "true"
USE_X_ACCEL_REDIRECT = os.getenv("USE_X_ACCEL_REDIRECT", "false").lower() == "true"
NGINX_LOG_SOCKET_GROUP = os.getenv("NGINX_LOG_SOCKET_GROUP")

TRUSTED_PROXIES_RAW = [p.strip() for p in os.getenv("TRUSTED_PROXIES", "127.0.0.1,::1").split(",")]
TRUSTED_PROXIES = []
if TRUST_REVERSE_PROXY:
    for p in TRUSTED_PROXIES_RAW:
        if not p:
            continue
        try:
            # Normalize IPs at startup
            TRUSTED_PROXIES.append(str(ipaddress.ip_address(p)))
        except ValueError:
            raise RuntimeError(f"CRITICAL STARTUP ERROR: Invalid trusted proxy IP configuration: {p}")

if not AUTH_USERNAME or not AUTH_PASSWORD:
    raise RuntimeError("CRITICAL STARTUP ERROR: AUTH_USERNAME and AUTH_PASSWORD must be defined in .env")
if not SESSION_SECRET or len(SESSION_SECRET) < 32:
    raise RuntimeError("CRITICAL STARTUP ERROR: SESSION_SECRET must be at least 32 characters long for cryptographic security.")
if not CANONICAL_ORIGIN_RAW:
    raise RuntimeError("CRITICAL STARTUP ERROR: CANONICAL_ORIGIN must be defined in .env")

try:
    CANONICAL_ORIGIN = normalize_origin(
        CANONICAL_ORIGIN_RAW,
        strict_startup=True
    )
except ValueError as e:
    raise RuntimeError(
        f"CRITICAL STARTUP ERROR: CANONICAL_ORIGIN configuration is invalid. {e}"
    )

ALLOWED_ORIGINS_RAW = os.getenv(
    "ALLOWED_ORIGINS",
    CANONICAL_ORIGIN_RAW
)

try:
    ALLOWED_ORIGINS = {
        normalize_origin(origin.strip(), strict_startup=True)
        for origin in ALLOWED_ORIGINS_RAW.split(",")
        if origin.strip()
    }

    if not ALLOWED_ORIGINS:
        raise ValueError("At least one allowed origin is required")

except ValueError as e:
    raise RuntimeError(
        f"Invalid ALLOWED_ORIGINS configuration: {e}"
    )
SESSION_EXPIRE_SECONDS = 2592000

active_sessions = {}
active_file_map = {}
active_downloads = {}
active_download_tasks: set[asyncio.Task] = set()
format_cache = {}
pending_disconnect_cleanups = {}

MAX_CONCURRENT_DOWNLOADS = 3
MAX_CONCURRENT_EXTRACTIONS = 5
UNCLAIMED_FILE_EXPIRY = 3600
DISCONNECT_CLEANUP_GRACE_SECONDS = 60
X_ACCEL_QUIET_PERIOD_SECONDS = 5
NGINX_MAX_DELIVERY_RECOVERY_SECONDS = 1800  # Conservative 30-min window for safe recovery of lost datagrams
TMPFS_THRESHOLD_BYTES = 200 * 1024 * 1024  # 200 MB
VALID_VIDEO_EXTENSIONS = {".mp4", ".webm", ".mkv", ".mov", ".m4v", ".avi"}

current_dir = os.path.dirname(os.path.abspath(__file__))
frontend_dir = os.path.join(current_dir, "frontend")
TEMP_DOWNLOAD_DIR = os.path.join(current_dir, "temp_downloads")
TMPFS_DOWNLOAD_DIR = os.path.join(current_dir, "temp_downloads_tmpfs")
RUN_DIR = os.path.join(current_dir, "run")
os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
os.makedirs(TMPFS_DOWNLOAD_DIR, exist_ok=True)
os.makedirs(RUN_DIR, exist_ok=True)

# Concurrency synchronization primitives (single-process asyncio event loop)
state_lock = asyncio.Lock()
extraction_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTIONS)


### --- SECTION 4: STORAGE TIERING & NGINX TIMEOUT HANDLING --- ###
async def migrate_tmpfs_to_disk(file_id: str, rec: dict) -> bool:
    """
    Migrates a file from tmpfs (TMPFS_DOWNLOAD_DIR) to persistent storage (TEMP_DOWNLOAD_DIR).
    Requirements:
    - Never migrate while another legitimate delivery is active.
    - Update active_file_map path and storage_type consistently after migration.
    """
    if rec.get("storage_type") != "tmpfs":
        return False

    if len(rec.get("pending_requests", set())) > 0 or rec.get("active_streams", 0) > 0:
        return False

    if rec.get("is_migrating"):
        return False
    rec["is_migrating"] = True

    src_path = rec.get("filepath")
    if not src_path or not os.path.exists(src_path):
        rec["is_migrating"] = False
        return False

    filename = os.path.basename(src_path)
    dest_path = os.path.join(TEMP_DOWNLOAD_DIR, filename)

    if os.path.abspath(src_path) == os.path.abspath(dest_path):
        rec["storage_type"] = "disk"
        rec["is_migrating"] = False
        return False

    try:
        loop = asyncio.get_running_loop()

        def _move():
            if os.path.exists(dest_path) and os.path.abspath(dest_path) != os.path.abspath(src_path):
                try:
                    os.remove(dest_path)
                except Exception:
                    pass
            shutil.move(src_path, dest_path)

        await loop.run_in_executor(None, _move)

        rec["filepath"] = dest_path
        rec["storage_type"] = "disk"
        print(f"[STORAGE_TIER] Migrated {file_id} from tmpfs to disk: {dest_path}")
        if diagnostic_logger.is_enabled():
            diagnostic_logger.log_event(
                "storage_tier_migrated",
                job_id=rec.get("job_id"),
                file_id=file_id,
                details={
                    "from_storage": "tmpfs",
                    "to_storage": "disk",
                    "filepath": dest_path
                }
            )
        return True
    except Exception as e:
        print(f"[STORAGE_TIER] Warning: Failed to migrate {file_id} to disk: {e}")
        return False
    finally:
        rec.pop("is_migrating", None)


async def expire_stale_nginx_request(file_id: str, request_id: str, trigger: str = "recovery"):
    """
    Safely expires an unacknowledged Nginx request only when transfer termination
    has been reliably established (e.g. after conservative maximum delivery window).
    Updates active_streams and pending_requests counters exactly once, and migrates
    from tmpfs to persistent storage once no legitimate delivery remains active.
    """
    async with state_lock:
        rec = active_file_map.get(file_id)
        if not rec:
            return
        pending_set = rec.get("pending_requests", set())
        if request_id not in pending_set:
            return

        pending_set.discard(request_id)
        rec.get("pending_request_times", {}).pop(request_id, None)

        rec["active_streams"] = max(0, rec.get("active_streams", 1) - 1)
        remaining_requests = len(pending_set)
        active_streams = rec.get("active_streams", 0)

        print(f"[NGINX_LOG] Stale request safely expired ({trigger}): file_id={file_id}, request_id={request_id} (remaining pending: {remaining_requests}, active_streams: {active_streams})")
        if diagnostic_logger.is_enabled():
            diagnostic_logger.log_event(
                "nginx_request_timeout",
                job_id=rec.get("job_id"),
                file_id=file_id,
                details={
                    "request_id": request_id,
                    "trigger": trigger,
                    "remaining_pending": remaining_requests,
                    "active_streams": active_streams
                }
            )

        if remaining_requests == 0 and active_streams == 0:
            if rec.get("storage_type") == "tmpfs":
                await migrate_tmpfs_to_disk(file_id, rec)


### --- SECTION 5: PERIODIC MAINTENANCE TASKS --- ###
async def run_maintenance_tasks():
    now = time.time()
    files_to_clean = []
    jobs_to_expire = []

    async with state_lock:
        expired_sessions = [sid for sid, exp in active_sessions.items() if now > exp]
        for sid in expired_sessions:
            active_sessions.pop(sid, None)

        for fid, rec in list(active_file_map.items()):
            active_streams = rec.get("active_streams", 0)
            pending_requests = len(rec.get("pending_requests", set()))
            is_expired = now > rec.get("expires", 0)
            fpath = rec.get("filepath")
            file_size = rec.get("size", 0)
            is_missing = not fpath or not os.path.exists(fpath)

            # Safe recovery for unacknowledged Nginx requests only when transfer termination
            # can be established reliably:
            # 1. Conservative maximum delivery duration based on file size (~50 KB/s minimum speed)
            # 2. Or when the asset has reached full expiration (is_expired)
            req_times = rec.get("pending_request_times", {})
            max_safe_duration = max(NGINX_MAX_DELIVERY_RECOVERY_SECONDS, int(file_size / (50 * 1024)))
            stale_rids = [
                rid for rid, t in list(req_times.items())
                if is_expired or (now - t) >= max_safe_duration
            ]
            for srid in stale_rids:
                rec.get("pending_requests", set()).discard(srid)
                req_times.pop(srid, None)
                rec["active_streams"] = max(0, rec.get("active_streams", 1) - 1)
                print(f"[MAINTENANCE] Safely recovered unacknowledged request {srid} for {fid}")

            active_streams = rec.get("active_streams", 0)
            pending_requests = len(rec.get("pending_requests", set()))

            # Tmpfs lifecycle management: if no delivery is active, migrate to persistent storage
            # instead of retaining in tmpfs for 3600 seconds
            if rec.get("storage_type") == "tmpfs" and active_streams == 0 and pending_requests == 0:
                await migrate_tmpfs_to_disk(fid, rec)
                fpath = rec.get("filepath")

            if is_expired or is_missing:
                # Active delivery must never be invalidated by maintenance
                if active_streams > 0 or pending_requests > 0:
                    continue
                active_file_map.pop(fid, None)
                if fpath:
                    files_to_clean.append(fpath)
                jobs_to_expire.append((rec.get("job_id"), fid))

        expired_formats = [u for u, rec in format_cache.items() if now > rec["expires"]]
        for u in expired_formats:
            format_cache.pop(u, None)

        if len(format_cache) > 500:
            oldest_formats = sorted(format_cache.keys(), key=lambda k: format_cache[k]["expires"])
            while len(format_cache) > 500 and oldest_formats:
                format_cache.pop(oldest_formats.pop(0), None)

        # Inspect actual tmpfs directory for unregistered / genuine orphaned files
        if os.path.exists(TMPFS_DOWNLOAD_DIR):
            try:
                # Set of registered files and identifiers currently tracked in active_file_map
                tracked_files = {
                    os.path.abspath(r["filepath"])
                    for r in active_file_map.values()
                    if r.get("filepath")
                }
                tracked_fids = set(active_file_map.keys())
                active_uuids = set(active_downloads.keys())

                for fname in os.listdir(TMPFS_DOWNLOAD_DIR):
                    tmpfs_fpath = os.path.join(TMPFS_DOWNLOAD_DIR, fname)
                    abs_tmpfs_path = os.path.abspath(tmpfs_fpath)

                    if not os.path.isfile(tmpfs_fpath):
                        continue

                    # Never remove files associated with registered deliveries in active_file_map
                    if abs_tmpfs_path in tracked_files or any(fname.startswith(fid) for fid in tracked_fids):
                        continue

                    # Never remove files associated with active in-progress downloads
                    if any(fname.startswith(u) for u in active_uuids):
                        continue

                    # Never remove files created very recently (within 60s) to protect in-flight moves and registrations
                    try:
                        mtime = os.path.getmtime(tmpfs_fpath)
                        if (now - mtime) < 60:
                            continue
                    except OSError:
                        continue

                    # Resolve file_id candidate from filename if available for diagnostic correlation
                    base_name = os.path.splitext(fname)[0]
                    first_part = base_name.split(".")[0]
                    orphan_fid = base_name if len(base_name) == 36 else (first_part if len(first_part) == 36 else None)

                    # Safely remove genuine orphaned tmpfs files to prevent RAM disk starvation
                    print(f"[MAINTENANCE] Removing genuine orphaned tmpfs file: {tmpfs_fpath}")
                    cleanup_file(tmpfs_fpath, trigger="orphan_tmpfs_cleanup", file_id=orphan_fid)
            except Exception as tmpfs_err:
                print(f"Warning: Error inspecting tmpfs directory for orphans: {tmpfs_err}")
                if diagnostic_logger.is_enabled():
                    diagnostic_logger.log_event(
                        "tmpfs_orphan_scan_error",
                        level="ERROR",
                        details={"error": str(tmpfs_err)},
                        error=str(tmpfs_err)
                    )

    for filepath in files_to_clean:
        cleanup_file(filepath, trigger="maintenance_expiry")

    for jid, fid in jobs_to_expire:
        try:
            job_manager.expire_job_asset(job_id=jid, file_id=fid)
            if diagnostic_logger.is_enabled():
                diagnostic_logger.log_event(
                    "maintenance_asset_expired",
                    job_id=jid,
                    file_id=fid,
                    details={"job_id": jid, "file_id": fid}
                )
        except Exception as expire_err:
            print(f"Warning: Failed to expire mobile job {jid or fid}: {expire_err}")

    # Prune expired mobile jobs
    try:
        await asyncio.to_thread(job_manager.delete_expired_jobs)
    except Exception as jm_err:
        print(f"Warning: Error pruning expired mobile jobs: {jm_err}")


async def periodic_maintenance_loop():
    try:
        while True:
            await asyncio.sleep(60)
            await run_maintenance_tasks()
    except asyncio.CancelledError:
        pass


### --- SECTION 6: AUTHENTICATION, CSRF & SECURITY VALIDATION --- ###
async def validate_url_safety(url_str: str):
    """
    Validates URL syntax and performs an initial DNS resolution check.

    SECURITY LIMITATIONS:
    1. DNS Rebinding: This checks the IP once. yt-dlp will resolve it again.
       An attacker can change the DNS record between checks.
    2. Tarpits: Python's getaddrinfo relies on the OS-level DNS timeout.
       Malicious DNS servers can tie up executor threads until the OS times out,
       potentially exhausting the thread pool. There is no guaranteed application-level timeout.
    3. Redirects: Secondary requests and redirects are not validated by this function.

    Robust SSRF protection requires network-level egress firewall rules restricting
    outbound connections to internal/private IP ranges.
    """
    if not url_str:
        raise ValueError("URL must be provided")
    if len(url_str) > 2048:
        raise ValueError("URL exceeds maximum allowed length")
    if not url_str.lower().startswith(("http://", "https://")):
        raise ValueError("URL must begin with http:// or https://")
    if re.search(r'[\s\x00-\x1f\x7f]', url_str):
        raise ValueError("URL contains invalid whitespace or control characters")

    try:
        parsed = urlparse(url_str)
        scheme = parsed.scheme.lower()
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as e:
        raise ValueError(f"Malformed URL structure: {e}")

    if parsed.username or parsed.password:
        raise ValueError("Embedded credentials in URL are not allowed")

    if scheme == "http" and port not in (None, 80):
        raise ValueError("HTTP must use standard port 80")
    if scheme == "https" and port not in (None, 443):
        raise ValueError("HTTPS must use standard port 443")

    if not hostname:
        raise ValueError("URL is missing a valid hostname")

    loop = asyncio.get_running_loop()
    try:
        # Pushes DNS resolution to the default executor pool.
        # Bounded only by the OS-level resolver timeout configuration.
        addrs = await loop.getaddrinfo(hostname, None)

        if not addrs:
            raise ValueError("Hostname did not resolve to any IP addresses")

        for addr in addrs:
            ip_str = addr[4][0]
            ip = ipaddress.ip_address(ip_str)
            if not getattr(ip, "is_global", False) or ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved or ip.is_unspecified:
                raise ValueError(f"URL resolves to a restricted or non-global network range: {ip_str}")
    except socket.gaierror:
        raise ValueError("Could not resolve hostname via DNS")
    except OSError as e:
        raise ValueError(f"OS error during DNS resolution: {e}")
    except ValueError as e:
        raise ValueError(str(e))

def validate_uuid(id_str: str):
    try:
        uuid.UUID(id_str)
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(status_code=400, detail="Invalid resource identifier format")

def get_client_ip(request: Request) -> str:
    ip = request.client.host if request.client else "unknown"
    if TRUST_REVERSE_PROXY:
        try:
            norm_ip = str(ipaddress.ip_address(ip))
        except ValueError:
            norm_ip = ip

        if norm_ip in TRUSTED_PROXIES:
            forwarded = request.headers.get("x-forwarded-for")
            if forwarded:
                proxies = [p.strip() for p in forwarded.split(",")]
                for p in reversed(proxies):
                    try:
                        parsed_p = str(ipaddress.ip_address(p))
                        if parsed_p not in TRUSTED_PROXIES:
                            ip = parsed_p
                            break
                    except ValueError:
                        continue
    return ip

def verify_csrf(request: Request):
    if request.method in ["POST", "PUT", "DELETE", "PATCH"]:
        origin = request.headers.get("origin")
        referer = request.headers.get("referer")

        if origin:
            try:
                norm_origin = normalize_origin(origin, strict_startup=False)
            except ValueError:
                raise HTTPException(status_code=403, detail="CSRF validation failed: Malformed Origin")
            if norm_origin not in ALLOWED_ORIGINS:
                raise HTTPException(status_code=403, detail="CSRF validation failed: Origin mismatch")
        elif referer:
            try:
                norm_referer = normalize_origin(referer, strict_startup=False)
            except ValueError:
                raise HTTPException(status_code=403, detail="CSRF validation failed: Malformed Referer")
            if norm_referer not in ALLOWED_ORIGINS:
                raise HTTPException(status_code=403, detail="CSRF validation failed: Referer mismatch")
        else:
            raise HTTPException(status_code=403, detail="CSRF validation failed: Missing Origin and Referer headers")

def verify_session(request: Request):
    token = request.cookies.get("streamvault_session")
    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        parts = token.split(":")
        if len(parts) != 4:
            raise HTTPException(status_code=401, detail="Invalid session format")

        username, session_id, timestamp_str, sig = parts
        timestamp = int(timestamp_str)
        now = time.time()

        if timestamp > now:
            raise HTTPException(status_code=401, detail="Invalid session (future-dated)")

        expected_sig = hmac.new(SESSION_SECRET.encode(), f"{username}:{session_id}:{timestamp_str}".encode(), digestmod="sha256").hexdigest()
        if not hmac.compare_digest(sig, expected_sig):
            raise HTTPException(status_code=401, detail="Invalid session signature")

        server_expiry = active_sessions.get(session_id)
        if not server_expiry or now > server_expiry:
            active_sessions.pop(session_id, None)
            raise HTTPException(status_code=401, detail="Session expired or revoked")

        return username
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Authentication failed")

class DownloadCancelled(Exception):
    pass


### --- SECTION 7: RUNTIME HARDENING & YOUTUBEDL WRAPPER --- ###
_thread_local = threading.local()

def _is_safe_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
        return getattr(ip, "is_global", False) and not (
            ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved or ip.is_unspecified
        )
    except ValueError:
        return False

_orig_socket_connect = socket.socket.connect
_orig_socket_connect_ex = socket.socket.connect_ex

def _safe_socket_connect(self, address):
    if getattr(_thread_local, "enforce_ssrf_protection", False):
        if isinstance(address, tuple) and len(address) >= 2 and isinstance(address[0], str):
            ip_str = address[0]
            if not _is_safe_ip(ip_str):
                raise PermissionError(f"SSRF blocked: connection to non-global destination '{ip_str}' denied")
    return _orig_socket_connect(self, address)

def _safe_socket_connect_ex(self, address):
    if getattr(_thread_local, "enforce_ssrf_protection", False):
        if isinstance(address, tuple) and len(address) >= 2 and isinstance(address[0], str):
            ip_str = address[0]
            if not _is_safe_ip(ip_str):
                return errno.EPERM
    return _orig_socket_connect_ex(self, address)

socket.socket.connect = _safe_socket_connect
socket.socket.connect_ex = _safe_socket_connect_ex


class SubprocessTracker:
    def __init__(self):
        self.procs = set()
        self.lock = threading.Lock()
        self.cancelled = False

    def add(self, p):
        with self.lock:
            if self.cancelled:
                try:
                    p.kill()
                except OSError:
                    pass
                return
            self.procs.add(p)

    def discard(self, p):
        with self.lock:
            self.procs.discard(p)

    def terminate_all(self):
        with self.lock:
            self.cancelled = True
            for p in list(self.procs):
                try:
                    p.kill()
                except OSError:
                    pass
            self.procs.clear()

_orig_popen_init = subprocess.Popen.__init__

def _tracking_popen_init(self, *args, **kwargs):
    _orig_popen_init(self, *args, **kwargs)
    tracker = getattr(_thread_local, "subprocess_tracker", None)
    if tracker is not None:
        tracker.add(self)

subprocess.Popen.__init__ = _tracking_popen_init


class ReadOnlyYoutubeDL(yt_dlp.YoutubeDL):
    """
    Custom YoutubeDL wrapper that:
    1. Suppresses automatic cookie-jar serialization to disk (cookies.txt remains strictly read-only).
    2. Enforces socket-level SSRF and DNS-rebinding protection during all extraction and download network activity.
    """
    def save_cookies(self):
        pass

    def __enter__(self):
        _thread_local.enforce_ssrf_protection = True
        return super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        _thread_local.enforce_ssrf_protection = False
        return super().__exit__(exc_type, exc_val, exc_tb)


def cleanup_file(filepath: str, trigger: typing.Optional[str] = None, job_id: typing.Optional[str] = None, file_id: typing.Optional[str] = None):
    if not filepath:
        return
    exists_before = os.path.exists(filepath)
    file_size = 0
    if exists_before:
        try:
            file_size = os.path.getsize(filepath)
        except Exception:
            pass
    success = False
    err_str = None
    if exists_before:
        try:
            try:
                os.remove(filepath)
            except PermissionError:
                os.chmod(filepath, stat.S_IWRITE | stat.S_IREAD)
                os.remove(filepath)
            success = True
        except Exception as e:
            err_str = str(e)

    if not file_id:
        base = os.path.splitext(os.path.basename(filepath))[0]
        first_part = base.split(".")[0]
        if len(base) == 36:
            file_id = base
        elif len(first_part) == 36:
            file_id = first_part

    if diagnostic_logger.is_enabled():
        diagnostic_logger.log_event(
            "file_cleanup",
            job_id=job_id,
            file_id=file_id,
            level="INFO" if success else "ERROR",
            details={
                "filepath": filepath,
                "trigger": trigger or "unspecified",
                "file_size": file_size,
                "existed": exists_before,
                "deleted": success,
                "error": err_str
            },
            error=err_str
        )

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", name)

def clean_media_title(info: dict) -> str:
    """
    Platform-aware video title resolver.
    Extracts and cleans video titles for display, download filenames, and history:
    - YouTube: Preserves info['title'] exactly as returned by yt-dlp.
    - Instagram: If title is missing or matches generic patterns ('Video by ...', 'Post by ...'),
      uses the first meaningful line of description when available; preserves genuine titles.
    - Facebook: Confidently detects and removes leading engagement statistics (views, reactions,
      shares, comments) while preserving the actual video title and internal punctuation.
    - Other platforms / Fallback: Preserves the title, or falls back to 'Video'.
    """
    if not isinstance(info, dict):
        return "Video"

    raw_title = (info.get("title") or "").strip()
    extractor = (info.get("extractor") or info.get("extractor_key") or "").lower()
    webpage_url = (info.get("webpage_url") or info.get("original_url") or info.get("url") or "").lower()

    # 1. YouTube handling: strictly preserve existing title behaviour unchanged
    if "youtube" in extractor or "youtube.com" in webpage_url or "youtu.be" in webpage_url:
        return raw_title if raw_title else "video"

    # 2. Instagram handling
    is_instagram = "instagram" in extractor or "instagram.com" in webpage_url
    generic_ig_pattern = r'^(?:Video|Post|Story)\s+(?:by\s+.+|\d+)$'
    if is_instagram or re.match(r'^(?:Video|Post|Story)\s+by\s+', raw_title, re.IGNORECASE):
        is_generic = not raw_title or bool(re.match(generic_ig_pattern, raw_title, re.IGNORECASE))
        if is_generic:
            description = info.get("description") or ""
            # Prefer the first meaningful line of the description
            meaningful_lines = [line.strip() for line in description.splitlines() if line.strip()]
            if meaningful_lines:
                return meaningful_lines[0]
            return raw_title if raw_title else "Video"
        return raw_title

    # 3. Facebook handling
    is_facebook = "facebook" in extractor or "facebook.com" in webpage_url or "fb.watch" in webpage_url
    if is_facebook or ("|" in raw_title and re.search(r'\b(?:views?|reactions?|shares?|comments?)\b', raw_title, re.IGNORECASE)):
        if "|" in raw_title:
            prefix, rest = raw_title.split("|", 1)
            single_metric = r'\d+(?:[.,]\d+)?\s*[kKmMbB]?\s+(?:views?|reactions?|shares?|comments?|likes?)'
            sep = r'\s*[·•,\-]\s*'
            engagement_prefix_pattern = rf'^\s*(?:{single_metric}(?:{sep}|\s+)*)+\s*$'
            if re.match(engagement_prefix_pattern, prefix, re.IGNORECASE) and rest.strip():
                return rest.strip()
        return raw_title if raw_title else "Video"

    # 4. Fallback for other platforms
    return raw_title if raw_title else "Video"

def clean_terminal_ansi(text: str) -> str:
    return re.sub(r'\x1b\[[0-9;]*m', '', str(text))


def add_delivered_range(delivered_ranges: list, start: int, end: int) -> list:
    """
    Merges a newly delivered byte range [start, end) into an existing list of
    disjoint sorted half-open intervals [[s0, e0), [s1, e1), ...].
    """
    if start >= end:
        return delivered_ranges

    new_ranges = []
    inserted = False

    for cur_s, cur_e in delivered_ranges:
        if inserted:
            new_ranges.append((cur_s, cur_e))
        elif end < cur_s:
            new_ranges.append((start, end))
            new_ranges.append((cur_s, cur_e))
            inserted = True
        elif start > cur_e:
            new_ranges.append((cur_s, cur_e))
        else:
            # Overlapping or contiguous intervals: merge
            start = min(start, cur_s)
            end = max(end, cur_e)

    if not inserted:
        new_ranges.append((start, end))

    return new_ranges

def is_fully_covered(delivered_ranges: list, total_size: int) -> bool:
    """Checks if the delivered intervals completely cover [0, total_size)."""
    if total_size <= 0:
        return False
    if len(delivered_ranges) == 1:
        return delivered_ranges[0][0] == 0 and delivered_ranges[0][1] >= total_size
    return False

### --- SECTION 8: NGINX PROTOCOL & APPLICATION LIFESPAN --- ###
### --- 8.1: NGINX LOG DATAGRAM LISTENER --- ###
class NginxLogDatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        super().__init__()
        self.active_tasks: set[asyncio.Task] = set()

    def _task_done_callback(self, task: asyncio.Task):
        self.active_tasks.discard(task)
        if task.cancelled():
            return
        try:
            exc = task.exception()
            if exc:
                print(f"[NGINX_LOG] Completion handler task raised an exception: {exc}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[NGINX_LOG] Error in completion handler done callback: {e}")

    def datagram_received(self, data: bytes, addr):
        try:
            raw_text = data.decode("utf-8", errors="ignore")
            json_start = raw_text.find("{")
            json_end = raw_text.rfind("}")
            if json_start == -1 or json_end == -1:
                return
            payload = json.loads(raw_text[json_start:json_end + 1])

            file_id = payload.get("file_id")
            request_id = payload.get("request_id")
            status = payload.get("status")
            bytes_sent = payload.get("bytes_sent", 0)
            content_length = str(payload.get("content_length", "0"))
            content_range = payload.get("content_range")

            if not file_id or not request_id:
                return

            task = asyncio.create_task(
                self.handle_completion(
                    file_id=file_id,
                    request_id=request_id,
                    status=status,
                    bytes_sent=bytes_sent,
                    content_length=content_length,
                    content_range=content_range
                )
            )
            self.active_tasks.add(task)
            task.add_done_callback(self._task_done_callback)
        except Exception as e:
            print(f"[NGINX_LOG] Error parsing datagram: {e}")

    async def handle_completion(
        self,
        file_id: str,
        request_id: str,
        status: int,
        bytes_sent: int,
        content_length: str,
        content_range: typing.Optional[str]
    ):
        async with state_lock:
            rec = active_file_map.get(file_id)
            if not rec:
                print(f"[NGINX_LOG] Event ignored: file_id={file_id} not found in active_file_map (request_id={request_id})")
                return

            pending_set = rec.get("pending_requests", set())
            if request_id not in pending_set:
                print(f"[NGINX_LOG] Event ignored: request_id={request_id} not in pending_requests for file_id={file_id} (pending: {len(pending_set)})")
                return

            # Always remove the completed/evaluated request ID
            pending_set.discard(request_id)
            rec.get("pending_request_times", {}).pop(request_id, None)

            # Decrement active_streams exactly once
            rec["active_streams"] = max(0, rec.get("active_streams", 1) - 1)
            print(f"[NGINX_LOG] Request removed: file_id={file_id}, request_id={request_id} (remaining pending: {len(pending_set)}, active_streams: {rec.get('active_streams', 0)})")

            file_size = rec.get("size", 0)
            delivered_ranges = rec.setdefault("delivered_ranges", [])

            delivered_interval = None
            rejection_reason = None

            if status == 200:
                # HTTP 200 response: require content_length == file_size and accept 0 < bytes_sent <= file_size
                if file_size <= 0:
                    rejection_reason = f"invalid recorded file_size ({file_size})"
                elif not content_length.isdigit():
                    rejection_reason = f"non-numeric content_length '{content_length}'"
                elif int(content_length) != file_size:
                    rejection_reason = f"content_length ({content_length}) != file_size ({file_size})"
                elif bytes_sent <= 0:
                    rejection_reason = f"bytes_sent ({bytes_sent}) <= 0"
                elif bytes_sent > file_size:
                    rejection_reason = f"bytes_sent ({bytes_sent}) exceeds file_size ({file_size})"
                else:
                    delivered_interval = (0, bytes_sent)

            elif status == 206:
                # Partial response / Range event: parse and validate Content-Range header
                clean_range = str(content_range).strip() if content_range else None

                if not clean_range or clean_range == "-":
                    rejection_reason = "missing or empty Content-Range header"
                elif file_size <= 0:
                    rejection_reason = f"invalid recorded file_size ({file_size})"
                else:
                    match = re.match(r"^bytes\s+(\d+)-(\d+)/(?:\*|(\d+))$", clean_range)
                    if not match:
                        rejection_reason = f"malformed Content-Range syntax '{clean_range}'"
                    else:
                        range_start = int(match.group(1))
                        range_end = int(match.group(2))
                        instance_length = int(match.group(3)) if match.group(3) else file_size
                        expected_length = range_end - range_start + 1

                        if range_start > range_end or expected_length <= 0:
                            rejection_reason = f"invalid range bounds: start ({range_start}) > end ({range_end})"
                        elif range_start < 0 or range_end >= file_size:
                            rejection_reason = f"range bounds [{range_start}, {range_end}] exceed file bounds [0, {file_size - 1}]"
                        elif instance_length != file_size:
                            rejection_reason = f"instance_length ({instance_length}) != file_size ({file_size})"
                        elif not content_length.isdigit():
                            rejection_reason = f"non-numeric content_length '{content_length}'"
                        elif int(content_length) != expected_length:
                            rejection_reason = f"content_length ({content_length}) != expected_length ({expected_length})"
                        elif bytes_sent <= 0:
                            rejection_reason = f"bytes_sent ({bytes_sent}) <= 0"
                        elif bytes_sent > expected_length:
                            rejection_reason = f"bytes_sent ({bytes_sent}) exceeds expected_length ({expected_length})"
                        else:
                            delivered_interval = (range_start, range_start + bytes_sent)
            else:
                rejection_reason = f"unsupported or non-delivery HTTP status {status}"

            # Log concise diagnostics for EVERY completion event
            if delivered_interval:
                print(
                    f"[NGINX_LOG] Event accepted: request_id={request_id}, status={status}, "
                    f"bytes_sent={bytes_sent}, content_length={content_length}, "
                    f"content_range={content_range}, file_size={file_size} -> "
                    f"accepted interval: {delivered_interval} for file_id={file_id}"
                )
            else:
                print(
                    f"[NGINX_LOG] Event rejected: request_id={request_id}, status={status}, "
                    f"bytes_sent={bytes_sent}, content_length={content_length}, "
                    f"content_range={content_range}, file_size={file_size} -> "
                    f"reason: {rejection_reason} for file_id={file_id}"
                )

            if diagnostic_logger.is_enabled():
                diagnostic_logger.log_event(
                    "nginx_datagram_received",
                    job_id=rec.get("job_id"),
                    file_id=file_id,
                    details={
                        "request_id": request_id,
                        "status": status,
                        "bytes_sent": bytes_sent,
                        "content_length": content_length,
                        "content_range": content_range,
                        "delivered_interval": delivered_interval,
                        "rejection_reason": rejection_reason,
                        "file_size": file_size
                    }
                )

            if delivered_interval:
                rec["delivered_ranges"] = add_delivered_range(
                    delivered_ranges,
                    delivered_interval[0],
                    delivered_interval[1]
                )

            remaining_requests = len(pending_set)
            fully_covered = is_fully_covered(rec.get("delivered_ranges", []), file_size)

            if fully_covered and remaining_requests == 0:
                jid = rec.get("job_id")
                if jid:
                    try:
                        job_manager.update_job(jid, status="completed")
                    except Exception as jm_err:
                        print(f"[NGINX_LOG] Warning: Failed to update job {jid} to completed: {jm_err}")
                    diagnostic_timing.tracker.record_delivery_end(jid, bytes_delivered=file_size)

                print(f"[NGINX_LOG] Scheduling 5s quiet period before deletion for file_id={file_id}")
                if diagnostic_logger.is_enabled():
                    diagnostic_logger.log_event(
                        "nginx_quiet_period_scheduled",
                        job_id=jid,
                        file_id=file_id,
                        details={
                            "file_id": file_id,
                            "quiet_period_seconds": X_ACCEL_QUIET_PERIOD_SECONDS,
                            "delivered_ranges": rec.get("delivered_ranges", []),
                            "file_size": file_size
                        }
                    )
                record_id = id(rec)
                filepath = rec["filepath"]

                async def delayed_quiet_period_cleanup():
                    try:
                        await asyncio.sleep(X_ACCEL_QUIET_PERIOD_SECONDS)
                        async with state_lock:
                            current_rec = active_file_map.get(file_id)
                            # Re-verify record identity, zero pending requests, and complete byte coverage
                            if (
                                current_rec is not None and
                                id(current_rec) == record_id and
                                len(current_rec.get("pending_requests", set())) == 0 and
                                is_fully_covered(current_rec.get("delivered_ranges", []), current_rec.get("size", 0))
                            ):
                                current_filepath = current_rec.get("filepath") or filepath
                                print(f"[NGINX_LOG] Decision: DELETE file_id={file_id} (quiet period elapsed, 0 pending requests, full coverage verified)")
                                active_file_map.pop(file_id, None)
                                if pending_disconnect_cleanups.get(file_id) is this_task:
                                    pending_disconnect_cleanups.pop(file_id, None)
                                cleanup_file(current_filepath, trigger="nginx_quiet_period_elapsed", job_id=jid, file_id=file_id)
                                if current_filepath != filepath and os.path.exists(filepath):
                                    cleanup_file(filepath, trigger="nginx_quiet_period_elapsed_original")
                            else:
                                print(f"[NGINX_LOG] Decision: KEEP file_id={file_id} (quiet period cancelled or new requests registered)")
                                if (
                                    current_rec is not None and
                                    len(current_rec.get("pending_requests", set())) == 0 and
                                    current_rec.get("active_streams", 0) == 0 and
                                    current_rec.get("storage_type") == "tmpfs"
                                ):
                                    await migrate_tmpfs_to_disk(file_id, current_rec)
                    except asyncio.CancelledError:
                        pass
                    finally:
                        async with state_lock:
                            if pending_disconnect_cleanups.get(file_id) is this_task:
                                pending_disconnect_cleanups.pop(file_id, None)

                existing_timer = pending_disconnect_cleanups.pop(file_id, None)
                if existing_timer:
                    existing_timer.cancel()
                this_task = asyncio.create_task(delayed_quiet_period_cleanup())
                pending_disconnect_cleanups[file_id] = this_task
            else:
                coverage_info = rec.get("delivered_ranges", [])
                active_streams = rec.get("active_streams", 0)
                print(f"[NGINX_LOG] Decision: KEEP file_id={file_id} (covered={coverage_info}, file_size={file_size}, pending_requests={remaining_requests}, active_streams={active_streams})")
                if remaining_requests == 0 and active_streams == 0:
                    if rec.get("storage_type") == "tmpfs":
                        await migrate_tmpfs_to_disk(file_id, rec)

### --- 8.2: STREAMING RESPONSE WRAPPER --- ###
class ResilientFileResponse(FileResponse):
    def __init__(self, *args, on_completed=None, on_disconnected=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.on_completed = on_completed
        self.on_disconnected = on_disconnected
        self.chunk_size = 1024 * 1024  # 1 MiB chunk size for high throughput

    async def __call__(self, scope, receive, send) -> None:
        completed = False
        callback_executed = False

        async def tracking_send(message):
            nonlocal completed
            # Forward the message to the client first; only mark completed if send succeeds
            await send(message)
            if message.get("type") == "http.response.body" and not message.get("more_body", True):
                completed = True

        try:
            await super().__call__(scope, receive, tracking_send)
        finally:
            if not callback_executed:
                callback_executed = True
                cb = self.on_completed if completed else self.on_disconnected
                if cb:
                    res = cb()
                    if asyncio.iscoroutine(res):
                        # Shield callback execution against cancellation to guarantee active_streams decrement
                        await asyncio.shield(res)

### --- 8.3: APPLICATION STARTUP & LIFESPAN MANAGEMENT --- ###
def remove_path_force(target_path: str):
    """
    Deletes a file, symlink, or directory tree during startup cleanup.
    Handles read-only attributes on Windows and POSIX.
    If removal fails, logs the exact path and error, then raises RuntimeError to fail startup.
    """
    try:
        if os.path.isdir(target_path) and not os.path.islink(target_path):
            def _handle_readonly(func, path, exc_info):
                try:
                    os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
                    func(path)
                except Exception:
                    raise exc_info[1]

            shutil.rmtree(target_path, onerror=_handle_readonly)
        else:
            try:
                os.remove(target_path)
            except PermissionError:
                os.chmod(target_path, stat.S_IWRITE | stat.S_IREAD)
                os.remove(target_path)
    except Exception as e:
        print(f"CRITICAL STARTUP ERROR: Could not remove temporary asset at {target_path} - {e}")
        raise RuntimeError(f"Startup cleanup failed: Could not remove {target_path}: {e}") from e


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Reset in-memory download registrations and pending request state during startup
    # Do not preserve or recover downloads from previous runs
    async with state_lock:
        active_file_map.clear()
        active_downloads.clear()
        active_download_tasks.clear()
        pending_disconnect_cleanups.clear()
        format_cache.clear()

    # 2. Reset SQLite mobile jobs database and in-memory mobile API state
    try:
        job_manager.init_db()
        cleared_count = job_manager.clear_all_jobs()
        import mobile_api
        mobile_api.reset_in_memory_state()
        print(f"Mobile jobs clean slate startup complete (cleared {cleared_count} previous records).")
    except Exception as db_init_err:
        print(f"Warning: Failed to initialize/clear mobile jobs SQLite database: {db_init_err}")

    # 3. Clear ALL contents of TMPFS_DOWNLOAD_DIR and TEMP_DOWNLOAD_DIR on startup
    # Never silently skip leftovers; fail startup if any item cannot be removed
    for target_dir in (TMPFS_DOWNLOAD_DIR, TEMP_DOWNLOAD_DIR):
        if not os.path.exists(target_dir):
            continue
        try:
            entries = os.listdir(target_dir)
        except Exception as ls_err:
            print(f"CRITICAL STARTUP ERROR: Could not list directory {target_dir} - {ls_err}")
            raise RuntimeError(f"Startup cleanup failed: Could not list {target_dir}: {ls_err}") from ls_err

        for filename in entries:
            file_path = os.path.join(target_dir, filename)
            remove_path_force(file_path)

    maintenance_task = asyncio.create_task(periodic_maintenance_loop())

    # Initialize AF_UNIX / SOCK_DGRAM listener for Nginx access-log events if running on POSIX
    nginx_sock_transport = None
    nginx_bound_socket = None
    socket_path = os.path.join(RUN_DIR, "streamvault_logs.sock")

    if hasattr(socket, "AF_UNIX"):
        try:
            if os.path.exists(socket_path):
                os.remove(socket_path)

            nginx_bound_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            nginx_bound_socket.setblocking(False)
            nginx_bound_socket.bind(socket_path)

            # Restrictive permissions: 660 (owner & group read/write, no world access)
            os.chmod(socket_path, 0o660)

            # Configurable group ownership for host Nginx access
            if NGINX_LOG_SOCKET_GROUP:
                try:
                    import grp
                    group_info = grp.getgrnam(NGINX_LOG_SOCKET_GROUP)
                    os.chown(socket_path, -1, group_info.gr_gid)
                except (KeyError, ImportError, PermissionError, OSError) as grp_err:
                    print(f"Warning: Could not set group ownership for {socket_path} to '{NGINX_LOG_SOCKET_GROUP}': {grp_err}")

            loop = asyncio.get_running_loop()
            nginx_sock_transport, _ = await loop.create_datagram_endpoint(
                lambda: NginxLogDatagramProtocol(),
                sock=nginx_bound_socket
            )
        except Exception as sock_init_err:
            print(f"Warning: Failed to initialize Nginx access-log Unix datagram listener: {sock_init_err}")
            if nginx_bound_socket:
                nginx_bound_socket.close()

    try:
        yield
    finally:
        # Shutdown Nginx access-log listener and remove socket file
        if nginx_sock_transport:
            nginx_sock_transport.close()
        elif nginx_bound_socket:
            nginx_bound_socket.close()

        if os.path.exists(socket_path):
            try:
                os.remove(socket_path)
            except OSError:
                pass

        maintenance_task.cancel()
        try:
            await maintenance_task
        except asyncio.CancelledError:
            pass

        # Cancel any pending disconnect cleanup timers and delete remaining files immediately
        async with state_lock:
            for task in list(pending_disconnect_cleanups.values()):
                task.cancel()
            pending_disconnect_cleanups.clear()

        # Graceful shutdown: signal cancellation to any running downloads and wait with timeout
        async with state_lock:
            running_tasks = [
                rec["worker_task"] for rec in active_downloads.values()
                if "worker_task" in rec and rec["worker_task"] is not None and not rec["worker_task"].done()
            ]
            for rec in active_downloads.values():
                rec["cancel_event"].set()

        if running_tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*running_tasks, return_exceptions=True), timeout=10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

        # Graceful shutdown of mobile background tasks
        try:
            import mobile_api
            await mobile_api.shutdown_mobile_tasks()
        except Exception as e:
            print(f"Error during mobile tasks shutdown: {e}")

app = FastAPI(title="StreamVault Premium Platform Downloader", lifespan=lifespan)
import mobile_api
app.include_router(mobile_api.router)
app.mount("/static", StaticFiles(directory=frontend_dir), name="static")


### --- SECTION 9: UI & AUTHENTICATION ROUTES --- ###
@app.get("/")
async def get_ui(request: Request):
    try:
        verify_session(request)
        return FileResponse(os.path.join(frontend_dir, "index.html"))
    except HTTPException:
        return FileResponse(os.path.join(frontend_dir, "login.html"))

@app.post("/api/login", dependencies=[Depends(verify_csrf)])
async def login(request: Request):
    try:
        body = (await request.body()).decode("utf-8")
        form_data = parse_qs(body)
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="Invalid login request")

    username = form_data.get("username", [""])[0]
    password = form_data.get("password", [""])[0]

    # Normalize unicode to NFC and compare as UTF-8 encoded bytes
    # Usernames are matched case-insensitively with leading/trailing whitespace stripped
    norm_req_user = unicodedata.normalize('NFC', username.strip().casefold()).encode('utf-8')
    norm_auth_user = unicodedata.normalize('NFC', AUTH_USERNAME.strip().casefold()).encode('utf-8')

    # Passwords preserve exact characters, normalized to NFC bytes to bridge OS input differences
    norm_req_pass = unicodedata.normalize('NFC', password).encode('utf-8')
    norm_auth_pass = unicodedata.normalize('NFC', AUTH_PASSWORD).encode('utf-8')

    if not hmac.compare_digest(norm_req_user, norm_auth_user) or not hmac.compare_digest(norm_req_pass, norm_auth_pass):
        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid credentials"}
        )


    now = time.time()
    session_id = str(uuid.uuid4())
    timestamp = int(now)
    msg = f"{username}:{session_id}:{timestamp}"
    sig = hmac.new(SESSION_SECRET.encode(), msg.encode(), digestmod="sha256").hexdigest()
    token = f"{msg}:{sig}"

    async with state_lock:
        active_sessions[session_id] = now + SESSION_EXPIRE_SECONDS

    response = JSONResponse(content={"message": "Authentication successful"})
    response.set_cookie(
        key="streamvault_session",
        value=token,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite="strict",
        max_age=SESSION_EXPIRE_SECONDS
    )
    return response

@app.post("/api/logout", dependencies=[Depends(verify_csrf)])
async def logout(request: Request):
    token = request.cookies.get("streamvault_session")
    if token:
        try:
            parts = token.split(":")
            if len(parts) == 4:
                async with state_lock:
                    active_sessions.pop(parts[1], None)
        except Exception:
            pass

    response = JSONResponse(content={"message": "Logged out"})
    response.delete_cookie(key="streamvault_session", httponly=True, secure=COOKIE_SECURE, samesite="strict")
    return response

### --- SECTION 10: METADATA EXTRACTION ROUTE --- ###
@app.get("/api/options", dependencies=[Depends(verify_session)])
async def get_options(url: str):
    try:
        await validate_url_safety(url)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"URL Security Exception: {e}")

    if extraction_semaphore.locked():
        raise HTTPException(status_code=429, detail="Server is at max metadata capacity. Try again shortly.")

    try:
        await asyncio.wait_for(extraction_semaphore.acquire(), timeout=0.1)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=429, detail="Server is at max metadata capacity. Try again shortly.")

    cookie_path = os.path.join(current_dir, "cookies.txt")

    ydl_opts = {
        'quiet': True,
        'skip_download': True,
        'socket_timeout': 15,
        'remote_components': ['ejs:github']
    }
    if os.path.exists(cookie_path):
        ydl_opts['cookiefile'] = cookie_path

    def fetch_metadata():
        with ReadOnlyYoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            video_title = clean_media_title(info) if isinstance(info, dict) else "video"
            raw_formats = info.get('formats', [])

            processed_options = []
            seen_resolutions = set()

            for f in raw_formats:
                height = f.get('height')
                ext = f.get('ext', '')

                if height and height >= 144:
                    res_label = f"{height}p"
                    format_id = f.get('format_id')

                    if f.get('vcodec') != 'none' and f.get('acodec') != 'none':
                        full_format_selector = format_id
                    else:
                        full_format_selector = f"bestvideo[height={height}]+bestaudio/best"

                    note = "High Efficiency" if ext == "webm" else "Standard MP4 Codec"

                    unique_key = f"{res_label}_{ext}"
                    if unique_key not in seen_resolutions:
                        seen_resolutions.add(unique_key)
                        processed_options.append({
                            "format_id": full_format_selector,
                            "resolution": res_label,
                            "ext": ext,
                            "note": note
                        })

            processed_options.insert(0, {
                "format_id": "bestvideo+bestaudio/best",
                "resolution": "Best Available Quality",
                "ext": "mp4",
                "note": "Auto-Detect Max Resolution"
            })

            if len(processed_options) > 1:
                sub_list = processed_options[1:]
                sub_list.sort(key=lambda x: int(x['resolution'].replace('p', '')) if 'p' in x['resolution'] else 0, reverse=True)
                processed_options = [processed_options[0]] + sub_list

            return {"title": video_title, "formats": processed_options}

    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, fetch_metadata)
        async with state_lock:
            format_cache[url] = {
                "formats": {opt["format_id"] for opt in result["formats"]},
                "expires": time.time() + 3600
            }
        return result
    except Exception as e:
        print(f"Metadata extraction error for {url}: {clean_terminal_ansi(str(e))}")
        raise HTTPException(status_code=400, detail="Failed to retrieve video metadata. Please verify the URL and try again.")
    finally:
        extraction_semaphore.release()


### --- SECTION 11: CORE DOWNLOAD ENGINE --- ###
### --- 11.1: DOWNLOAD REGISTRATION & PROGRESS MONITORING --- ###
async def execute_download_core(
    file_uuid: str,
    url: str,
    format_id: str,
    cancel_event: threading.Event,
    progress_callback: typing.Optional[typing.Callable[[dict], None]] = None,
    job_id: typing.Optional[str] = None,
) -> dict:
    main_loop = asyncio.get_running_loop()
    worker_task = None
    captured_meta = {}

    async with state_lock:
        if file_uuid not in active_downloads:
            active_downloads[file_uuid] = {
                "cancel_event": cancel_event,
                "worker_task": None
            }

    def progress_hook(d):
        if cancel_event.is_set():
            raise DownloadCancelled("User aborted the download.")

        info_dict = d.get('info_dict') or {}
        if info_dict:
            extracted_title = clean_media_title(info_dict)
        elif d.get('title'):
            extracted_title = clean_media_title({'title': d.get('title')})
        else:
            extracted_title = None

        if extracted_title and 'title' not in captured_meta:
            captured_meta['title'] = extracted_title

        status = d.get('status')
        if status == 'downloading':
            diagnostic_timing.tracker.record_extraction_end(job_id)
            if not captured_meta.get("_logged_dl_start"):
                captured_meta["_logged_dl_start"] = True
                if diagnostic_logger.is_enabled():
                    diagnostic_logger.log_event(
                        "ytdlp_media_download_start",
                        job_id=job_id,
                        file_id=file_uuid,
                        details={"total_bytes": d.get('total_bytes') or d.get('total_bytes_estimate') or 0}
                    )
            total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
            downloaded = d.get('downloaded_bytes', 0)
            percent = round((downloaded / total) * 100, 2) if total > 0 else 0

            size_mib = f"{round(total / (1024*1024), 2)} MiB" if total > 0 else "Unknown Size"
            speed_value = d.get('speed')
            speed_str = f"{round(speed_value / (1024*1024), 2)} MiB/s" if speed_value is not None else "Calculating..."
            eta_val = d.get('eta')
            eta_str = f"{eta_val}s" if eta_val is not None else "Unknown"

            payload = {
                "status": "processing",
                "file_id": file_uuid,
                "percent": percent,
                "total_size": size_mib,
                "speed": speed_str,
                "eta": eta_str
            }
            if 'title' in captured_meta:
                payload["title"] = captured_meta['title']

            if progress_callback:
                main_loop.call_soon_threadsafe(progress_callback, payload)
        elif status == 'finished':
            diagnostic_timing.tracker.record_media_download_end(job_id)
            if not captured_meta.get("_logged_dl_end"):
                captured_meta["_logged_dl_end"] = True
                if diagnostic_logger.is_enabled():
                    diagnostic_logger.log_event(
                        "ytdlp_media_download_end",
                        job_id=job_id,
                        file_id=file_uuid,
                        details={"downloaded_bytes": d.get('downloaded_bytes', 0)}
                    )

    def postprocessor_hook(d):
        if cancel_event.is_set():
            raise DownloadCancelled("User aborted the download.")
        status = d.get('status') if isinstance(d, dict) else None
        if status in (None, 'started'):
            diagnostic_timing.tracker.record_postprocess_start(job_id)
            if not captured_meta.get("_logged_pp_start"):
                captured_meta["_logged_pp_start"] = True
                if diagnostic_logger.is_enabled():
                    diagnostic_logger.log_event(
                        "ytdlp_postprocess_start",
                        job_id=job_id,
                        file_id=file_uuid,
                        details={"postprocessor": d.get('postprocessor') if isinstance(d, dict) else None}
                    )

    cookie_path = os.path.join(current_dir, "cookies.txt")
    ydl_opts = {
        'format': format_id,
        'outtmpl': os.path.join(TEMP_DOWNLOAD_DIR, f"{file_uuid}.%(ext)s"),
        'merge_output_format': 'mp4',
        'progress_hooks': [progress_hook],
        'postprocessor_hooks': [postprocessor_hook],
        'quiet': True,
        'no_warnings': True,
        'socket_timeout': 30,
        'max_filesize': 50 * 1024 * 1024 * 1024,
        'remote_components': ['ejs:github']
    }
    if os.path.exists(cookie_path):
        ydl_opts['cookiefile'] = cookie_path

    subproc_tracker = SubprocessTracker()

    ### --- 11.2: SYNCHRONOUS WORKER & YT-DLP EXECUTION --- ###
    def run_native_download():
        _thread_local.subprocess_tracker = subproc_tracker
        try:
            diagnostic_timing.tracker.record_extraction_start(job_id)
            if diagnostic_logger.is_enabled():
                diagnostic_logger.log_event(
                    "ytdlp_extraction_start",
                    job_id=job_id,
                    file_id=file_uuid,
                    details={"format_id": format_id, "url": diagnostic_logger.sanitize_url_for_logging(url)}
                )
            with ReadOnlyYoutubeDL(ydl_opts) as ydl:
                info = None
                if hasattr(ydl, "extract_info"):
                    info = ydl.extract_info(url, download=True)
                else:
                    ydl.download([url])
                if isinstance(info, dict):
                    resolved_title = clean_media_title(info)
                    if resolved_title:
                        captured_meta['title'] = resolved_title
            diagnostic_timing.tracker.record_postprocess_end(job_id)
            if diagnostic_logger.is_enabled():
                diagnostic_logger.log_event(
                    "ytdlp_postprocess_end",
                    job_id=job_id,
                    file_id=file_uuid,
                    details={"resolved_title": captured_meta.get('title')}
                )
            return "DONE", None
        except DownloadCancelled:
            subproc_tracker.terminate_all()
            return "CANCELLED", None
        except Exception as e:
            subproc_tracker.terminate_all()
            err_text = clean_terminal_ansi(str(e))
            print(f"Download processing error for {url}: {err_text}")
            if diagnostic_logger.is_enabled():
                diagnostic_logger.log_event(
                    "download_core_exception",
                    job_id=job_id,
                    file_id=file_uuid,
                    details={"error": err_text, "traceback": traceback.format_exc()}
                )
            return "ERROR", f"Failed to retrieve video metadata or download stream: {err_text}"
        finally:
            subproc_tracker.terminate_all()
            _thread_local.subprocess_tracker = None

    worker_task = main_loop.run_in_executor(None, run_native_download)
    async with state_lock:
        if file_uuid in active_downloads:
            active_downloads[file_uuid]["worker_task"] = worker_task

    MAX_EXECUTION_TIME = 7200  # 2 hours

    try:
        try:
            outcome, err_msg = await asyncio.wait_for(asyncio.shield(worker_task), timeout=MAX_EXECUTION_TIME)
        except asyncio.TimeoutError:
            cancel_event.set()
            outcome, err_msg = "ERROR", "Download execution time limit exceeded."
        except asyncio.CancelledError:
            cancel_event.set()
            outcome, err_msg = "CANCELLED", "Download Cancelled."
            raise

        ### --- 11.3: STORAGE TIERING & POST-DOWNLOAD COMPLETION --- ###
        if outcome == "DONE":
            downloaded_file = None
            for file in os.listdir(TEMP_DOWNLOAD_DIR):
                if file.startswith(file_uuid) and not file.endswith((".part", ".ytdl")):
                    downloaded_file = os.path.join(TEMP_DOWNLOAD_DIR, file)
                    break

            if downloaded_file and os.path.exists(downloaded_file):
                raw_ext = os.path.splitext(downloaded_file)[1].lower()
                if raw_ext not in VALID_VIDEO_EXTENSIONS:
                    normalized_path = os.path.join(TEMP_DOWNLOAD_DIR, f"{file_uuid}.mp4")
                    try:
                        os.replace(downloaded_file, normalized_path)
                        downloaded_file = normalized_path
                        if diagnostic_logger.is_enabled():
                            diagnostic_logger.log_event(
                                "extension_normalized",
                                job_id=job_id,
                                file_id=file_uuid,
                                details={"raw_ext": raw_ext, "normalized_ext": ".mp4"}
                            )
                    except Exception as ren_err:
                        print(f"Warning: Failed to normalize extension of {downloaded_file}: {ren_err}")

                actual_size = os.path.getsize(downloaded_file)
                target_path = downloaded_file
                storage_type = "disk"

                disk_free = shutil.disk_usage(TEMP_DOWNLOAD_DIR).free if os.path.exists(TEMP_DOWNLOAD_DIR) else 0
                tmpfs_free = shutil.disk_usage(TMPFS_DOWNLOAD_DIR).free if os.path.exists(TMPFS_DOWNLOAD_DIR) else 0
                if diagnostic_logger.is_enabled():
                    diagnostic_logger.log_event(
                        "storage_tiering_evaluation",
                        job_id=job_id,
                        file_id=file_uuid,
                        details={
                            "actual_size": actual_size,
                            "threshold_bytes": TMPFS_THRESHOLD_BYTES,
                            "disk_free_bytes": disk_free,
                            "tmpfs_free_bytes": tmpfs_free
                        }
                    )

                # Hybrid storage tiering: move files < 200 MB to dedicated 200 MB tmpfs
                if actual_size < TMPFS_THRESHOLD_BYTES:
                    tmpfs_dest = os.path.join(TMPFS_DOWNLOAD_DIR, os.path.basename(downloaded_file))
                    try:
                        shutil.move(downloaded_file, tmpfs_dest)
                        target_path = tmpfs_dest
                        storage_type = "tmpfs"
                        if diagnostic_logger.is_enabled():
                            diagnostic_logger.log_event(
                                "storage_tiering_result",
                                job_id=job_id,
                                file_id=file_uuid,
                                details={"storage_type": "tmpfs", "dest": tmpfs_dest}
                            )
                    except Exception as move_err:
                        print(f"Warning: Failed to move {downloaded_file} to tmpfs: {move_err}. Retaining on disk.")
                        # Ensure any partial or corrupted destination file on tmpfs is cleaned up
                        cleanup_file(tmpfs_dest, trigger="tmpfs_move_error", job_id=job_id, file_id=file_uuid)
                        target_path = downloaded_file
                        storage_type = "disk"
                        if diagnostic_logger.is_enabled():
                            diagnostic_logger.log_event(
                                "storage_tiering_result",
                                job_id=job_id,
                                file_id=file_uuid,
                                details={"storage_type": "disk", "fallback_reason": str(move_err)}
                            )

                async with state_lock:
                    active_file_map[file_uuid] = {
                        "filepath": target_path,
                        "expires": time.time() + UNCLAIMED_FILE_EXPIRY,
                        "storage_type": storage_type,
                        "size": actual_size,
                        "job_id": job_id
                    }

                if diagnostic_logger.is_enabled():
                    diagnostic_logger.log_event(
                        "download_core_outcome",
                        job_id=job_id,
                        file_id=file_uuid,
                        details={
                            "outcome": "DONE",
                            "storage_type": storage_type,
                            "size": actual_size,
                            "title": captured_meta.get("title", "Video")
                        }
                    )
                return {
                    "status": "completed",
                    "file_id": file_uuid,
                    "filepath": target_path,
                    "size": actual_size,
                    "storage_type": storage_type,
                    "title": captured_meta.get("title", "Video")
                }
            else:
                if diagnostic_logger.is_enabled():
                    diagnostic_logger.log_event(
                        "download_core_outcome",
                        job_id=job_id,
                        file_id=file_uuid,
                        details={"outcome": "FAILED", "error": "Assembly merging failure."}
                    )
                return {"status": "failed", "error": "Assembly merging failure."}

        elif outcome == "CANCELLED":
            if diagnostic_logger.is_enabled():
                diagnostic_logger.log_event(
                    "download_core_outcome",
                    job_id=job_id,
                    file_id=file_uuid,
                    details={"outcome": "CANCELLED", "error": "Download Cancelled."}
                )
            return {"status": "cancelled", "error": "Download Cancelled."}
        else:
            if diagnostic_logger.is_enabled():
                diagnostic_logger.log_event(
                    "download_core_outcome",
                    job_id=job_id,
                    file_id=file_uuid,
                    details={"outcome": "FAILED", "error": err_msg}
                )
            return {"status": "failed", "error": err_msg or "Download failed due to an internal error. Please try again."}

    finally:
        worker_finished = False
        try:
            cancel_event.set()
            subproc_tracker.terminate_all()
            try:
                if worker_task and not worker_task.done():
                    await asyncio.wait_for(asyncio.shield(worker_task), timeout=15.0)
                worker_finished = True
            except asyncio.TimeoutError:
                print(f"Worker task did not terminate within timeout for {file_uuid}; tracking until completion.")
                async def background_reaper():
                    try:
                        if worker_task:
                            await worker_task
                    except Exception:
                        pass
                    finally:
                        try:
                            async with state_lock:
                                active_downloads.pop(file_uuid, None)
                                has_claimed = file_uuid in active_file_map
                            if not has_claimed:
                                for f in os.listdir(TEMP_DOWNLOAD_DIR):
                                    if f.startswith(file_uuid):
                                        cleanup_file(os.path.join(TEMP_DOWNLOAD_DIR, f), trigger="background_reaper", job_id=job_id, file_id=file_uuid)
                        except Exception as reaper_err:
                            print(f"Error during background reaper cleanup for {file_uuid}: {clean_terminal_ansi(str(reaper_err))}")
                reaper_task = asyncio.create_task(background_reaper())
                active_download_tasks.add(reaper_task)
                reaper_task.add_done_callback(active_download_tasks.discard)
            except asyncio.CancelledError:
                raise
            except Exception as wait_err:
                print(f"Error while waiting for worker task termination for {file_uuid}: {clean_terminal_ansi(str(wait_err))}")
                worker_finished = True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Unexpected error in cleanup_worker signalling phase: {clean_terminal_ansi(str(e))}")

        # Only perform immediate file cleanup if the worker thread has actually finished
        if worker_finished:
            try:
                async with state_lock:
                    active_downloads.pop(file_uuid, None)
                    has_claimed_file = file_uuid in active_file_map
                if not has_claimed_file:
                    for f in os.listdir(TEMP_DOWNLOAD_DIR):
                        if f.startswith(file_uuid):
                            cleanup_file(os.path.join(TEMP_DOWNLOAD_DIR, f), trigger="unclaimed_download_cleanup", job_id=job_id, file_id=file_uuid)
            except Exception as e:
                print(f"Error during physical file cleanup for {file_uuid}: {clean_terminal_ansi(str(e))}")

        # Notify mobile queue dispatcher that a download capacity slot has freed up
        try:
            import mobile_api
            mobile_api.schedule_mobile_queue()
        except Exception:
            pass


### --- SECTION 12: DOWNLOAD PROGRESS ROUTE (SSE ADAPTER) --- ###
@app.get("/api/download-progress", dependencies=[Depends(verify_session)])
async def download_progress(request: Request, url: str, format_id: str):
    try:
        await validate_url_safety(url)
        now = time.time()
        async with state_lock:
            cache_entry = format_cache.get(url)
            if not cache_entry or now >= cache_entry["expires"] or format_id not in cache_entry["formats"]:
                raise ValueError("Invalid or expired format_id requested for this URL.")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    async def sse_event_generator():
        cancel_event = threading.Event()
        async with state_lock:
            if len(active_downloads) >= MAX_CONCURRENT_DOWNLOADS:
                yield f"data: {json.dumps({'status': 'failed', 'error': 'Server at maximum download capacity. Try again later.'})}\n\n"
                return

            file_uuid = str(uuid.uuid4())
            active_downloads[file_uuid] = {
                "cancel_event": cancel_event,
                "worker_task": None
            }

        queue = asyncio.Queue()

        def on_progress(payload: dict):
            queue.put_nowait(payload)

        core_task = asyncio.create_task(
            execute_download_core(
                file_uuid=file_uuid,
                url=url,
                format_id=format_id,
                cancel_event=cancel_event,
                progress_callback=on_progress
            )
        )
        active_download_tasks.add(core_task)
        core_task.add_done_callback(active_download_tasks.discard)

        try:
            while True:
                if await request.is_disconnected():
                    cancel_event.set()
                    break

                if core_task.done() and queue.empty():
                    break

                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=1.0)
                    yield f"data: {json.dumps(payload)}\n\n"
                except asyncio.TimeoutError:
                    continue

            # If client disconnected, signal cancel_event
            if await request.is_disconnected():
                cancel_event.set()

            # Drain any remaining progress payloads before yielding final outcome
            while not queue.empty():
                try:
                    payload = queue.get_nowait()
                    yield f"data: {json.dumps(payload)}\n\n"
                except asyncio.QueueEmpty:
                    break

            # If task not finished yet and not disconnected, wait briefly for completion result
            if not core_task.done() and not cancel_event.is_set():
                try:
                    res = await asyncio.wait_for(asyncio.shield(core_task), timeout=2.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    res = None
            elif core_task.done():
                try:
                    res = core_task.result()
                except Exception:
                    res = {"status": "failed", "error": "Download failed due to an internal error. Please try again."}
            else:
                res = None

            if res:
                if res.get("status") == "completed":
                    yield f"data: {json.dumps({'status': 'completed', 'file_id': file_uuid})}\n\n"
                elif res.get("status") == "cancelled":
                    yield f"data: {json.dumps({'status': 'failed', 'error': 'Download Cancelled.'})}\n\n"
                elif res.get("status") == "failed":
                    yield f"data: {json.dumps({'status': 'failed', 'error': res.get('error', 'Download failed due to an internal error. Please try again.')})}\n\n"

        finally:
            if await request.is_disconnected():
                cancel_event.set()

    return StreamingResponse(sse_event_generator(), media_type="text/event-stream")

### --- SECTION 13: CANCELLATION & FILE RETRIEVAL ROUTES --- ###
@app.post("/api/cancel", dependencies=[Depends(verify_csrf), Depends(verify_session)])
async def cancel_download(file_id: str):
    validate_uuid(file_id)
    async with state_lock:
        if file_id in active_downloads:
            active_downloads[file_id]["cancel_event"].set()
            return {"message": "Cancellation requested"}
    raise HTTPException(status_code=404, detail="Download not running")

async def deliver_file_response(request: Request, file_id: str, title: str, job_id: typing.Optional[str] = None) -> Response:
    validate_uuid(file_id)

    async with state_lock:
        file_record = active_file_map.get(file_id)
        if not file_record:
            job_manager.expire_job_asset(job_id=job_id, file_id=file_id)
            raise HTTPException(status_code=404, detail="Asset expired, invalid, or already downloaded.")

        if job_id:
            file_record["job_id"] = job_id
        resolved_job_id = file_record.get("job_id") or job_id

        # Cancel any pending 60s disconnect cleanup timer before registering a new stream
        pending_timer = pending_disconnect_cleanups.pop(file_id, None)
        if pending_timer:
            pending_timer.cancel()

        file_record["active_streams"] = file_record.get("active_streams", 0) + 1
        filepath = file_record["filepath"]
        file_size = file_record.get("size", 0)
        storage_type = file_record.get("storage_type", "disk")
        record_id = id(file_record)

        # Check for asset expiration or disk absence
        if time.time() >= file_record["expires"]:
            file_record["active_streams"] = max(0, file_record["active_streams"] - 1)
            active_file_map.pop(file_id, None)
            cleanup_file(filepath, trigger="asset_expired", job_id=resolved_job_id, file_id=file_id)
            job_manager.expire_job_asset(job_id=resolved_job_id, file_id=file_id)
            raise HTTPException(status_code=404, detail="Asset has expired.")

        if not os.path.exists(filepath):
            file_record["active_streams"] = max(0, file_record["active_streams"] - 1)
            active_file_map.pop(file_id, None)
            job_manager.expire_job_asset(job_id=resolved_job_id, file_id=file_id)
            raise HTTPException(status_code=404, detail="Asset no longer exists on disk.")

    # Flag indicating whether cleanup responsibility has been transferred to response callbacks / Nginx registration
    cleanup_registered = False

    try:
        safe_name = sanitize_filename(title).strip().rstrip(". ")

        raw_ext = os.path.splitext(filepath)[1].lower()
        ext = raw_ext if raw_ext in VALID_VIDEO_EXTENSIONS else ".mp4"

        if safe_name.lower().endswith(ext.lower()):
            safe_name = safe_name[:-len(ext)].rstrip(". ")

        # Bound HTTP download filename length to prevent oversized headers (e.g. Nginx 502 upstream header overflow)
        # Long Unicode/Malayalam titles can expand dramatically when percent-encoded (up to 9x bytes).
        MAX_DOWNLOAD_NAME_LENGTH = 80
        if len(safe_name) > MAX_DOWNLOAD_NAME_LENGTH:
            safe_name = safe_name[:MAX_DOWNLOAD_NAME_LENGTH]
            while safe_name and (unicodedata.combining(safe_name[-1]) or safe_name[-1] in ". "):
                safe_name = safe_name[:-1]

        if not safe_name:
            safe_name = "StreamVault_Download"

        guessed_type, _ = mimetypes.guess_type(f"video{ext}")
        media_type = guessed_type or ("video/mp4" if ext == ".mp4" else "application/octet-stream")
        encoded_filename = quote(f"{safe_name}{ext}")
        ascii_safe_name = re.sub(r'\s+', ' ', re.sub(r'[^\x20-\x7E]', ' ', safe_name).replace('"', '')).strip(". ")
        if len(ascii_safe_name) > MAX_DOWNLOAD_NAME_LENGTH:
            ascii_safe_name = ascii_safe_name[:MAX_DOWNLOAD_NAME_LENGTH].strip(". ")
        if not ascii_safe_name:
            ascii_safe_name = "StreamVault_Download"

        content_disposition = f'attachment; filename="{ascii_safe_name}{ext}"; filename*=UTF-8\'\'{encoded_filename}'

        range_header = request.headers.get("range", "").strip() if request.headers else ""
        is_range_request = bool(range_header)
        range_bounds = None
        if is_range_request and range_header.startswith("bytes="):
            parts = range_header[6:].split("-")
            if len(parts) == 2:
                try:
                    r_start = int(parts[0]) if parts[0] else 0
                    r_end = int(parts[1]) if parts[1] else (file_size - 1)
                    r_end = min(r_end, file_size - 1)
                    if 0 <= r_start <= r_end < file_size:
                        range_bounds = (r_start, r_end + 1)
                except ValueError:
                    range_bounds = None

        if diagnostic_logger.is_enabled():
            diagnostic_logger.log_event(
                "delivery_request_received",
                job_id=resolved_job_id,
                file_id=file_id,
                details={
                    "delivery_mode": "nginx_xaccel" if USE_X_ACCEL_REDIRECT else "python_stream",
                    "range_header": range_header,
                    "is_range_request": is_range_request,
                    "range_bounds": range_bounds,
                    "file_size": file_size,
                    "storage_type": storage_type
                }
            )

        def schedule_inactivity_cleanup(rec):
            """Helper to schedule a safe 60s delayed cleanup verifying record identity."""
            if diagnostic_logger.is_enabled():
                diagnostic_logger.log_event(
                    "inactivity_cleanup_scheduled",
                    job_id=rec.get("job_id"),
                    file_id=file_id,
                    details={"grace_seconds": DISCONNECT_CLEANUP_GRACE_SECONDS}
                )
            async def delayed_disconnect_cleanup():
                try:
                    await asyncio.sleep(DISCONNECT_CLEANUP_GRACE_SECONDS)
                    async with state_lock:
                        current_rec = active_file_map.get(file_id)
                        # Verify record identity and that zero streams are active
                        if current_rec is rec and current_rec.get("active_streams", 0) == 0:
                            current_filepath = current_rec.get("filepath") or filepath
                            active_file_map.pop(file_id, None)
                            if pending_disconnect_cleanups.get(file_id) is this_task:
                                pending_disconnect_cleanups.pop(file_id, None)
                            cleanup_file(current_filepath, trigger="inactivity_grace_elapsed", job_id=current_rec.get("job_id"), file_id=file_id)
                            if current_filepath != filepath and os.path.exists(filepath):
                                cleanup_file(filepath, trigger="inactivity_grace_elapsed_original")
                except asyncio.CancelledError:
                    pass
                finally:
                    async with state_lock:
                        if pending_disconnect_cleanups.get(file_id) is this_task:
                            pending_disconnect_cleanups.pop(file_id, None)

            existing_timer = pending_disconnect_cleanups.pop(file_id, None)
            if existing_timer:
                existing_timer.cancel()
            this_task = asyncio.create_task(delayed_disconnect_cleanup())
            pending_disconnect_cleanups[file_id] = this_task

        async def on_stream_end(was_full_success: bool):
            async with state_lock:
                rec = active_file_map.get(file_id)
                if not rec or id(rec) != record_id:
                    return
                rec["active_streams"] = max(0, rec.get("active_streams", 1) - 1)
                remaining_streams = rec["active_streams"]

                # If the response completed successfully, record delivered byte range
                if was_full_success:
                    if is_range_request and range_bounds:
                        rec["delivered_ranges"] = add_delivered_range(
                            rec.get("delivered_ranges", []),
                            range_bounds[0],
                            range_bounds[1]
                        )
                    elif not is_range_request and file_size > 0:
                        rec["delivered_ranges"] = add_delivered_range(
                            rec.get("delivered_ranges", []),
                            0,
                            file_size
                        )

                fully_covered = is_fully_covered(rec.get("delivered_ranges", []), file_size)

                if diagnostic_logger.is_enabled():
                    diagnostic_logger.log_event(
                        "stream_ended" if was_full_success else "stream_interrupted",
                        job_id=rec.get("job_id") or resolved_job_id,
                        file_id=file_id,
                        details={
                            "was_full_success": was_full_success,
                            "is_range_request": is_range_request,
                            "range_bounds": range_bounds,
                            "remaining_streams": remaining_streams,
                            "delivered_ranges": rec.get("delivered_ranges", []),
                            "fully_covered": fully_covered
                        }
                    )

                # If full byte coverage is achieved, update mobile job status to completed idempotently
                if fully_covered:
                    jid = rec.get("job_id")
                    if jid:
                        try:
                            job_manager.update_job(jid, status="completed")
                        except Exception as jm_err:
                            print(f"Warning: Failed to update job {jid} to completed: {jm_err}")
                        diagnostic_timing.tracker.record_delivery_end(jid, bytes_delivered=file_size)

                # Immediate cleanup only when fully covered and no other streams are active
                if fully_covered and remaining_streams == 0:
                    current_filepath = rec.get("filepath") or filepath
                    active_file_map.pop(file_id, None)
                    timer = pending_disconnect_cleanups.pop(file_id, None)
                    if timer:
                        timer.cancel()
                    cleanup_file(current_filepath, trigger="delivery_fully_covered", job_id=rec.get("job_id"), file_id=file_id)
                    if current_filepath != filepath and os.path.exists(filepath):
                        cleanup_file(filepath, trigger="delivery_fully_covered_original")
                    return

                # For Range requests, disconnects, or partial streams with 0 active streams, start 60s grace timer
                if remaining_streams == 0:
                    if rec.get("storage_type") == "tmpfs":
                        await migrate_tmpfs_to_disk(file_id, rec)
                    schedule_inactivity_cleanup(rec)

        # Mode 1: Nginx X-Accel-Redirect delivery
        if USE_X_ACCEL_REDIRECT:
            filename_only = os.path.basename(filepath)
            internal_base = f"/internal_tmpfs/{filename_only}" if storage_type == "tmpfs" else f"/internal_downloads/{filename_only}"

            request_id = str(uuid.uuid4())
            async with state_lock:
                # Register unique request ID before returning X-Accel-Redirect
                rec = active_file_map.get(file_id)
                if rec:
                    pending_set = rec.setdefault("pending_requests", set())
                    pending_set.add(request_id)
                    rec.setdefault("pending_request_times", {})[request_id] = time.time()
                    print(f"[NGINX_LOG] Request registered: file_id={file_id}, request_id={request_id} (pending: {len(pending_set)})")

            # Ownership of decrement handed over to Nginx log listener socket / stale request expiry
            cleanup_registered = True

            internal_uri = f"{internal_base}?sv_fid={file_id}&sv_rid={request_id}"

            if diagnostic_logger.is_enabled():
                diagnostic_logger.log_event(
                    "nginx_xaccel_registered",
                    job_id=resolved_job_id,
                    file_id=file_id,
                    details={
                        "request_id": request_id,
                        "internal_uri": internal_uri,
                        "pending_requests_count": len(pending_set) if rec else 0
                    }
                )

            headers = {
                "X-Accel-Redirect": internal_uri,
                "Content-Type": media_type,
                "Content-Disposition": content_disposition,
                "Accept-Ranges": "bytes"
            }
            return Response(status_code=200, headers=headers)

        # Mode 2: Hybrid Python ResilientFileResponse delivery with Range & 60s disconnect handling
        headers = {
            "Content-Disposition": content_disposition,
            "Accept-Ranges": "bytes"
        }
        response = ResilientFileResponse(
            path=filepath,
            media_type=media_type,
            headers=headers,
            filename=f"{safe_name}{ext}",
            status_code=206 if is_range_request else 200,
            on_completed=lambda: on_stream_end(was_full_success=(not is_range_request or range_bounds is not None)),
            on_disconnected=lambda: on_stream_end(was_full_success=False)
        )
        cleanup_registered = True
        return response
    except Exception:
        if not cleanup_registered:
            async with state_lock:
                rec = active_file_map.get(file_id)
                if rec and id(rec) == record_id:
                    rec["active_streams"] = max(0, rec.get("active_streams", 1) - 1)
        raise

@app.get("/api/get-file", dependencies=[Depends(verify_session)])
async def get_file(request: Request, file_id: str, title: str):
    return await deliver_file_response(request, file_id, title)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8300)
