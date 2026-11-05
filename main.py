### --- SECTION 1: IMPORTS & ENVIRONMENT SETUP --- ###
import os
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
from contextlib import asynccontextmanager
from urllib.parse import urlparse, parse_qs, quote
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Depends
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
format_cache = {}
pending_disconnect_cleanups = {}

MAX_CONCURRENT_DOWNLOADS = 3
MAX_CONCURRENT_EXTRACTIONS = 5
UNCLAIMED_FILE_EXPIRY = 3600
DISCONNECT_CLEANUP_GRACE_SECONDS = 60
TMPFS_THRESHOLD_BYTES = 200 * 1024 * 1024  # 200 MB

# Concurrency synchronization primitives (single-process asyncio event loop)
state_lock = asyncio.Lock()
extraction_semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTIONS)

### --- SECTION 4: MAINTENANCE TASKS --- ###
async def run_maintenance_tasks():
    now = time.time()

    async with state_lock:
        expired_sessions = [sid for sid, exp in active_sessions.items() if now > exp]
        for sid in expired_sessions:
            active_sessions.pop(sid, None)

        expired_files = [fid for fid, rec in active_file_map.items() if now > rec["expires"]]
        files_to_clean = []
        for fid in expired_files:
            rec = active_file_map.pop(fid, None)
            if rec:
                files_to_clean.append(rec["filepath"])

        expired_formats = [u for u, rec in format_cache.items() if now > rec["expires"]]
        for u in expired_formats:
            format_cache.pop(u, None)

        if len(format_cache) > 500:
            oldest_formats = sorted(format_cache.keys(), key=lambda k: format_cache[k]["expires"])
            while len(format_cache) > 500 and oldest_formats:
                format_cache.pop(oldest_formats.pop(0), None)

    for filepath in files_to_clean:
        cleanup_file(filepath)


async def periodic_maintenance_loop():
    try:
        while True:
            await asyncio.sleep(60)
            await run_maintenance_tasks()
    except asyncio.CancelledError:
        pass


### --- SECTION 5: SECURITY & VALIDATION HELPERS --- ###
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


### --- SECTION 6: UTILITY FUNCTIONS & APP INIT --- ###
def cleanup_file(filepath: str):
    if os.path.exists(filepath):
        try:
            os.remove(filepath)
        except Exception:
            pass

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", name)

def clean_terminal_ansi(text: str) -> str:
    return re.sub(r'\x1b\[[0-9;]*m', '', str(text))

current_dir = os.path.dirname(os.path.abspath(__file__))
frontend_dir = os.path.join(current_dir, "frontend")
TEMP_DOWNLOAD_DIR = os.path.join(current_dir, "temp_downloads")
TMPFS_DOWNLOAD_DIR = os.path.join(current_dir, "temp_downloads_tmpfs")
RUN_DIR = os.path.join(current_dir, "run")
os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
os.makedirs(TMPFS_DOWNLOAD_DIR, exist_ok=True)
os.makedirs(RUN_DIR, exist_ok=True)

class NginxLogDatagramProtocol(asyncio.DatagramProtocol):
    def datagram_received(self, data: bytes, addr):
        try:
            raw_text = data.decode("utf-8", errors="ignore")
            print(f"[NGINX_LOG_DIAG] Received datagram from {addr}: {raw_text.strip()}")
            json_start = raw_text.find("{")
            json_end = raw_text.rfind("}")
            if json_start == -1 or json_end == -1:
                print("[NGINX_LOG_DIAG] No JSON payload found in datagram")
                return
            payload = json.loads(raw_text[json_start:json_end + 1])

            file_id = payload.get("file_id")
            request_id = payload.get("request_id")
            status = payload.get("status")
            bytes_sent = payload.get("bytes_sent", 0)
            content_length = str(payload.get("content_length", "0"))

            print(f"[NGINX_LOG_DIAG] Parsed fields: file_id={file_id}, request_id={request_id}, status={status}, bytes_sent={bytes_sent}, content_length={content_length}")

            if not file_id or not request_id:
                print("[NGINX_LOG_DIAG] Missing file_id or request_id; ignoring datagram")
                return

            asyncio.create_task(self.handle_completion(file_id, request_id, status, bytes_sent, content_length))
        except Exception as e:
            print(f"[NGINX_LOG_DIAG] Error parsing Nginx access-log datagram: {e}")

    async def handle_completion(self, file_id: str, request_id: str, status: int, bytes_sent: int, content_length: str):
        async with state_lock:
            rec = active_file_map.get(file_id)
            if not rec:
                print(f"[NGINX_LOG_DIAG] Lookup failed: file_id={file_id} not found in active_file_map")
                return

            pending_set = rec.get("pending_requests", set())
            if request_id not in pending_set:
                print(f"[NGINX_LOG_DIAG] Lookup failed: request_id={request_id} not in pending_requests for file_id={file_id} (pending: {len(pending_set)})")
                return

            pending_set.discard(request_id)
            file_size = rec.get("size", 0)

            # Completion criteria: HTTP 200, bytes_sent matches Content-Length and recorded file size
            is_full_complete = (
                status == 200 and
                content_length.isdigit() and
                bytes_sent == int(content_length) == file_size and
                file_size > 0
            )

            remaining_requests = len(pending_set)

            # Idempotent cleanup: delete only if full response validated and no other registered requests remain
            if is_full_complete and remaining_requests == 0:
                print(f"[NGINX_LOG_DIAG] Decision: DELETE file_id={file_id} (full completion verified, remaining_requests=0)")
                active_file_map.pop(file_id, None)
                timer = pending_disconnect_cleanups.pop(file_id, None)
                if timer:
                    timer.cancel()
                cleanup_file(rec["filepath"])
            else:
                print(f"[NGINX_LOG_DIAG] Decision: KEEP file_id={file_id} (is_full_complete={is_full_complete}, remaining_requests={remaining_requests}, file_size={file_size})")

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Clean up any abandoned temporary files from previous runs
    for target_dir in (TEMP_DOWNLOAD_DIR, TMPFS_DOWNLOAD_DIR):
        if os.path.exists(target_dir):
            for filename in os.listdir(target_dir):
                file_path = os.path.join(target_dir, filename)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                except Exception as e:
                    print(f"Startup cleanup error: Could not remove {file_path} - {e}")

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

app = FastAPI(title="StreamVault Premium Platform Downloader", lifespan=lifespan)
current_dir = os.path.dirname(os.path.abspath(__file__))
frontend_dir = os.path.join(current_dir, "frontend")
app.mount("/static", StaticFiles(directory=frontend_dir), name="static")


### --- SECTION 7: UI & AUTH ROUTES --- ###
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

### --- SECTION 8: METADATA ROUTE --- ###
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
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            video_title = info.get('title', 'video')
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


### --- SECTION 9.1: DOWNLOAD PROGRESS ROUTE (SETUP & WORKER) --- ###
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
        main_loop = asyncio.get_running_loop()

        def progress_hook(d):
            if cancel_event.is_set():
                raise DownloadCancelled("User aborted the download.")

            if d['status'] == 'downloading':
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

                asyncio.run_coroutine_threadsafe(queue.put(f"data: {json.dumps(payload)}\n\n"), main_loop)

        def postprocessor_hook(d):
            if cancel_event.is_set():
                raise DownloadCancelled("User aborted the download.")

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

        def run_native_download():
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([url])
                asyncio.run_coroutine_threadsafe(queue.put("DONE"), main_loop)
            except DownloadCancelled:
                asyncio.run_coroutine_threadsafe(queue.put("CANCELLED"), main_loop)
            except Exception as e:
                print(f"Download processing error for {url}: {clean_terminal_ansi(str(e))}")
                asyncio.run_coroutine_threadsafe(queue.put("ERROR:Download failed due to an internal error. Please try again."), main_loop)

        worker_task = main_loop.run_in_executor(None, run_native_download)
        async with state_lock:
            if file_uuid in active_downloads:
                active_downloads[file_uuid]["worker_task"] = worker_task

        # Soft limit for max download execution time before the client is informed of a timeout.
        MAX_EXECUTION_TIME = 7200  # 2 hours
        start_time = time.time()


### --- SECTION 9.2: DOWNLOAD PROGRESS ROUTE (EVENT LOOP & CLEANUP) --- ###
        try:
            while True:
                if await request.is_disconnected():
                    cancel_event.set()
                    break

                if time.time() - start_time > MAX_EXECUTION_TIME:
                    cancel_event.set()
                    yield f"data: {json.dumps({'status': 'failed', 'error': 'Download execution time limit exceeded.'})}\n\n"
                    break

                try:
                    item = await asyncio.wait_for(queue.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    continue

                if item == "DONE":
                    downloaded_file = None
                    for file in os.listdir(TEMP_DOWNLOAD_DIR):
                        if file.startswith(file_uuid):
                            downloaded_file = os.path.join(TEMP_DOWNLOAD_DIR, file)
                            break

                    if downloaded_file and os.path.exists(downloaded_file):
                        actual_size = os.path.getsize(downloaded_file)
                        target_path = downloaded_file
                        storage_type = "disk"

                        # Hybrid storage tiering: move files < 200 MB to dedicated 200 MB tmpfs
                        if actual_size < TMPFS_THRESHOLD_BYTES:
                            tmpfs_dest = os.path.join(TMPFS_DOWNLOAD_DIR, os.path.basename(downloaded_file))
                            try:
                                shutil.move(downloaded_file, tmpfs_dest)
                                target_path = tmpfs_dest
                                storage_type = "tmpfs"
                            except Exception as move_err:
                                print(f"Warning: Failed to move {downloaded_file} to tmpfs: {move_err}. Retaining on disk.")
                                # Ensure any partial or corrupted destination file on tmpfs is cleaned up
                                cleanup_file(tmpfs_dest)
                                target_path = downloaded_file
                                storage_type = "disk"

                        async with state_lock:
                            active_file_map[file_uuid] = {
                                "filepath": target_path,
                                "expires": time.time() + UNCLAIMED_FILE_EXPIRY,
                                "storage_type": storage_type,
                                "size": actual_size
                            }
                        yield f"data: {json.dumps({'status': 'completed', 'file_id': file_uuid})}\n\n"
                    else:
                        yield f"data: {json.dumps({'status': 'failed', 'error': 'Assembly merging failure.'})}\n\n"
                    break

                elif item == "CANCELLED":
                    yield f"data: {json.dumps({'status': 'failed', 'error': 'Download Cancelled.'})}\n\n"
                    break

                elif item.startswith("ERROR:"):
                    err_parts = item.split(":", 1)
                    err_msg = err_parts[1].strip() if len(err_parts) > 1 else err_parts[0]
                    yield f"data: {json.dumps({'status': 'failed', 'error': err_msg})}\n\n"
                    break
                else:
                    yield item
        finally:
            async def cleanup_worker():
                worker_finished = False
                try:
                    # Signal the worker to stop writing (caught by progress_hook & postprocessor_hook)
                    cancel_event.set()

                    try:
                        # Wait for worker with timeout to avoid indefinite event loop blocking
                        await asyncio.wait_for(asyncio.shield(worker_task), timeout=15.0)
                        worker_finished = True
                    except asyncio.TimeoutError:
                        print(f"Worker task did not terminate within timeout for {file_uuid}; tracking until completion.")
                        # Wait in background so cleanup does not delete files under active I/O
                        async def background_reaper():
                            try:
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
                                                cleanup_file(os.path.join(TEMP_DOWNLOAD_DIR, f))
                                except Exception as reaper_err:
                                    print(f"Error during background reaper cleanup for {file_uuid}: {clean_terminal_ansi(str(reaper_err))}")
                        asyncio.create_task(background_reaper())
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
                                    cleanup_file(os.path.join(TEMP_DOWNLOAD_DIR, f))
                    except Exception as e:
                        print(f"Error during physical file cleanup for {file_uuid}: {clean_terminal_ansi(str(e))}")

            asyncio.create_task(cleanup_worker())

    return StreamingResponse(sse_event_generator(), media_type="text/event-stream")

### --- SECTION 10: CANCELLATION & FILE RETRIEVAL ROUTES --- ###
@app.post("/api/cancel", dependencies=[Depends(verify_csrf), Depends(verify_session)])
async def cancel_download(file_id: str):
    validate_uuid(file_id)
    async with state_lock:
        if file_id in active_downloads:
            active_downloads[file_id]["cancel_event"].set()
            return {"message": "Cancellation requested"}
    raise HTTPException(status_code=404, detail="Download not running")

@app.get("/api/get-file", dependencies=[Depends(verify_session)])
async def get_file(request: Request, file_id: str, title: str):
    validate_uuid(file_id)

    async with state_lock:
        file_record = active_file_map.get(file_id)
        if not file_record:
            raise HTTPException(status_code=404, detail="Asset expired, invalid, or already downloaded.")

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
            cleanup_file(filepath)
            raise HTTPException(status_code=404, detail="Asset has expired.")

        if not os.path.exists(filepath):
            file_record["active_streams"] = max(0, file_record["active_streams"] - 1)
            active_file_map.pop(file_id, None)
            raise HTTPException(status_code=404, detail="Asset no longer exists on disk.")

    safe_name = sanitize_filename(title)
    if not safe_name:
        safe_name = "StreamVault_Download"

    ext = os.path.splitext(filepath)[1].lower()
    if not ext:
        ext = ".mp4"
    guessed_type, _ = mimetypes.guess_type(filepath)
    media_type = guessed_type or "application/octet-stream"
    encoded_filename = quote(f"{safe_name}{ext}")

    def schedule_inactivity_cleanup(rec):
        """Helper to schedule a safe 60s delayed cleanup verifying record identity."""
        async def delayed_disconnect_cleanup():
            try:
                await asyncio.sleep(DISCONNECT_CLEANUP_GRACE_SECONDS)
                async with state_lock:
                    current_rec = active_file_map.get(file_id)
                    # Verify record identity and that zero streams are active
                    if current_rec is rec and current_rec.get("active_streams", 0) == 0:
                        active_file_map.pop(file_id, None)
                        pending_disconnect_cleanups.pop(file_id, None)
                        cleanup_file(filepath)
            except asyncio.CancelledError:
                pass

        existing_timer = pending_disconnect_cleanups.pop(file_id, None)
        if existing_timer:
            existing_timer.cancel()
        pending_disconnect_cleanups[file_id] = asyncio.create_task(delayed_disconnect_cleanup())

    async def on_stream_end(was_full_success: bool):
        async with state_lock:
            rec = active_file_map.get(file_id)
            if not rec or id(rec) != record_id:
                return
            rec["active_streams"] = max(0, rec.get("active_streams", 1) - 1)
            remaining_streams = rec["active_streams"]

            # Immediate cleanup only for full non-range downloads when no other streams are active
            if was_full_success and remaining_streams == 0:
                active_file_map.pop(file_id, None)
                timer = pending_disconnect_cleanups.pop(file_id, None)
                if timer:
                    timer.cancel()
                cleanup_file(filepath)
                return

            # For Range requests, disconnects, or partial streams with 0 active streams, start 60s grace timer
            if remaining_streams == 0:
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
                rec.setdefault("pending_requests", set()).add(request_id)

        internal_uri = f"{internal_base}?sv_fid={file_id}&sv_rid={request_id}"

        headers = {
            "X-Accel-Redirect": internal_uri,
            "Content-Type": media_type,
            "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}",
            "Accept-Ranges": "bytes"
        }
        return Response(status_code=200, headers=headers)

    # Mode 2: Hybrid Python ResilientFileResponse delivery with Range & 60s disconnect handling
    is_range_request = "range" in request.headers
    return ResilientFileResponse(
        path=filepath,
        media_type=media_type,
        filename=f"{safe_name}{ext}",
        on_completed=lambda: on_stream_end(was_full_success=(not is_range_request)),
        on_disconnected=lambda: on_stream_end(was_full_success=False)
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8300)
