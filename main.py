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
from contextlib import asynccontextmanager
from urllib.parse import urlparse, parse_qs
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Depends
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
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

MAX_CONCURRENT_DOWNLOADS = 3
MAX_CONCURRENT_EXTRACTIONS = 5
UNCLAIMED_FILE_EXPIRY = 3600

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Clean up any abandoned temporary files from previous runs
    for filename in os.listdir(TEMP_DOWNLOAD_DIR):
        file_path = os.path.join(TEMP_DOWNLOAD_DIR, filename)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"Startup cleanup error: Could not remove {file_path} - {e}")

    maintenance_task = asyncio.create_task(periodic_maintenance_loop())
    try:
        yield
    finally:
        maintenance_task.cancel()
        try:
            await maintenance_task
        except asyncio.CancelledError:
            pass

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

TEMP_DOWNLOAD_DIR = os.path.join(current_dir, "temp_downloads")
os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)


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
                        async with state_lock:
                            active_file_map[file_uuid] = {
                                "filepath": downloaded_file,
                                "expires": time.time() + UNCLAIMED_FILE_EXPIRY
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
async def get_file(file_id: str, title: str, background_tasks: BackgroundTasks):
    validate_uuid(file_id)

    # Atomically consume the file entry to ensure strictly one-time retrieval
    async with state_lock:
        file_record = active_file_map.pop(file_id, None)
    if not file_record:
        raise HTTPException(status_code=404, detail="Asset expired, invalid, or already downloaded.")

    filepath = file_record["filepath"]

    # Explicitly check for expiry
    if time.time() >= file_record["expires"]:
        cleanup_file(filepath)
        raise HTTPException(status_code=404, detail="Asset has expired.")

    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Asset no longer exists on disk.")

    safe_name = sanitize_filename(title)
    if not safe_name:
        safe_name = "StreamVault_Download"

    ext = os.path.splitext(filepath)[1].lower()
    if not ext:
        ext = ".mp4"
    guessed_type, _ = mimetypes.guess_type(filepath)
    media_type = guessed_type or "application/octet-stream"

    response = FileResponse(
        path=filepath,
        media_type=media_type,
        filename=f"{safe_name}{ext}"
    )

    # Ensure the file is deleted from disk after it has been successfully streamed
    background_tasks.add_task(cleanup_file, filepath)

    return response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8300)
