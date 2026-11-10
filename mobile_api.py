"""
mobile_api.py - StreamVault Mobile API

Provides endpoints for mobile clients (Android):
- POST /api/mobile/submit
- GET /api/mobile/status/{job_id}
- GET /api/mobile/jobs
- POST /api/mobile/cancel/{job_id}
- GET /api/mobile/download/{job_id}

Features:
- Decoupled background execution (persists even if Android disconnects)
- Shared download concurrency (max 3 concurrent with web downloads)
- Mobile queue capacity management (max 10 queued/active mobile jobs)
- Reuses existing URL validation, download engine, and file delivery
- No authentication for closed mobile testing
"""

import asyncio
import os
import threading
import uuid
from typing import Any, Dict, Optional, Set
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

import job_manager
import main
import diagnostic_timing

router = APIRouter(prefix="/api/mobile", tags=["mobile"])

MAX_MOBILE_QUEUED_ACTIVE = 10

# Mobile video format policy:
# Enforces a maximum resolution of 1080p exclusively for mobile API downloads.
# Selects the best available video stream (height <= 1080) paired with the best
# audio stream, or the best pre-merged stream (height <= 1080), falling back to
# the best available lower resolution (720p, 480p, etc.) when 1080p is unavailable.
MOBILE_MAX_HEIGHT = 1080
MOBILE_FORMAT_SELECTOR = (
    f"bestvideo[height<=?{MOBILE_MAX_HEIGHT}]+bestaudio/"
    f"best[height<=?{MOBILE_MAX_HEIGHT}]/"
    f"bestvideo[height<=?{MOBILE_MAX_HEIGHT}]"
)


def get_mobile_format_selector() -> str:
    """Returns the format selector enforcing a maximum height of 1080p for mobile downloads."""
    return MOBILE_FORMAT_SELECTOR

# Strong references to running mobile asyncio tasks
active_mobile_tasks: Set[asyncio.Task] = set()

# Live mobile task tracking: job_id -> {"cancel_event": threading.Event, "file_uuid": str, "task": asyncio.Task}
_active_mobile_jobs: Dict[str, Dict[str, Any]] = {}
_mobile_lock = asyncio.Lock()


class SubmitRequest(BaseModel):
    url: str


def reset_in_memory_state() -> None:
    """Ensures in-memory mobile queue/task tracking starts empty."""
    _active_mobile_jobs.clear()
    active_mobile_tasks.clear()


def schedule_mobile_queue() -> None:
    """Schedules the queue runner on the running asyncio loop."""
    try:
        loop = asyncio.get_running_loop()
        task = loop.create_task(process_mobile_queue())
        active_mobile_tasks.add(task)
        task.add_done_callback(active_mobile_tasks.discard)
    except RuntimeError:
        pass


async def process_mobile_queue() -> None:
    """Picks up queued mobile jobs in FIFO order if download capacity is available."""
    async with _mobile_lock:
        while True:
            # Check available slots against shared MAX_CONCURRENT_DOWNLOADS
            async with main.state_lock:
                mobile_file_uuids = {
                    rec["file_uuid"] for rec in _active_mobile_jobs.values() if "file_uuid" in rec
                }
                web_active_downloads = sum(
                    1 for fid in main.active_downloads if fid not in mobile_file_uuids
                )
                total_in_flight = web_active_downloads + len(_active_mobile_jobs)
                available_slots = main.MAX_CONCURRENT_DOWNLOADS - total_in_flight

            if available_slots <= 0:
                break

            # Atomically claim the oldest queued job in FIFO order
            job = job_manager.claim_next_queued_job()
            if not job:
                break

            job_id = job["job_id"]
            url = job["url"]

            file_uuid = str(uuid.uuid4())
            cancel_event = threading.Event()

            # Pre-register in memory synchronously before creating task to prevent race conditions
            _active_mobile_jobs[job_id] = {
                "cancel_event": cancel_event,
                "file_uuid": file_uuid,
                "task": None,
            }

            try:
                task = asyncio.create_task(_execute_mobile_job(job_id, url, file_uuid, cancel_event))
                _active_mobile_jobs[job_id]["task"] = task
                active_mobile_tasks.add(task)
                task.add_done_callback(active_mobile_tasks.discard)
            except Exception as task_err:
                _active_mobile_jobs.pop(job_id, None)
                job_manager.unclaim_job(job_id)
                print(f"Error creating mobile task for {job_id}: {task_err}")
                break


async def _execute_mobile_job(
    job_id: str,
    url: str,
    file_uuid: Optional[str] = None,
    cancel_event: Optional[threading.Event] = None,
) -> None:
    """Executes download and assembly for a mobile job via the shared core engine."""
    if file_uuid is None:
        file_uuid = str(uuid.uuid4())
    if cancel_event is None:
        cancel_event = threading.Event()

    # Pre-register in memory if not already present
    if job_id not in _active_mobile_jobs:
        _active_mobile_jobs[job_id] = {
            "cancel_event": cancel_event,
            "file_uuid": file_uuid,
            "task": asyncio.current_task(),
        }
    try:
        diagnostic_timing.tracker.record_worker_start(job_id)
        if cancel_event.is_set():
            job_manager.update_job(job_id, status="cancelled")
            return

        # Transition lifecycle state to downloading
        job_manager.update_job(job_id, status="downloading")

        last_status = "downloading"
        captured_title_set = False

        def mobile_progress_callback(payload: dict):
            nonlocal last_status, captured_title_set

            if not captured_title_set:
                t = payload.get("title")
                if t:
                    job_manager.update_job(job_id, title=t)
                    captured_title_set = True

            percent = payload.get("percent", 0.0)
            speed = payload.get("speed")
            eta = payload.get("eta")

            # Determine genuine state transition (only pass status on actual state changes)
            target_status = "processing" if percent >= 100.0 else "downloading"
            transition_status = None
            if target_status != last_status:
                transition_status = target_status
                last_status = target_status

            job_manager.update_progress(
                job_id=job_id,
                progress=percent,
                speed=speed,
                eta=eta,
                status=transition_status
            )

        # Execute download via shared core engine enforcing mobile max 1080p policy
        exec_kwargs = {
            "file_uuid": file_uuid,
            "url": url,
            "format_id": MOBILE_FORMAT_SELECTOR,
            "cancel_event": cancel_event,
            "progress_callback": mobile_progress_callback,
        }
        import inspect
        sig = inspect.signature(main.execute_download_core)
        if "job_id" in sig.parameters or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
            exec_kwargs["job_id"] = job_id

        result = await main.execute_download_core(**exec_kwargs)

        res_status = result.get("status")
        if res_status == "completed":
            async with main.state_lock:
                if file_uuid in main.active_file_map:
                    main.active_file_map[file_uuid]["job_id"] = job_id

            final_title = result.get("title")
            actual_size = result.get("size", 0)
            job_manager.update_job(
                job_id=job_id,
                status="ready",
                file_id=file_uuid,
                progress=100.0,
                title=final_title
            )
            diagnostic_timing.tracker.record_ready(
                job_id=job_id,
                file_id=file_uuid,
                file_size=actual_size,
                title=final_title or (job_manager.get_job(job_id) or {}).get("title")
            )
        elif res_status == "cancelled":
            job_manager.update_job(
                job_id=job_id,
                status="cancelled",
                error_message="Download Cancelled."
            )
        else:
            job_manager.update_job(
                job_id=job_id,
                status="failed",
                error_message=result.get("error", "Download failed.")
            )

    except asyncio.CancelledError:
        cancel_event.set()
        job_manager.update_job(job_id, status="cancelled")
        raise
    except Exception as e:
        print(f"Unexpected error in mobile job {job_id}: {main.clean_terminal_ansi(str(e))}")
        job_manager.update_job(
            job_id,
            status="failed",
            error_message="Download failed due to an internal error."
        )
    finally:
        _active_mobile_jobs.pop(job_id, None)
        # Concurrency slot freed; schedule next queued job
        schedule_mobile_queue()


@router.post("/submit", status_code=201)
async def submit_job(payload: SubmitRequest):
    """
    Submits a video URL for download:
    - Runs validate_url_safety().
    - Validates mobile queue capacity (max 10 queued/active).
    - Creates job in SQLite with status 'queued'.
    - Starts or queues background execution.
    - Returns job_id and queued status immediately.
    """
    url = (payload.url or "").strip()
    try:
        await main.validate_url_safety(url)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    active_count = job_manager.get_active_job_count()
    if active_count >= MAX_MOBILE_QUEUED_ACTIVE:
        raise HTTPException(
            status_code=429,
            detail=f"Mobile queue limit reached. Maximum {MAX_MOBILE_QUEUED_ACTIVE} active/queued jobs allowed."
        )

    job_id = str(uuid.uuid4())
    diagnostic_timing.tracker.record_submission_received(job_id, url)
    job = job_manager.create_job(job_id=job_id, url=url, status="queued")
    diagnostic_timing.tracker.record_queued(job_id)

    schedule_mobile_queue()

    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "url": job["url"],
    }


@router.get("/status/{job_id}")
async def get_job_status(job_id: str):
    """Returns persisted job state and real-time live progress."""
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/jobs")
async def list_mobile_jobs():
    """Returns the latest 50 mobile jobs."""
    return job_manager.list_jobs(limit=50)


@router.post("/cancel/{job_id}")
async def cancel_mobile_job(job_id: str):
    """Signals cancellation to an active/queued mobile job and updates status safely."""
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job["status"] in ("completed", "failed", "cancelled"):
        return {
            "job_id": job_id,
            "status": job["status"],
            "message": f"Job is already {job['status']}."
        }

    # Signal active cancellation if currently running
    active_rec = _active_mobile_jobs.get(job_id)
    if active_rec:
        active_rec["cancel_event"].set()
        task = active_rec.get("task")
        if task and not task.done():
            task.cancel()

    job_manager.update_job(job_id, status="cancelled")

    # Trigger queue processor to immediately schedule the next queued job
    schedule_mobile_queue()

    return {
        "job_id": job_id,
        "status": "cancelled",
        "message": "Job cancelled successfully."
    }


@router.get("/download/{job_id}")
async def download_mobile_job(request: Request, job_id: str):
    """
    Delivers the completed video asset to the mobile client:
    - Resolves job_id to file_id.
    - Reuses existing deliver_file_response() implementation.
    - Preserves X-Accel, Range handling, and completion accounting.
    """
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    status = job.get("status")
    if status in ("failed", "cancelled"):
        raise HTTPException(status_code=400, detail=f"Cannot download job in '{status}' state.")

    if status in ("queued", "extracting", "downloading", "processing"):
        raise HTTPException(status_code=409, detail="Job is not ready for download yet.")

    file_id = job.get("file_id")
    if not file_id:
        raise HTTPException(status_code=404, detail="Asset not found for this job.")

    async with main.state_lock:
        if file_id not in main.active_file_map:
            raise HTTPException(status_code=404, detail="Asset expired, invalid, or already downloaded.")

    job_manager.update_job(job_id, status="delivering")

    delivery_mode = "nginx" if main.USE_X_ACCEL_REDIRECT else "python"
    diagnostic_timing.tracker.record_download_request(job_id, delivery_mode=delivery_mode)

    title = job.get("title") or "StreamVault_Download"
    return await main.deliver_file_response(request, file_id, title, job_id=job_id)


async def shutdown_mobile_tasks() -> None:
    """Cancels and awaits all active mobile tasks during application shutdown."""
    for active_rec in list(_active_mobile_jobs.values()):
        active_rec["cancel_event"].set()
        task = active_rec.get("task")
        if task and not task.done():
            task.cancel()

    if active_mobile_tasks:
        try:
            await asyncio.wait_for(
                asyncio.gather(*list(active_mobile_tasks), return_exceptions=True),
                timeout=5.0
            )
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
