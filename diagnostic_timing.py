"""
diagnostic_timing.py - Lightweight Diagnostic Timing for StreamVault Mobile Downloads.

Instruments and tracks monotonic elapsed times across:
1. Mobile job submission received
2. Job queued
3. Worker starts processing
4. yt-dlp extraction begins and completes
5. Actual media download begins and completes
6. FFmpeg / post-processing begins and completes
7. Job becomes READY
8. Android requests completed file
9. Server finishes delivering the file
"""

import time
import threading
from typing import Optional, Dict, Any, List
from collections import OrderedDict


def _format_duration(duration_ms: float) -> str:
    safe_ms = max(0.0, duration_ms)
    total_seconds = int(safe_ms / 1000.0)
    hundredths = int((safe_ms % 1000.0) / 10.0)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{hundredths:02d}"
    return f"{minutes:02d}:{seconds:02d}.{hundredths:02d}"


def _format_bytes(num_bytes: Optional[int]) -> str:
    if num_bytes is None or num_bytes <= 0:
        return "Unknown size"
    mib = num_bytes / (1024.0 * 1024.0)
    return f"{mib:.2f} MiB ({num_bytes:,} bytes)"


class JobTimingRecord:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.created_mono: float = time.perf_counter()
        self.created_wall: float = time.time()

        # Monotonic timestamps
        self.t_submission_recv: float = self.created_mono
        self.t_queued: Optional[float] = None
        self.t_worker_start: Optional[float] = None
        self.t_extraction_start: Optional[float] = None
        self.t_extraction_end: Optional[float] = None
        self.t_media_download_start: Optional[float] = None
        self.t_media_download_end: Optional[float] = None
        self.t_postprocess_start: Optional[float] = None
        self.t_postprocess_end: Optional[float] = None
        self.t_ready: Optional[float] = None
        self.t_download_req: Optional[float] = None
        self.t_delivery_end: Optional[float] = None

        # Metadata
        self.url: Optional[str] = None
        self.file_id: Optional[str] = None
        self.file_size: Optional[int] = None
        self.title: Optional[str] = None
        self.delivery_mode: Optional[str] = None
        self.bytes_delivered: Optional[int] = None

    def elapsed_ms(self, t: Optional[float] = None) -> float:
        curr = t if t is not None else time.perf_counter()
        return round((curr - self.t_submission_recv) * 1000.0, 2)

    def stage_ms(self, start_t: Optional[float], end_t: Optional[float] = None) -> Optional[float]:
        if start_t is None:
            return None
        end = end_t if end_t is not None else time.perf_counter()
        return round((end - start_t) * 1000.0, 2)


class DiagnosticTimingTracker:
    def __init__(self, max_records: int = 500):
        self._lock = threading.Lock()
        self._records: OrderedDict[str, JobTimingRecord] = OrderedDict()
        self._max_records = max_records

    def get_or_create(self, job_id: str) -> JobTimingRecord:
        with self._lock:
            if job_id not in self._records:
                if len(self._records) >= self._max_records:
                    self._records.popitem(last=False)
                self._records[job_id] = JobTimingRecord(job_id)
            return self._records[job_id]

    def get(self, job_id: str) -> Optional[JobTimingRecord]:
        with self._lock:
            return self._records.get(job_id)

    def record_submission_received(self, job_id: str, url: str = "") -> JobTimingRecord:
        rec = self.get_or_create(job_id)
        rec.t_submission_recv = time.perf_counter()
        rec.url = url
        self._log(job_id, "SUBMISSION_RECEIVED", 0.0, f"url={url}")
        return rec

    def record_queued(self, job_id: str) -> None:
        rec = self.get_or_create(job_id)
        rec.t_queued = time.perf_counter()
        sub_to_queue_ms = rec.stage_ms(rec.t_submission_recv, rec.t_queued)
        self._log(job_id, "JOB_QUEUED", rec.elapsed_ms(rec.t_queued), f"queue_admit_ms={sub_to_queue_ms}")

    def record_worker_start(self, job_id: str) -> None:
        rec = self.get_or_create(job_id)
        rec.t_worker_start = time.perf_counter()
        queue_wait_ms = rec.stage_ms(rec.t_queued, rec.t_worker_start) if rec.t_queued else None
        details = f"queue_wait_ms={queue_wait_ms}" if queue_wait_ms is not None else "queue_wait_ms=0"
        self._log(job_id, "WORKER_START", rec.elapsed_ms(rec.t_worker_start), details)

    def record_extraction_start(self, job_id: Optional[str]) -> None:
        if not job_id:
            return
        rec = self.get_or_create(job_id)
        rec.t_extraction_start = time.perf_counter()
        self._log(job_id, "YTDLP_EXTRACTION_START", rec.elapsed_ms(rec.t_extraction_start))

    def record_extraction_end(self, job_id: Optional[str]) -> None:
        if not job_id:
            return
        rec = self.get_or_create(job_id)
        if rec.t_extraction_end is not None:
            return
        now = time.perf_counter()
        rec.t_extraction_end = now
        rec.t_media_download_start = now
        ext_ms = rec.stage_ms(rec.t_extraction_start, now) if rec.t_extraction_start else None
        self._log(job_id, "YTDLP_EXTRACTION_END", rec.elapsed_ms(now), f"extraction_duration_ms={ext_ms}")
        self._log(job_id, "MEDIA_DOWNLOAD_START", rec.elapsed_ms(now))

    def record_media_download_end(self, job_id: Optional[str]) -> None:
        if not job_id:
            return
        rec = self.get_or_create(job_id)
        if rec.t_media_download_end is not None:
            return
        now = time.perf_counter()
        rec.t_media_download_end = now
        dl_ms = rec.stage_ms(rec.t_media_download_start, now) if rec.t_media_download_start else None
        self._log(job_id, "MEDIA_DOWNLOAD_END", rec.elapsed_ms(now), f"media_download_duration_ms={dl_ms}")

    def record_postprocess_start(self, job_id: Optional[str]) -> None:
        if not job_id:
            return
        rec = self.get_or_create(job_id)
        if rec.t_media_download_end is None and rec.t_media_download_start is not None:
            now = time.perf_counter()
            rec.t_media_download_end = now
            dl_ms = rec.stage_ms(rec.t_media_download_start, now)
            self._log(job_id, "MEDIA_DOWNLOAD_END", rec.elapsed_ms(now), f"media_download_duration_ms={dl_ms}")
        if rec.t_postprocess_start is None:
            rec.t_postprocess_start = time.perf_counter()
            self._log(job_id, "POSTPROCESS_START", rec.elapsed_ms(rec.t_postprocess_start))

    def record_postprocess_end(self, job_id: Optional[str]) -> None:
        if not job_id:
            return
        rec = self.get_or_create(job_id)
        now = time.perf_counter()
        if rec.t_media_download_end is None and rec.t_media_download_start is not None:
            rec.t_media_download_end = now
        if rec.t_postprocess_start is not None and rec.t_postprocess_end is None:
            rec.t_postprocess_end = now
            pp_ms = rec.stage_ms(rec.t_postprocess_start, now)
            self._log(job_id, "POSTPROCESS_END", rec.elapsed_ms(now), f"postprocess_duration_ms={pp_ms}")

    def record_ready(self, job_id: str, file_id: str, file_size: int, title: Optional[str] = None) -> None:
        rec = self.get_or_create(job_id)
        now = time.perf_counter()
        rec.t_ready = now
        rec.file_id = file_id
        rec.file_size = file_size
        if title:
            rec.title = title
        self.record_postprocess_end(job_id)
        total_prep_ms = rec.stage_ms(rec.t_worker_start, now) if rec.t_worker_start else None
        self._log(
            job_id,
            "JOB_READY",
            rec.elapsed_ms(now),
            f"file_id={file_id} | size={_format_bytes(file_size)} | server_prep_ms={total_prep_ms}"
        )
        self._print_ready_summary(rec)

    def record_download_request(self, job_id: str, delivery_mode: str = "nginx") -> None:
        rec = self.get_or_create(job_id)
        rec.t_download_req = time.perf_counter()
        rec.delivery_mode = delivery_mode
        poll_delay_ms = rec.stage_ms(rec.t_ready, rec.t_download_req) if rec.t_ready else None
        self._log(
            job_id,
            "CLIENT_DOWNLOAD_REQUEST",
            rec.elapsed_ms(rec.t_download_req),
            f"mode={delivery_mode} | polling_discovery_delay_ms={poll_delay_ms}"
        )

    def record_delivery_end(self, job_id: Optional[str], bytes_delivered: Optional[int] = None) -> None:
        if not job_id:
            return
        rec = self.get(job_id)
        if not rec:
            return
        now = time.perf_counter()
        rec.t_delivery_end = now
        if bytes_delivered is not None:
            rec.bytes_delivered = bytes_delivered
        deliv_ms = rec.stage_ms(rec.t_download_req, now) if rec.t_download_req else None
        speed_mb_s = None
        delivered = bytes_delivered or rec.file_size or 0
        if deliv_ms and deliv_ms > 0 and delivered > 0:
            speed_mb_s = round((delivered / (1024.0 * 1024.0)) / (deliv_ms / 1000.0), 2)
        speed_info = f" | speed={speed_mb_s} MB/s" if speed_mb_s else ""
        self._log(job_id, "DELIVERY_COMPLETE", rec.elapsed_ms(now), f"transfer_time_ms={deliv_ms}{speed_info}")
        self._print_full_summary(rec)

    def _log(self, job_id: str, stage: str, elapsed_ms: float, details: str = "") -> None:
        extra = f" | {details}" if details else ""
        print(f"[TIMING:BACKEND] job_id={job_id} | stage={stage:<23} | elapsed_ms={elapsed_ms:8.2f}{extra}")

    def _print_ready_summary(self, rec: JobTimingRecord) -> None:
        queue_ms = rec.stage_ms(rec.t_queued, rec.t_worker_start) if (rec.t_queued and rec.t_worker_start) else 0.0
        ext_ms = rec.stage_ms(rec.t_extraction_start, rec.t_extraction_end) if (rec.t_extraction_start and rec.t_extraction_end) else 0.0
        dl_ms = rec.stage_ms(rec.t_media_download_start, rec.t_media_download_end) if (rec.t_media_download_start and rec.t_media_download_end) else 0.0
        pp_ms = rec.stage_ms(rec.t_postprocess_start, rec.t_postprocess_end) if (rec.t_postprocess_start and rec.t_postprocess_end) else 0.0
        ready_elapsed = rec.elapsed_ms(rec.t_ready)

        print("\n" + "=" * 78)
        print(f"[TIMING:BACKEND:SERVER_PREPARATION] Job READY for Client Retrieval")
        print("-" * 78)
        print(f"Job ID:            {rec.job_id}")
        print(f"File ID:           {rec.file_id}")
        print(f"Video Title:       {rec.title or 'Unknown'}")
        print(f"File Size:         {_format_bytes(rec.file_size)}")
        print("-" * 78)
        print(f"  1. Submission Received:       0.00 ms")
        print(f"  2. Job Queued:               +{rec.stage_ms(rec.t_submission_recv, rec.t_queued):.2f} ms  (elapsed: {rec.elapsed_ms(rec.t_queued):.2f} ms)")
        print(f"  3. Worker Started:           +{queue_ms:.2f} ms  (elapsed: {rec.elapsed_ms(rec.t_worker_start):.2f} ms) [Queue Wait]")
        print(f"  4. yt-dlp Extraction:        +{ext_ms:.2f} ms  [Metadata & Formats]")
        print(f"  5. Media Download:           +{dl_ms:.2f} ms  [Stream Download]")
        print(f"  6. FFmpeg / Post-process:    +{pp_ms:.2f} ms  [Assembly / Muxing]")
        print(f"  7. Job READY:                elapsed {ready_elapsed:.2f} ms ({_format_duration(ready_elapsed)})")
        print("=" * 78 + "\n")

    def _print_full_summary(self, rec: JobTimingRecord) -> None:
        queue_ms = rec.stage_ms(rec.t_queued, rec.t_worker_start) if (rec.t_queued and rec.t_worker_start) else 0.0
        ext_ms = rec.stage_ms(rec.t_extraction_start, rec.t_extraction_end) if (rec.t_extraction_start and rec.t_extraction_end) else 0.0
        dl_ms = rec.stage_ms(rec.t_media_download_start, rec.t_media_download_end) if (rec.t_media_download_start and rec.t_media_download_end) else 0.0
        pp_ms = rec.stage_ms(rec.t_postprocess_start, rec.t_postprocess_end) if (rec.t_postprocess_start and rec.t_postprocess_end) else 0.0
        prep_elapsed = rec.elapsed_ms(rec.t_ready) if rec.t_ready else 0.0

        poll_delay = rec.stage_ms(rec.t_ready, rec.t_download_req) if (rec.t_ready and rec.t_download_req) else 0.0
        deliv_ms = rec.stage_ms(rec.t_download_req, rec.t_delivery_end) if (rec.t_download_req and rec.t_delivery_end) else 0.0
        total_ms = rec.elapsed_ms(rec.t_delivery_end)

        delivered = rec.bytes_delivered or rec.file_size or 0
        speed_mb_s = round((delivered / (1024.0 * 1024.0)) / (deliv_ms / 1000.0), 2) if deliv_ms > 0 and delivered > 0 else 0.0

        print("\n" + "=" * 78)
        print(f"[TIMING:BACKEND:COMPLETE_SUMMARY] Full Job Lifecycle & Delivery")
        print("-" * 78)
        print(f"Job ID:            {rec.job_id}")
        print(f"File ID:           {rec.file_id}")
        print(f"Video Title:       {rec.title or 'Unknown'}")
        print(f"File Size:         {_format_bytes(rec.file_size)}")
        print(f"Delivery Mode:     {rec.delivery_mode or 'UNKNOWN'}")
        print("-" * 78)
        print(f"  1. Submission Received:       0.00 ms")
        print(f"  2. Job Queued:               +{rec.stage_ms(rec.t_submission_recv, rec.t_queued):.2f} ms  (elapsed: {rec.elapsed_ms(rec.t_queued):.2f} ms)")
        print(f"  3. Worker Started:           +{queue_ms:.2f} ms  (elapsed: {rec.elapsed_ms(rec.t_worker_start):.2f} ms) [Queue Wait]")
        print(f"  4. yt-dlp Extraction:        +{ext_ms:.2f} ms  [Metadata & Formats]")
        print(f"  5. Media Download:           +{dl_ms:.2f} ms  [Stream Download]")
        print(f"  6. FFmpeg / Post-process:    +{pp_ms:.2f} ms  [Assembly / Muxing]")
        print(f"  7. Job READY:                +{prep_elapsed:.2f} ms  (elapsed: {prep_elapsed:.2f} ms)")
        print(f"  8. Client Download Request:  +{poll_delay:.2f} ms  [Polling / Discovery Delay]")
        print(f"  9. Delivery Complete:        +{deliv_ms:.2f} ms  [Transfer Time | Speed: {speed_mb_s} MB/s]")
        print("-" * 78)
        print(f"TOTAL SERVER ELAPSED:          {total_ms:.2f} ms ({_format_duration(total_ms)})")
        print("=" * 78 + "\n")


# Global singleton instance
tracker = DiagnosticTimingTracker()
