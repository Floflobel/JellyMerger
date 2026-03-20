#!/usr/bin/env python3

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Optional
import requests
import re
import json
import time
import threading
from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from dotenv import load_dotenv
from croniter import croniter

# ==========================================
# CONFIGURATION
# ==========================================
load_dotenv()
JELLYFIN_URL = os.getenv("JELLYFIN_URL")
API_KEY = os.getenv("JELLYFIN_API_KEY")
USER_ID = os.getenv("JELLYFIN_USER_ID")  # Optionnal, None if empty

if not JELLYFIN_URL or not API_KEY:
    raise ValueError(
        "❌ ERROR: The JELLYFIN_URL and JELLYFIN_API_KEY environment variables are required."
    )

# clean url
JELLYFIN_URL = JELLYFIN_URL.rstrip("/")

app = FastAPI(title="JellyMerger")
templates = Jinja2Templates(directory="templates")

# ==========================================
# MODELS
# ==========================================


class FileItem(BaseModel):
    name: str
    quality: str
    path: str


class MergeGroup(BaseModel):
    label: str
    files: List[FileItem]
    ids: List[str]


class MergeRequest(BaseModel):
    groups: List[MergeGroup]


class ScanConfig(BaseModel):
    delay_seconds: float = 0.1
    max_concurrent: int = 5


class ScheduleConfig(BaseModel):
    enabled: bool = False
    cron_expression: str = "0 3 * * *"
    auto_merge: bool = False
    scan_config: ScanConfig = ScanConfig()


# ==========================================
# JELLYFIN MANAGER
# ==========================================


class JellyfinManager:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Emby-Token": API_KEY,
                "Authorization": f'MediaBrowser Client="FastAPI Merger", Device="Web", Version="1.0.0", Token="{API_KEY}"',
                "Content-Type": "application/json",
            }
        )

    def _get(self, endpoint, params=None):
        if params is None:
            params = {}
        if USER_ID:
            params["UserId"] = USER_ID
        try:
            r = self.session.get(f"{JELLYFIN_URL}{endpoint}", params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"Error API: {e}")
            return {}

    def search_series(self, query: str):
        params = {
            "Recursive": "true",
            "IncludeItemTypes": "Series",
            "SearchTerm": query,
            "Fields": "Name,Id,ProductionYear",
        }
        data = self._get("/Items", params)
        return data.get("Items", [])

    def get_all_series(self):
        params = {
            "Recursive": "true",
            "IncludeItemTypes": "Series",
            "Fields": "Name,Id,ProductionYear",
            "SortBy": "SortName",
            "SortOrder": "Ascending",
        }
        data = self._get("/Items", params)
        return data.get("Items", [])

    def get_all_episodes_recursive(self, series_id: str):
        params = {
            "Recursive": "true",
            "ParentId": series_id,
            "IncludeItemTypes": "Episode",
            "Fields": "ParentIndexNumber,IndexNumber,Path,Name,MediaSources",
        }
        data = self._get("/Items", params)
        return data.get("Items", [])

    def merge_versions(self, ids: List[str]):
        endpoint = f"{JELLYFIN_URL}/Videos/MergeVersions"
        ids_param = ",".join(ids)
        params = {"Ids": ids_param}
        try:
            r = self.session.post(endpoint, params=params, timeout=30)
            r.raise_for_status()
            return True
        except Exception as e:
            print(f"Error Merge: {e}")
            return False


manager = JellyfinManager()

# ==========================================
# HELPERS
# ==========================================


def extract_episode_info(item):
    s = item.get("ParentIndexNumber")
    e = item.get("IndexNumber")
    if s is None or e is None:
        path = item.get("Path", "")
        match = re.search(r"[sS](\d+)[eE](\d+)", path)
        if match:
            s = int(match.group(1))
            e = int(match.group(2))
    return s, e


def normalize_path(path):
    if not path:
        return ""
    return path.replace("\\", "/").strip()


def analyze_series_duplicates(series_id: str) -> dict:
    """Reusable duplicate-detection logic for a single series."""
    episodes = manager.get_all_episodes_recursive(series_id)
    grouped = defaultdict(list)

    for ep in episodes:
        s, e = extract_episode_info(ep)
        if s is not None and e is not None:
            key = f"S{s:02d}E{e:02d}"
            grouped[key].append(ep)

    candidates = []

    for key, items in grouped.items():
        if len(items) < 2:
            continue

        # --- Detection Already Merged ---
        detected_paths = set()
        for item in items:
            if item.get("Path"):
                detected_paths.add(normalize_path(item["Path"]))

        already_merged = False
        for item in items:
            sources = item.get("MediaSources", [])
            if len(sources) > 1:
                known_paths = set()
                for src in sources:
                    if src.get("Path"):
                        known_paths.add(normalize_path(src["Path"]))

                if detected_paths.issubset(known_paths):
                    already_merged = True
                    break

        if already_merged:
            continue
        # --------------------------------

        # --- Skip groups of distinct episodes mis-indexed under the same key ---
        # True duplicates share the same episode. Detect mis-indexed groups by
        # checking both Jellyfin episode names AND numbers extracted from filenames.
        ep_names = set(item.get("Name", "") for item in items)
        if len(ep_names) > 1 and len(ep_names) == len(items):
            continue

        # Also check: if filenames contain different episode numbers, these are
        # distinct episodes that Jellyfin grouped under the same index.
        file_numbers = set()
        for item in items:
            path = item.get("Path", "")
            fname = path.split("/")[-1].split("\\")[-1]
            # Match common patterns: " - 29 -", "E03", "e03", " 03 ", "ep03"
            m = re.search(r"[\s\-._](?:E|e|ep|EP)?(\d{1,4})[\s\-._]", fname)
            if m:
                file_numbers.add(m.group(1).lstrip("0") or "0")
        if len(file_numbers) > 1:
            # Filenames reference different episode numbers — not real duplicates
            continue
        # -------------------------------------------------------------------

        files = []
        ids = []
        for item in items:
            path = item.get("Path", "Inconnu")
            filename = path.split("/")[-1].split("\\")[-1]
            quality = (
                "4K/HDR"
                if any(x in filename for x in ["2160", "4K", "HDR", "DV"])
                else "1080p"
                if "1080" in filename
                else "SD"
            )

            files.append({"name": filename, "quality": quality, "path": path})
            ids.append(item["Id"])

        candidates.append({"label": key, "files": files, "ids": ids})

    candidates.sort(key=lambda x: x["label"])

    return {"count": len(candidates), "results": candidates}


# ==========================================
# SCAN STATE
# ==========================================


class ScanState:
    def __init__(self):
        self._lock = threading.Lock()
        self.status = "idle"  # idle | running | completed | error | cancelled
        self.current = 0
        self.total = 0
        self.current_series_name = ""
        self.results = []  # list of {series_id, series_name, year, candidates}
        self.started_at = None
        self.completed_at = None
        self.error = None
        self.config = None
        self._cancel_requested = False

    def request_cancel(self):
        with self._lock:
            self._cancel_requested = True

    def is_cancelled(self):
        with self._lock:
            return self._cancel_requested

    def reset(self, total: int, config: ScanConfig):
        with self._lock:
            self.status = "running"
            self.current = 0
            self.total = total
            self.current_series_name = ""
            self.results = []
            self.started_at = datetime.now(timezone.utc).isoformat()
            self.completed_at = None
            self.error = None
            self.config = config.model_dump()
            self._cancel_requested = False

    def update_progress(self, current: int, series_name: str):
        with self._lock:
            self.current = current
            self.current_series_name = series_name

    def set_active_series(self, active_names: list):
        """For concurrent scans: set all currently active series names."""
        with self._lock:
            self.current_series_name = ", ".join(active_names)

    def add_result(self, series_id: str, series_name: str, year, candidates: list):
        with self._lock:
            self.results.append(
                {
                    "series_id": series_id,
                    "series_name": series_name,
                    "year": year,
                    "candidates": candidates,
                }
            )

    def complete(self, status: str = "completed", error: str = None):
        with self._lock:
            self.status = status
            self.completed_at = datetime.now(timezone.utc).isoformat()
            self.error = error

    def to_dict(self, include_results: bool = True) -> dict:
        with self._lock:
            d = {
                "status": self.status,
                "progress": {
                    "current": self.current,
                    "total": self.total,
                    "current_series_name": self.current_series_name,
                },
                "started_at": self.started_at,
                "completed_at": self.completed_at,
                "error": self.error,
                "config": self.config,
                "result_count": sum(len(r["candidates"]) for r in self.results),
                "series_with_duplicates": len(self.results),
            }
            if include_results:
                d["results"] = list(self.results)
            return d


scan_state = ScanState()

# ==========================================
# SCHEDULE STATE
# ==========================================


class ScheduleState:
    def __init__(self):
        self._lock = threading.Lock()
        self.enabled = False
        self.cron_expression = "0 3 * * *"
        self.auto_merge = False
        self.scan_config = ScanConfig()
        self.next_run_at = None
        self._timer = None

    def get_config(self) -> dict:
        with self._lock:
            return {
                "enabled": self.enabled,
                "cron_expression": self.cron_expression,
                "auto_merge": self.auto_merge,
                "scan_config": self.scan_config.model_dump(),
                "next_run_at": self.next_run_at,
            }

    def update(self, config: ScheduleConfig, run_scan_fn):
        with self._lock:
            # Cancel existing timer
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
                self.next_run_at = None

            self.enabled = config.enabled
            self.cron_expression = config.cron_expression
            self.auto_merge = config.auto_merge
            self.scan_config = config.scan_config

            if self.enabled:
                self._schedule_next_locked(run_scan_fn)

    def _schedule_next_locked(self, run_scan_fn):
        """Must be called while holding self._lock."""
        now = datetime.now(timezone.utc)
        cron = croniter(self.cron_expression, now)
        next_dt = cron.get_next(datetime)
        self.next_run_at = next_dt.isoformat()
        delay = (next_dt - now).total_seconds()

        def _on_fire():
            run_scan_fn(self.scan_config, scheduled=True, auto_merge=self.auto_merge)
            with self._lock:
                if self.enabled:
                    self._schedule_next_locked(run_scan_fn)

        self._timer = threading.Timer(delay, _on_fire)
        self._timer.daemon = True
        self._timer.start()

    def disable(self):
        with self._lock:
            self.enabled = False
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            self.next_run_at = None


schedule_state = ScheduleState()

# ==========================================
# SCAN WORKER
# ==========================================


def run_full_scan(
    config: ScanConfig, scheduled: bool = False, auto_merge: bool = False
):
    """Run a full library scan in the current thread.

    Expects scan_state to already be set to 'running' by the caller.
    """
    all_series_raw = manager.get_all_series()
    # Deduplicate series by ID and by (Name, Year) — same series can appear
    # in multiple libraries with different IDs
    seen_ids = set()
    seen_names = set()
    all_series = []
    for s in all_series_raw:
        key = (s.get("Name", ""), s.get("ProductionYear"))
        if s["Id"] not in seen_ids and key not in seen_names:
            seen_ids.add(s["Id"])
            seen_names.add(key)
            all_series.append(s)
    with scan_state._lock:
        scan_state.total = len(all_series)

    try:
        if config.max_concurrent <= 1:
            # Sequential scan
            for i, series in enumerate(all_series):
                if scan_state.is_cancelled():
                    scan_state.complete(status="cancelled")
                    return

                name = series.get("Name", "Unknown")
                scan_state.update_progress(i + 1, name)

                try:
                    result = analyze_series_duplicates(series["Id"])
                    if result["count"] > 0:
                        scan_state.add_result(
                            series_id=series["Id"],
                            series_name=name,
                            year=series.get("ProductionYear"),
                            candidates=result["results"],
                        )
                except Exception as e:
                    print(f"Error scanning series {name}: {e}")

                if config.delay_seconds > 0 and i < len(all_series) - 1:
                    time.sleep(config.delay_seconds)
        else:
            # Concurrent scan with batching
            completed_count = 0
            active_names = []
            active_lock = threading.Lock()

            def _scan_one(series):
                name = series.get("Name", "Unknown")
                with active_lock:
                    active_names.append(name)
                    scan_state.set_active_series(list(active_names))
                try:
                    return analyze_series_duplicates(series["Id"])
                finally:
                    with active_lock:
                        if name in active_names:
                            active_names.remove(name)

            with ThreadPoolExecutor(max_workers=config.max_concurrent) as executor:
                futures = {}
                for series in all_series:
                    if scan_state.is_cancelled():
                        break
                    future = executor.submit(_scan_one, series)
                    futures[future] = series

                for future in as_completed(futures):
                    if scan_state.is_cancelled():
                        scan_state.complete(status="cancelled")
                        return

                    series = futures[future]
                    name = series.get("Name", "Unknown")
                    completed_count += 1

                    with active_lock:
                        scan_state.update_progress(
                            completed_count,
                            ", ".join(active_names) if active_names else name,
                        )

                    try:
                        result = future.result()
                        if result["count"] > 0:
                            scan_state.add_result(
                                series_id=series["Id"],
                                series_name=name,
                                year=series.get("ProductionYear"),
                                candidates=result["results"],
                            )
                    except Exception as e:
                        print(f"Error scanning series {name}: {e}")

        if scan_state.is_cancelled():
            scan_state.complete(status="cancelled")
        else:
            scan_state.complete(status="completed")

            # Auto-merge if enabled
            if auto_merge:
                state_dict = scan_state.to_dict()
                for series_result in state_dict.get("results", []):
                    for candidate in series_result.get("candidates", []):
                        try:
                            manager.merge_versions(candidate["ids"])
                        except Exception as e:
                            print(f"Auto-merge error: {e}")

    except Exception as e:
        scan_state.complete(status="error", error=str(e))


def start_scan_thread(
    config: ScanConfig, scheduled: bool = False, auto_merge: bool = False
):
    # Ensure running state is set before thread starts (for scheduled scans)
    if scan_state.status != "running":
        scan_state.reset(total=0, config=config)
    t = threading.Thread(
        target=run_full_scan,
        args=(config,),
        kwargs={"scheduled": scheduled, "auto_merge": auto_merge},
        daemon=True,
    )
    t.start()


# ==========================================
# API ROUTES
# ==========================================


@app.get("/", response_class=HTMLResponse)
def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/search")
def search(q: str = ""):
    if len(q) < 2:
        return []
    return manager.search_series(q)


@app.get("/api/analyze/{series_id}")
def analyze(series_id: str):
    return analyze_series_duplicates(series_id)


@app.post("/api/merge")
def run_merge(request: MergeRequest):
    success = 0
    errors = 0

    for group in request.groups:
        if manager.merge_versions(group.ids):
            success += 1
        else:
            errors += 1

    return {"success": success, "errors": errors}


# ==========================================
# SCAN ENDPOINTS
# ==========================================


@app.post("/api/scan/start")
def scan_start(config: ScanConfig = ScanConfig()):
    if scan_state.status == "running":
        raise HTTPException(status_code=409, detail="A scan is already running")
    # Set running state immediately so /api/scan/status reflects it before thread starts
    scan_state.reset(total=0, config=config)
    start_scan_thread(config)
    return {"status": "started"}


@app.get("/api/scan/progress")
def scan_progress():
    def event_stream():
        while True:
            data = scan_state.to_dict(include_results=False)

            if data["status"] in ("completed", "error", "cancelled"):
                # Send final event with full results
                full = scan_state.to_dict(include_results=True)
                yield f"data: {json.dumps(full)}\n\n"
                break

            yield f"data: {json.dumps(data)}\n\n"
            time.sleep(1)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/api/scan/status")
def scan_status():
    return scan_state.to_dict(include_results=True)


@app.post("/api/scan/cancel")
def scan_cancel():
    if scan_state.status != "running":
        raise HTTPException(status_code=400, detail="No scan is currently running")
    scan_state.request_cancel()
    return {"status": "cancel_requested"}


# ==========================================
# SCHEDULE ENDPOINTS
# ==========================================


@app.get("/api/schedule")
def get_schedule():
    return schedule_state.get_config()


@app.post("/api/schedule")
def set_schedule(config: ScheduleConfig):
    if config.enabled:
        if not croniter.is_valid(config.cron_expression):
            raise HTTPException(status_code=400, detail="Invalid cron expression")
    schedule_state.update(config, start_scan_thread)
    return schedule_state.get_config()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
