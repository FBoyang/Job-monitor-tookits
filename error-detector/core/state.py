"""Manages per-job state files for monitoring tracking."""

import json
import os
import time
from pathlib import Path

STATE_DIR = Path(__file__).resolve().parent.parent / "state"


class StateManager:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.path = STATE_DIR / f"{job_id}.json"
        STATE_DIR.mkdir(exist_ok=True)

    def initialize(self, pid: int, error_file: str, output_file: str):
        data = {
            "job_id": self.job_id,
            "monitor_pid": pid,
            "error_file": error_file,
            "output_file": output_file,
            "started_at": time.time(),
            "heartbeat": time.time(),
            "status": "monitoring",
        }
        self.path.write_text(json.dumps(data, indent=2))

    def write_heartbeat(self):
        if not self.path.exists():
            return
        data = json.loads(self.path.read_text())
        data["heartbeat"] = time.time()
        self.path.write_text(json.dumps(data, indent=2))

    def mark_complete(self, final_state: str):
        if not self.path.exists():
            return
        data = json.loads(self.path.read_text())
        data["status"] = "complete"
        data["final_state"] = final_state
        data["completed_at"] = time.time()
        self.path.write_text(json.dumps(data, indent=2))

    def exists_and_alive(self) -> bool:
        """Check if a monitor for this job is already running."""
        if not self.path.exists():
            return False
        try:
            data = json.loads(self.path.read_text())
        except (json.JSONDecodeError, OSError):
            return False
        if data.get("status") != "monitoring":
            return False
        pid = data.get("monitor_pid")
        if pid is None:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    @classmethod
    def list_all(cls) -> list:
        """List all state files with alive-check."""
        STATE_DIR.mkdir(exist_ok=True)
        results = []
        for f in sorted(STATE_DIR.glob("*.json")):
            try:
                data = json.loads(f.read_text())
            except (json.JSONDecodeError, OSError):
                continue

            if data.get("status") == "monitoring":
                pid = data.get("monitor_pid")
                try:
                    os.kill(pid, 0)
                    data["monitor_alive"] = True
                except (OSError, TypeError):
                    data["monitor_alive"] = False
            else:
                data["monitor_alive"] = False

            results.append(data)
        return results

    @classmethod
    def list_recoverable(cls) -> list:
        """List jobs that are still in 'monitoring' state but whose monitor is dead."""
        STATE_DIR.mkdir(exist_ok=True)
        results = []
        for f in sorted(STATE_DIR.glob("*.json")):
            try:
                data = json.loads(f.read_text())
            except (json.JSONDecodeError, OSError):
                continue
            if data.get("status") != "monitoring":
                continue
            pid = data.get("monitor_pid")
            try:
                os.kill(pid, 0)
                # Monitor still alive, skip
            except (OSError, TypeError):
                results.append(data)
        return results
