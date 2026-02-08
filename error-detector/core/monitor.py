"""Core monitoring loop: polls SLURM, detects completion, triggers analysis + notification."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

# Add parent directory to path for imports
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from core.error_analyzer import ErrorAnalyzer
from core.notifier import DiscordNotifier
from core.state import StateManager

TERMINAL_STATES = {
    "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT",
    "OUT_OF_MEMORY", "NODE_FAIL", "PREEMPTED", "BOOT_FAIL",
    "DEADLINE", "REVOKED",
}

POLL_INTERVAL_RUNNING = 30
POLL_INTERVAL_PENDING = 60
SACCT_RETRY_COUNT = 5
SACCT_RETRY_DELAY = 10


def load_config() -> dict:
    config_path = BASE_DIR / "config.json"
    if not config_path.exists():
        print(f"[jobmon] ERROR: config.json not found at {config_path}", file=sys.stderr)
        sys.exit(1)
    with open(config_path) as f:
        return json.load(f)


def daemonize_and_monitor(job_id: str, error_file: str, output_file: str):
    """Double-fork to create a daemon process that monitors the job."""
    state = StateManager(job_id)

    # Check for duplicate monitor
    if state.exists_and_alive():
        print(f"[jobmon] Monitor already running for job {job_id}")
        return

    # First fork
    pid = os.fork()
    if pid > 0:
        # Parent: record the child info and return
        # Wait briefly for grandchild PID to be written
        time.sleep(0.5)
        return

    # Child: create new session
    os.setsid()

    # Second fork
    pid2 = os.fork()
    if pid2 > 0:
        # First child exits
        os._exit(0)

    # Grandchild: this is the daemon
    grandchild_pid = os.getpid()

    # Redirect stdout/stderr to log file
    log_dir = BASE_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"{job_id}.monitor.log"

    sys.stdout.flush()
    sys.stderr.flush()

    fd = os.open(str(log_file), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    os.dup2(fd, 1)  # stdout
    os.dup2(fd, 2)  # stderr
    os.close(fd)

    # Close stdin
    devnull = os.open(os.devnull, os.O_RDONLY)
    os.dup2(devnull, 0)
    os.close(devnull)

    # Initialize state
    state.initialize(grandchild_pid, error_file, output_file)

    print(f"[jobmon] Daemon started for job {job_id} (PID {grandchild_pid})")
    print(f"[jobmon] Error file: {error_file}")
    print(f"[jobmon] Output file: {output_file}")

    try:
        _monitor_loop(job_id, error_file, output_file, state)
    except Exception as e:
        print(f"[jobmon] Monitor crashed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        # Try to send a notification about the crash
        try:
            config = load_config()
            notifier = DiscordNotifier(
                config["discord"]["success_webhook"],
                config["discord"]["error_webhook"],
            )
            notifier.notify(
                job_id,
                {"state": "MONITOR_CRASH", "exit_code": "N/A", "elapsed": "N/A",
                 "job_name": "N/A", "max_rss": "N/A"},
                {"has_errors": True, "error_summary": f"Monitor process crashed: {e}",
                 "matched_patterns": ["Monitor Crash"], "relevant_lines": [str(e)], "tail": []},
            )
        except Exception:
            pass
    finally:
        os._exit(0)


def _monitor_loop(job_id: str, error_file: str, output_file: str, state: StateManager):
    """Main polling loop."""
    heartbeat_counter = 0

    while True:
        status = _check_squeue(job_id)

        if status is None:
            # Job no longer in queue — get final state from sacct
            print(f"[jobmon] Job {job_id} left the queue, querying sacct...")
            final = _check_sacct_with_retry(job_id)
            print(f"[jobmon] Final state: {final}")
            _handle_termination(job_id, final, error_file, output_file, state)
            return

        # Update heartbeat periodically
        heartbeat_counter += 1
        if heartbeat_counter % 2 == 0:  # Every other poll
            state.write_heartbeat()

        interval = POLL_INTERVAL_PENDING if status == "PENDING" else POLL_INTERVAL_RUNNING
        print(f"[jobmon] Job {job_id} status: {status}, next check in {interval}s")
        time.sleep(interval)


def _check_squeue(job_id: str) -> str | None:
    """Check if job is still in the queue. Returns state string or None."""
    try:
        result = subprocess.run(
            ["squeue", "-j", job_id, "-h", "-o", "%T"],
            capture_output=True, text=True, timeout=30,
        )
        output = result.stdout.strip()
        return output if output else None
    except subprocess.TimeoutExpired:
        return "UNKNOWN"  # Assume still running if squeue hangs


def _check_sacct_with_retry(job_id: str) -> dict:
    """Query sacct with retries (data may not be immediately available)."""
    for attempt in range(SACCT_RETRY_COUNT):
        result = _check_sacct(job_id)
        if result.get("state") != "UNKNOWN":
            return result
        print(f"[jobmon] sacct returned no data, retry {attempt + 1}/{SACCT_RETRY_COUNT}...")
        time.sleep(SACCT_RETRY_DELAY)
    return result  # Return whatever we got


def _check_sacct(job_id: str) -> dict:
    """Query sacct for final job state."""
    try:
        result = subprocess.run(
            ["sacct", "-j", job_id, "--parsable2", "--noheader",
             "-o", "JobID,State,ExitCode,Elapsed,MaxRSS,JobName"],
            capture_output=True, text=True, timeout=30,
        )
    except subprocess.TimeoutExpired:
        return {"job_id": job_id, "state": "UNKNOWN", "exit_code": "N/A",
                "elapsed": "N/A", "max_rss": "N/A", "job_name": "N/A"}

    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        fields = line.split("|")
        if len(fields) < 6:
            continue
        # Match the base job ID (not <jobid>.batch or <jobid>.0)
        if fields[0] == job_id:
            return {
                "job_id": fields[0],
                "state": fields[1],
                "exit_code": fields[2],
                "elapsed": fields[3],
                "max_rss": fields[4],
                "job_name": fields[5],
            }

    return {"job_id": job_id, "state": "UNKNOWN", "exit_code": "N/A",
            "elapsed": "N/A", "max_rss": "N/A", "job_name": "N/A"}


def _handle_termination(job_id: str, sacct_info: dict, error_file: str,
                         output_file: str, state: StateManager):
    """Analyze error file and send notification."""
    config = load_config()

    # Wait a bit for error file to be flushed
    time.sleep(5)

    # Analyze error file
    patterns_file = BASE_DIR / "patterns" / "error_patterns.json"
    analyzer = ErrorAnalyzer(str(patterns_file))
    analysis = analyzer.analyze(error_file)

    print(f"[jobmon] Analysis: has_errors={analysis['has_errors']}, "
          f"patterns={analysis['matched_patterns']}")

    # Send notification
    notifier = DiscordNotifier(
        config["discord"]["success_webhook"],
        config["discord"]["error_webhook"],
    )
    success = notifier.notify(job_id, sacct_info, analysis)
    print(f"[jobmon] Discord notification sent: {success}")

    # Update state
    state.mark_complete(sacct_info.get("state", "UNKNOWN"))
    print(f"[jobmon] Monitoring complete for job {job_id}")
