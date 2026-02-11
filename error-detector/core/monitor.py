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
from core.submitter import _substitute_slurm_vars

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


def daemonize_and_monitor(job_id: str, error_file: str, output_file: str,
                          job_name: str = "", is_array: bool = False):
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

    # Line-buffered so crash output is not lost
    sys.stdout = os.fdopen(1, "w", buffering=1)
    sys.stderr = os.fdopen(2, "w", buffering=1)

    # Close stdin
    devnull = os.open(os.devnull, os.O_RDONLY)
    os.dup2(devnull, 0)
    os.close(devnull)

    # Initialize state
    state.initialize(grandchild_pid, error_file, output_file, job_name=job_name, is_array=is_array)

    print(f"[jobmon] Daemon started for job {job_id} (PID {grandchild_pid})")
    print(f"[jobmon] Error file: {error_file}")
    print(f"[jobmon] Output file: {output_file}")

    try:
        _monitor_loop(job_id, error_file, output_file, state, job_name, is_array)
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


def _monitor_loop(job_id: str, error_file: str, output_file: str, state: StateManager,
                  job_name: str = "", is_array: bool = False):
    """Main polling loop."""
    heartbeat_counter = 0
    # Track which array sub-tasks have already been reported as failed
    reported_failures = set()

    while True:
        status = _check_squeue(job_id)

        if status is None:
            # Job no longer in queue — get final state from sacct
            print(f"[jobmon] Job {job_id} left the queue, querying sacct...")
            sacct_results = _check_sacct_all_with_retry(job_id)
            print(f"[jobmon] Final state(s): {sacct_results}")
            _handle_termination(job_id, sacct_results, error_file, output_file, state,
                               job_name, is_array,
                               already_reported=reported_failures)
            return

        # For array jobs that are running, check sacct for early failures
        if is_array and status == "RUNNING":
            reported_failures = _check_early_array_failures(
                job_id, error_file, job_name, reported_failures)

        # Update heartbeat periodically
        heartbeat_counter += 1
        if heartbeat_counter % 2 == 0:  # Every other poll
            state.write_heartbeat()

        interval = POLL_INTERVAL_PENDING if status == "PENDING" else POLL_INTERVAL_RUNNING
        print(f"[jobmon] Job {job_id} status: {status}, next check in {interval}s")
        time.sleep(interval)


def _check_squeue(job_id: str) -> str | None:
    """Check if job is still in the queue. Returns state string or None.
    For array jobs, summarizes multi-line output (RUNNING if any task running, etc.).
    """
    try:
        result = subprocess.run(
            ["squeue", "-j", job_id, "-h", "-o", "%T"],
            capture_output=True, text=True, timeout=30,
        )
        output = result.stdout.strip()
        if not output:
            return None
        lines = [l.strip() for l in output.splitlines() if l.strip()]
        if not lines:
            return None
        if any(s == "RUNNING" for s in lines):
            return "RUNNING"
        if any(s == "COMPLETING" for s in lines):
            return "RUNNING"
        if all(s == "PENDING" for s in lines):
            return "PENDING"
        return lines[0]
    except subprocess.TimeoutExpired:
        return "UNKNOWN"  # Assume still running if squeue hangs


def _check_sacct_all_with_retry(job_id: str) -> list:
    """Query sacct with retries. Returns list of dicts (one per sub-task or single for non-array)."""
    unknown_fallback = [{"job_id": job_id, "state": "UNKNOWN", "exit_code": "N/A",
                         "elapsed": "N/A", "max_rss": "N/A", "job_name": "N/A"}]
    for attempt in range(SACCT_RETRY_COUNT):
        results = _check_sacct_all(job_id)
        if not results:
            results = unknown_fallback
        if results[0].get("state") != "UNKNOWN":
            return results
        print(f"[jobmon] sacct returned no data, retry {attempt + 1}/{SACCT_RETRY_COUNT}...")
        time.sleep(SACCT_RETRY_DELAY)
    return results


def _check_sacct_all(job_id: str) -> list:
    """Query sacct for final job state(s). Returns list of dicts (one per sub-task or single)."""
    try:
        result = subprocess.run(
            ["sacct", "-j", job_id, "--parsable2", "--noheader",
             "-o", "JobID,State,ExitCode,Elapsed,MaxRSS,JobName"],
            capture_output=True, text=True, timeout=30,
        )
    except subprocess.TimeoutExpired:
        return [{"job_id": job_id, "state": "UNKNOWN", "exit_code": "N/A",
                 "elapsed": "N/A", "max_rss": "N/A", "job_name": "N/A"}]

    results = []
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        fields = line.split("|")
        if len(fields) < 6:
            continue
        fid = fields[0]
        # Match base job ID (non-array) or sub-task IDs (e.g. 59482853_0); exclude .batch / .extern
        if fid == job_id or (fid.startswith(job_id + "_") and "." not in fid):
            results.append({
                "job_id": fid,
                "state": fields[1],
                "exit_code": fields[2],
                "elapsed": fields[3],
                "max_rss": fields[4],
                "job_name": fields[5],
            })

    if not results:
        return [{"job_id": job_id, "state": "UNKNOWN", "exit_code": "N/A",
                "elapsed": "N/A", "max_rss": "N/A", "job_name": "N/A"}]
    # If sacct returned both base job and sub-task rows (array job), keep only sub-tasks
    # to avoid analyzing a template path and sending a spurious notification for the base row
    sub_task_entries = [r for r in results if "_" in r["job_id"]]
    if sub_task_entries:
        return sub_task_entries
    return results


def _check_early_array_failures(job_id: str, error_file: str, job_name: str,
                                reported_failures: set) -> set:
    """Check sacct for any array sub-tasks that have already failed while the array is still running.
    Sends immediate Discord notifications for newly failed tasks. Returns updated set of reported IDs.
    """
    results = _check_sacct_all(job_id)
    if not results:
        return reported_failures

    # Only look at sub-task rows (contain '_')
    sub_tasks = [r for r in results if "_" in r["job_id"]]
    if not sub_tasks:
        return reported_failures

    new_failures = []
    for sacct_info in sub_tasks:
        sub_id = sacct_info["job_id"]
        state_str = sacct_info.get("state", "")
        exit_code = sacct_info.get("exit_code", "")
        # Skip if already reported, not in a terminal state, or is a success/cancel
        if sub_id in reported_failures:
            continue
        if state_str not in TERMINAL_STATES:
            continue
        if state_str.startswith("CANCELLED"):
            continue
        if state_str == "COMPLETED" and exit_code == "0:0":
            continue
        # This is a new failure
        new_failures.append(sacct_info)

    if not new_failures:
        return reported_failures

    # Send notifications for new failures
    config = load_config()
    patterns_file = BASE_DIR / "patterns" / "error_patterns.json"
    analyzer = ErrorAnalyzer(str(patterns_file))
    notifier = DiscordNotifier(
        config["discord"]["success_webhook"],
        config["discord"]["error_webhook"],
    )
    time.sleep(3)  # Brief wait for error files to flush

    total = len(sub_tasks)
    for sacct_info in new_failures:
        sub_id = sacct_info["job_id"]
        array_task_id = sub_id.split("_", 1)[1] if "_" in sub_id else None
        if array_task_id is not None:
            err_path = _substitute_slurm_vars(error_file, job_id, job_name,
                                              array_task_id=array_task_id, is_array=True)
        else:
            err_path = error_file
        analysis = analyzer.analyze(err_path)
        print(f"[jobmon] Early failure detected: {sub_id} state={sacct_info['state']}")
        array_info = {"task_id": sub_id, "task_index": array_task_id, "total_tasks": total}
        success = notifier.notify(sub_id, sacct_info, analysis, array_info=array_info)
        print(f"[jobmon] Early failure notification sent for {sub_id}: {success}")
        reported_failures.add(sub_id)

    return reported_failures


def _handle_termination(job_id: str, sacct_results: list, error_file: str,
                       output_file: str, state: StateManager,
                       job_name: str = "", is_array: bool = False,
                       already_reported: set = None):
    """Analyze error file(s) and send notification(s). Array jobs: one notification per failed
    sub-task, one success summary if all succeeded. CANCELLED (per task or whole job) skips notification.
    Sub-tasks in already_reported are skipped (already notified during early failure detection).
    """
    if already_reported is None:
        already_reported = set()
    config = load_config()
    patterns_file = BASE_DIR / "patterns" / "error_patterns.json"
    analyzer = ErrorAnalyzer(str(patterns_file))
    notifier = DiscordNotifier(
        config["discord"]["success_webhook"],
        config["discord"]["error_webhook"],
    )

    # Wait a bit for error files to be flushed
    time.sleep(5)

    if not sacct_results:
        state.mark_complete("UNKNOWN")
        print(f"[jobmon] Monitoring complete for job {job_id} (no sacct results)")
        return

    # Single result: non-array job
    if len(sacct_results) == 1:
        sacct_info = sacct_results[0]
        state_str = sacct_info.get("state", "")
        if state_str.startswith("CANCELLED"):
            print(f"[jobmon] Job {job_id} was cancelled — skipping notification")
            state.mark_complete("CANCELLED")
            return
        analysis = analyzer.analyze(error_file)
        print(f"[jobmon] Analysis: has_errors={analysis['has_errors']}, "
              f"patterns={analysis['matched_patterns']}")
        success = notifier.notify(job_id, sacct_info, analysis)
        print(f"[jobmon] Discord notification sent: {success}")
        state.mark_complete(state_str)
        print(f"[jobmon] Monitoring complete for job {job_id}")
        return

    # Multiple results: array job
    base_id = job_id
    total = len(sacct_results)
    completed_count = 0
    cancelled_count = 0
    all_success = True
    for sacct_info in sacct_results:
        sub_id = sacct_info.get("job_id", "")
        state_str = sacct_info.get("state", "")
        if state_str.startswith("CANCELLED"):
            cancelled_count += 1
            continue
        if state_str.startswith("COMPLETED") and sacct_info.get("exit_code", "") == "0:0":
            completed_count += 1
            continue
        all_success = False
        # Skip if already reported during early failure detection
        if sub_id in already_reported:
            print(f"[jobmon] Sub-task {sub_id} already reported — skipping")
            continue
        # Resolve error file path for this sub-task
        array_task_id = sub_id.split("_", 1)[1] if "_" in sub_id else None
        if array_task_id is not None:
            err_path = _substitute_slurm_vars(error_file, base_id, job_name,
                                              array_task_id=array_task_id, is_array=True)
        else:
            err_path = error_file
        analysis = analyzer.analyze(err_path)
        print(f"[jobmon] Sub-task {sub_id} Analysis: has_errors={analysis['has_errors']}, "
              f"patterns={analysis['matched_patterns']}")
        array_info = {"task_id": sub_id, "task_index": array_task_id, "total_tasks": total}
        success = notifier.notify(sub_id, sacct_info, analysis, array_info=array_info)
        print(f"[jobmon] Discord notification sent for {sub_id}: {success}")

    # All cancelled: skip notification (do not send false "COMPLETED" summary)
    if cancelled_count == total:
        print(f"[jobmon] Job {base_id} array: all {total} tasks cancelled — skipping notification")
        state.mark_complete("CANCELLED")
        print(f"[jobmon] Monitoring complete for job {base_id}")
        return

    if all_success and completed_count > 0:
        # One green summary notification (at least one task completed, none failed)
        summary_info = {
            "job_id": base_id,
            "state": "COMPLETED",
            "exit_code": "0:0",
            "elapsed": "N/A",
            "max_rss": "N/A",
            "job_name": job_name,
        }
        summary_analysis = {"has_errors": False, "matched_patterns": [], "error_summary": ""}
        array_info = {"total_tasks": total, "all_success": True}
        notifier.notify(base_id, summary_info, summary_analysis, array_info=array_info)
        print(f"[jobmon] Discord success summary sent for array {base_id} ({completed_count}/{total} tasks)")

    aggregate = "COMPLETED" if all_success else "MIXED"
    state.mark_complete(aggregate)
    print(f"[jobmon] Monitoring complete for job {base_id}")
