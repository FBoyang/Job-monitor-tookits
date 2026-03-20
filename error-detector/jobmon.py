#!/usr/bin/env python3
"""jobmon - SLURM Job Monitor with Discord Notifications.

Usage:
    jobmon submit <script.sbatch> [sbatch-args...]      Submit and auto-monitor
    jobmon bash <script.sh> [args...]                   Run bash submitter and auto-watch child jobs
    jobmon watch <jobid> [--error FILE] [--output FILE] Monitor existing job
    jobmon status                                        Show active monitors
    jobmon cancel <jobid>                                Stop monitoring a job
    jobmon recover                                       Restart dead monitors
    jobmon test-discord                                  Test webhook connectivity
"""

import argparse
import json
import os
import re
import selectors
import signal
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from core.submitter import submit_job
from core.monitor import daemonize_and_monitor, load_config
from core.notifier import DiscordNotifier
from core.state import StateManager


def _infer_job_metadata_from_scontrol(job_id: str) -> tuple[str, str, str, bool]:
    """Infer stderr/stdout paths + name for an already-submitted job."""
    default = os.path.join(os.getcwd(), f"slurm-{job_id}.out")
    error_file = default
    output_file = default
    job_name = ""
    is_array = False

    try:
        result = subprocess.run(
            ["scontrol", "show", "job", "-o", job_id],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return error_file, output_file, job_name, is_array

        line = result.stdout.strip().splitlines()[0]
        fields = {}
        for token in line.split():
            if "=" in token:
                k, v = token.split("=", 1)
                fields[k] = v

        job_name = fields.get("JobName", "")
        stderr_raw = fields.get("StdErr")
        stdout_raw = fields.get("StdOut")
        array_spec = fields.get("ArrayTaskId", "")
        is_array = bool(array_spec and array_spec != "N/A")

        if stderr_raw:
            error_file = stderr_raw
        if stdout_raw:
            output_file = stdout_raw
    except (OSError, subprocess.SubprocessError):
        pass

    return error_file, output_file, job_name, is_array


def _notify_bash_failure(script_path: str, return_code: int, tail_lines: list[str]):
    """Send an immediate Discord error notification for local bash failure."""
    try:
        config = load_config()
        notifier = DiscordNotifier(
            config["discord"]["success_webhook"],
            config["discord"]["error_webhook"],
        )
        script_name = os.path.basename(script_path)
        notifier.notify(
            job_id=f"bash:{script_name}",
            sacct_info={
                "state": "BASH_FAILED",
                "exit_code": f"{return_code}:0",
                "elapsed": "N/A",
                "job_name": script_name,
                "max_rss": "N/A",
            },
            analysis={
                "has_errors": True,
                "error_summary": f"Local bash submitter failed: {script_path} (exit {return_code})",
                "matched_patterns": ["bash submitter failure"],
                "relevant_lines": tail_lines[-8:],
                "tail": tail_lines[-20:],
            },
        )
    except Exception as e:
        print(f"[jobmon] Warning: failed to send bash failure notification: {e}")


def cmd_submit(args):
    """Submit a job via sbatch and start monitoring."""
    sbatch_args = args.sbatch_args
    if not sbatch_args:
        print("Error: No sbatch arguments provided.")
        print("Usage: jobmon submit <script.sbatch> [sbatch-args...]")
        sys.exit(1)

    print(f"[jobmon] Submitting: sbatch {' '.join(sbatch_args)}")

    try:
        job_id, error_file, output_file, working_dir, job_name, is_array = submit_job(sbatch_args)
    except RuntimeError as e:
        print(f"[jobmon] Submit failed: {e}")
        sys.exit(1)

    print(f"[jobmon] Job {job_id} submitted successfully")
    print(f"[jobmon] Error file: {error_file}")
    print(f"[jobmon] Output file: {output_file}")
    print(f"[jobmon] Starting background monitor...")

    daemonize_and_monitor(job_id, error_file, output_file, job_name=job_name, is_array=is_array)

    # Give daemon time to start
    time.sleep(1)
    print(f"[jobmon] Monitor running in background. Check with: jobmon status")
    print(f"[jobmon] Monitor log: {BASE_DIR / 'logs' / f'{job_id}.monitor.log'}")


def cmd_bash(args):
    """Run a local bash submitter script and auto-watch child sbatch jobs."""
    script_path = args.script_path
    script_args = args.script_args or []

    if not os.path.isfile(script_path):
        print(f"[jobmon] Error: script not found: {script_path}")
        sys.exit(1)

    cmd = ["bash", script_path] + script_args
    print(f"[jobmon] Running local submitter: {' '.join(cmd)}")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    submitted_job_ids = []
    seen_job_ids = set()
    tail_lines = []
    submit_re = re.compile(r"Submitted batch job (\d+)")

    sel = selectors.DefaultSelector()
    if proc.stdout:
        sel.register(proc.stdout, selectors.EVENT_READ, data="stdout")
    if proc.stderr:
        sel.register(proc.stderr, selectors.EVENT_READ, data="stderr")

    while sel.get_map():
        for key, _ in sel.select(timeout=0.5):
            stream = key.fileobj
            line = stream.readline()
            if line == "":
                sel.unregister(stream)
                continue

            print(line, end="")
            line_clean = line.rstrip("\n")
            tail_lines.append(line_clean)
            if len(tail_lines) > 100:
                tail_lines = tail_lines[-100:]

            for match in submit_re.finditer(line):
                job_id = match.group(1)
                if job_id in seen_job_ids:
                    continue
                seen_job_ids.add(job_id)
                submitted_job_ids.append(job_id)

                error_file, output_file, job_name, is_array = _infer_job_metadata_from_scontrol(job_id)
                print(f"[jobmon] Detected child job {job_id}; starting monitor...")
                print(f"[jobmon] Error file: {error_file}")
                print(f"[jobmon] Output file: {output_file}")
                daemonize_and_monitor(job_id, error_file, output_file, job_name=job_name, is_array=is_array)
                time.sleep(0.2)

    return_code = proc.wait()
    if return_code != 0:
        print(f"[jobmon] Local submitter failed with exit code {return_code}")
        _notify_bash_failure(script_path, return_code, tail_lines)
        sys.exit(return_code)

    if submitted_job_ids:
        print(f"[jobmon] Local submitter finished successfully.")
        print(f"[jobmon] Monitors started for child jobs: {', '.join(submitted_job_ids)}")
        print(f"[jobmon] Check with: jobmon status")
    else:
        print("[jobmon] Local submitter finished successfully, but no child jobs were detected.")


def cmd_watch(args):
    """Monitor an already-submitted job."""
    job_id = args.job_id
    error_file = args.error
    output_file = args.output

    # If files not provided, try to figure them out from sacct or defaults
    if not error_file or not output_file:
        cwd = os.getcwd()
        default = os.path.join(cwd, f"slurm-{job_id}.out")
        if not error_file:
            error_file = default
        if not output_file:
            output_file = default
        print(f"[jobmon] Using default file paths (override with --error/--output):")
        print(f"[jobmon] Error file: {error_file}")
        print(f"[jobmon] Output file: {output_file}")

    print(f"[jobmon] Starting monitor for job {job_id}...")

    daemonize_and_monitor(job_id, error_file, output_file, job_name="", is_array=False)

    time.sleep(1)
    print(f"[jobmon] Monitor running in background. Check with: jobmon status")
    print(f"[jobmon] Monitor log: {BASE_DIR / 'logs' / f'{job_id}.monitor.log'}")


def cmd_status(args):
    """Show all monitored jobs."""
    all_states = StateManager.list_all()

    if not all_states:
        print("[jobmon] No monitored jobs found.")
        return

    print(f"{'Job ID':<12} {'Status':<14} {'Monitor':<10} {'Final State':<16} {'Error File'}")
    print("-" * 80)

    for s in all_states:
        job_id = s.get("job_id", "?")
        status = s.get("status", "?")
        alive = s.get("monitor_alive", False)
        monitor_str = "alive" if alive else ("done" if status == "complete" else "DEAD")
        final = s.get("final_state", "-")
        error_f = s.get("error_file", "-")
        # Truncate long paths
        if len(error_f) > 35:
            error_f = "..." + error_f[-32:]
        print(f"{job_id:<12} {status:<14} {monitor_str:<10} {final:<16} {error_f}")


def cmd_cancel(args):
    """Stop monitoring a job (does NOT cancel the SLURM job)."""
    job_id = args.job_id
    state = StateManager(job_id)

    if not state.path.exists():
        print(f"[jobmon] No monitor found for job {job_id}")
        return

    try:
        data = json.loads(state.path.read_text())
    except (json.JSONDecodeError, OSError):
        print(f"[jobmon] Could not read state for job {job_id}")
        return

    pid = data.get("monitor_pid")
    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"[jobmon] Sent SIGTERM to monitor PID {pid}")
        except OSError:
            print(f"[jobmon] Monitor PID {pid} not running")

    state.mark_complete("MONITOR_CANCELLED")
    print(f"[jobmon] Monitor for job {job_id} cancelled (SLURM job is NOT affected)")


def cmd_recover(args):
    """Restart monitors for jobs whose monitor process died."""
    recoverable = StateManager.list_recoverable()

    if not recoverable:
        print("[jobmon] No dead monitors to recover.")
        return

    import subprocess
    for data in recoverable:
        job_id = data["job_id"]
        error_file = data.get("error_file", "")
        output_file = data.get("output_file", "")
        job_name = data.get("job_name", "")
        is_array = data.get("is_array", False)

        # Check if the SLURM job is still active
        result = subprocess.run(
            ["squeue", "-j", job_id, "-h", "-o", "%T"],
            capture_output=True, text=True,
        )
        slurm_status = result.stdout.strip()

        if not slurm_status:
            print(f"[jobmon] Job {job_id}: SLURM job already finished, running final check...")
            # Job is done but monitor died before completing — run inline
            from core.monitor import _check_sacct_all_with_retry, _handle_termination
            sacct_results = _check_sacct_all_with_retry(job_id)
            sm = StateManager(job_id)
            _handle_termination(job_id, sacct_results, error_file, output_file, sm, job_name, is_array)
            print(f"[jobmon] Job {job_id}: recovery complete")
        else:
            print(f"[jobmon] Job {job_id}: still {slurm_status}, restarting monitor...")
            daemonize_and_monitor(job_id, error_file, output_file, job_name=job_name, is_array=is_array)
            time.sleep(1)
            print(f"[jobmon] Job {job_id}: monitor restarted")


def cmd_test_discord(args):
    """Send test messages to both Discord channels."""
    config = load_config()
    notifier = DiscordNotifier(
        config["discord"]["success_webhook"],
        config["discord"]["error_webhook"],
    )
    print("[jobmon] Sending test messages to Discord...")
    results = notifier.send_test()

    if results.get("success_channel"):
        print("[jobmon] Success channel: OK")
    else:
        print(f"[jobmon] Success channel: FAILED - {results.get('success_error', 'unknown')}")

    if results.get("error_channel"):
        print("[jobmon] Error channel: OK")
    else:
        print(f"[jobmon] Error channel: FAILED - {results.get('error_error', 'unknown')}")

    if results.get("success_channel") and results.get("error_channel"):
        print("[jobmon] All tests passed!")
    else:
        print("[jobmon] Some tests failed. Check your webhook URLs in config.json")


def main():
    parser = argparse.ArgumentParser(
        prog="jobmon",
        description="SLURM Job Monitor with Discord Notifications",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # submit
    sub = subparsers.add_parser("submit", help="Submit a job and auto-monitor")
    sub.add_argument("sbatch_args", nargs=argparse.REMAINDER, help="Arguments passed to sbatch")

    # bash
    sub = subparsers.add_parser("bash", help="Run local bash submitter and auto-watch child jobs")
    sub.add_argument("script_path", help="Path to local bash script")
    sub.add_argument("script_args", nargs=argparse.REMAINDER, help="Arguments passed to the bash script")

    # watch
    sub = subparsers.add_parser("watch", help="Monitor an already-submitted job")
    sub.add_argument("job_id", help="SLURM job ID")
    sub.add_argument("--error", "-e", help="Path to error file")
    sub.add_argument("--output", "-o", help="Path to output file")

    # status
    subparsers.add_parser("status", help="Show active monitors")

    # cancel
    sub = subparsers.add_parser("cancel", help="Stop monitoring a job")
    sub.add_argument("job_id", help="SLURM job ID")

    # recover
    subparsers.add_parser("recover", help="Restart dead monitors")

    # test-discord
    subparsers.add_parser("test-discord", help="Test Discord webhook connectivity")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    commands = {
        "submit": cmd_submit,
        "bash": cmd_bash,
        "watch": cmd_watch,
        "status": cmd_status,
        "cancel": cmd_cancel,
        "recover": cmd_recover,
        "test-discord": cmd_test_discord,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()
