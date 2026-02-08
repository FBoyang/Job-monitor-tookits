#!/usr/bin/env python3
"""jobmon - SLURM Job Monitor with Discord Notifications.

Usage:
    jobmon submit <script.sbatch> [sbatch-args...]   Submit and auto-monitor
    jobmon watch <jobid> [--error FILE] [--output FILE]  Monitor existing job
    jobmon status                                     Show active monitors
    jobmon cancel <jobid>                             Stop monitoring a job
    jobmon recover                                    Restart dead monitors
    jobmon test-discord                               Test webhook connectivity
"""

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from core.submitter import submit_job
from core.monitor import daemonize_and_monitor, load_config
from core.notifier import DiscordNotifier
from core.state import StateManager


def cmd_submit(args):
    """Submit a job via sbatch and start monitoring."""
    sbatch_args = args.sbatch_args
    if not sbatch_args:
        print("Error: No sbatch arguments provided.")
        print("Usage: jobmon submit <script.sbatch> [sbatch-args...]")
        sys.exit(1)

    print(f"[jobmon] Submitting: sbatch {' '.join(sbatch_args)}")

    try:
        job_id, error_file, output_file, working_dir = submit_job(sbatch_args)
    except RuntimeError as e:
        print(f"[jobmon] Submit failed: {e}")
        sys.exit(1)

    print(f"[jobmon] Job {job_id} submitted successfully")
    print(f"[jobmon] Error file: {error_file}")
    print(f"[jobmon] Output file: {output_file}")
    print(f"[jobmon] Starting background monitor...")

    daemonize_and_monitor(job_id, error_file, output_file)

    # Give daemon time to start
    time.sleep(1)
    print(f"[jobmon] Monitor running in background. Check with: jobmon status")
    print(f"[jobmon] Monitor log: {BASE_DIR / 'logs' / f'{job_id}.monitor.log'}")


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

    daemonize_and_monitor(job_id, error_file, output_file)

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

        # Check if the SLURM job is still active
        result = subprocess.run(
            ["squeue", "-j", job_id, "-h", "-o", "%T"],
            capture_output=True, text=True,
        )
        slurm_status = result.stdout.strip()

        if not slurm_status:
            print(f"[jobmon] Job {job_id}: SLURM job already finished, running final check...")
            # Job is done but monitor died before completing — run inline
            from core.monitor import _check_sacct_with_retry, _handle_termination
            final = _check_sacct_with_retry(job_id)
            sm = StateManager(job_id)
            _handle_termination(job_id, final, error_file, output_file, sm)
            print(f"[jobmon] Job {job_id}: recovery complete")
        else:
            print(f"[jobmon] Job {job_id}: still {slurm_status}, restarting monitor...")
            daemonize_and_monitor(job_id, error_file, output_file)
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
        "watch": cmd_watch,
        "status": cmd_status,
        "cancel": cmd_cancel,
        "recover": cmd_recover,
        "test-discord": cmd_test_discord,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()
