# jobmon — SLURM Job Monitor with Discord Notifications

A lightweight CLI tool that wraps `sbatch`, automatically monitors your SLURM jobs in the background, and sends Discord notifications when they finish — routing successes and failures to separate channels.

**Zero dependencies to install.** Uses only Python stdlib + `requests` (already available on Kempner).

---

## Quick Start

### 1. Add to PATH (one-time)

```bash
echo 'export PATH="/n/home12/bof695/holylfs06/Users/bof695/auxiliary/Job-monitor-tookits/error-detector:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

### 2. Verify Discord connection

```bash
jobmon test-discord
```

### 3. Submit a job

```bash
# Instead of:  sbatch submit_job.sh
# Use:
jobmon submit submit_job.sh

# All sbatch flags pass through:
jobmon submit --gres=gpu:4 -p kempner_h100 -t 02:00:00 train.sbatch
```

That's it. A background monitor starts automatically and will send a Discord message when the job finishes.

---

## Commands

| Command | Description |
|---|---|
| `jobmon submit <script> [sbatch-args...]` | Submit via sbatch + start monitoring |
| `jobmon watch <jobid>` | Attach a monitor to an already-submitted job |
| `jobmon status` | List all monitored jobs and monitor health |
| `jobmon cancel <jobid>` | Stop monitoring (does **not** cancel the SLURM job) |
| `jobmon recover` | Restart monitors that died (e.g. after a node reboot) |
| `jobmon test-discord` | Send test messages to both Discord channels |

### Examples

```bash
# Submit and monitor
jobmon submit train.sbatch

# Monitor a job you already submitted with regular sbatch
jobmon watch 48291037

# Monitor with explicit log paths (if non-default)
jobmon watch 48291037 --error logs/train-48291037.err --output logs/train-48291037.out

# Check what's being monitored
jobmon status

# Stop monitoring job 48291037 (the SLURM job keeps running)
jobmon cancel 48291037

# SSH disconnected and monitor died? Recover it:
jobmon recover
```

---

## How It Works

### Architecture Overview

```
You run:  jobmon submit train.sbatch
              │
              ▼
         ┌─────────┐
         │ sbatch   │ ──▶ SLURM schedules your job (e.g. job 12345678)
         └─────────┘
              │
              ▼
     ┌──────────────────┐
     │ Background Daemon │   (double-fork, survives SSH disconnect)
     │                    │
     │  loop:             │
     │    squeue -j 12345 │ ──▶ RUNNING? wait 30s, poll again
     │    ...             │     PENDING? wait 60s, poll again
     │    (job vanishes)  │     Gone from queue? ──▼
     │                    │
     │  sacct -j 12345    │ ──▶ Get final state, exit code, runtime, memory
     │                    │
     │  read stderr file  │ ──▶ Match against 18 error patterns
     │                    │
     │  POST to Discord   │ ──▶ Success channel (green) or Error channel (red)
     └──────────────────┘
```

### Step-by-step

1. **Submit**: `jobmon submit` runs `sbatch` with your exact arguments, captures the job ID from stdout.

2. **Parse directives**: Reads your sbatch script for `#SBATCH --error` / `#SBATCH --output` to locate log files. Handles `%j` substitution. Falls back to `slurm-<jobid>.out`.

3. **Spawn daemon**: Forks a fully detached background process (double-fork + `setsid`). This process is independent of your terminal — it keeps running if you close SSH, log out, or disconnect.

4. **Poll**: The daemon checks `squeue -j <jobid> -h -o "%T"` every 30 seconds (60s if pending). When the job disappears from the queue, it moves to the next step.

5. **Query final state**: Runs `sacct` with retries (data can lag a few seconds) to get: state (COMPLETED/FAILED/TIMEOUT/etc.), exit code, wall time, peak memory.

6. **Analyze errors**: Reads the last 500 lines of the SLURM error file and matches against regex patterns in `patterns/error_patterns.json`. Detects: Python tracebacks, CUDA OOM, NCCL errors, segfaults, disk quota, killed signals, and more.

7. **Notify**: Sends a Discord embed via webhook:
   - **COMPLETED + exit code 0** → `#success` channel (green card with job stats)
   - **Anything else** → `#error` channel (red card with job stats + matched error patterns + relevant stderr lines)

8. **Cleanup**: Updates the state file to `complete`.

### Resilience

| Scenario | What happens |
|---|---|
| SSH disconnects | Monitor is a daemon — keeps running independently |
| Monitor process dies | `jobmon recover` detects dead monitors and restarts them |
| `sacct` returns empty | Retries 5 times, 10 seconds apart |
| Error file is huge (GBs) | Only reads last 500 lines |
| Discord webhook unreachable | Retries 3 times with backoff |
| Duplicate monitor for same job | Detected and rejected |

---

## Error Patterns Detected

The analyzer checks stderr against these patterns (configurable in `patterns/error_patterns.json`):

| Pattern | Example match |
|---|---|
| Python Traceback | `Traceback (most recent call last)` |
| CUDA Out of Memory | `torch.cuda.OutOfMemoryError` |
| SLURM OOM Kill | `Exceeded job memory limit` |
| Segmentation Fault | `Segmentation fault (core dumped)` |
| NCCL Error | `NCCL error`, `ncclSystemError` |
| CUDA Error | `RuntimeError: CUDA error` |
| ImportError | `ModuleNotFoundError: No module named` |
| Permission Denied | `Permission denied` |
| Disk Quota Exceeded | `No space left on device` |
| Timeout | `DUE TO TIME LIMIT` |
| Bus Error | `Bus error` |
| Killed Signal | `Killed`, `SIGKILL` |
| NCCL Timeout | `Watchdog caught collective operation timeout` |
| Distributed Error | `RendezvousError` |
| Memory Error | `std::bad_alloc` |

Add your own patterns by editing `patterns/error_patterns.json`.

---

## File Structure

```
error-detector/
├── jobmon                   # Bash entry point (add to PATH)
├── jobmon.py                # CLI dispatcher (argparse, subcommands)
├── config.json              # Your Discord webhook URLs (gitignored)
├── config.json.example      # Template for config.json
├── core/
│   ├── __init__.py
│   ├── submitter.py         # Wraps sbatch, parses #SBATCH directives
│   ├── monitor.py           # Daemon lifecycle, polling loop, sacct queries
│   ├── error_analyzer.py    # Regex matching against stderr
│   ├── notifier.py          # Discord webhook POST with rich embeds
│   └── state.py             # Per-job JSON state files for tracking
├── patterns/
│   └── error_patterns.json  # Configurable error regexes
├── state/                   # Runtime: one JSON file per monitored job
├── logs/                    # Runtime: one log file per monitor daemon
└── README.md
```

---

## Configuration

Edit `config.json`:

```json
{
  "discord": {
    "success_webhook": "https://discord.com/api/webhooks/...",
    "error_webhook": "https://discord.com/api/webhooks/..."
  }
}
```

To create webhooks: Open Discord → your server → channel settings (gear) → Integrations → Webhooks → New Webhook → Copy URL. Create one for each channel.

---

## Troubleshooting

**`jobmon: command not found`** — Add the tool to your PATH (see Quick Start step 1).

**Discord test fails** — Check that your webhook URLs in `config.json` are correct and the webhooks haven't been deleted in Discord.

**Monitor died (shown as `DEAD` in `jobmon status`)** — Run `jobmon recover`. If the SLURM job already finished, it will do a one-time analysis and send the notification. If still running, it restarts the monitor.

**Wrong error/output file detected** — When using `jobmon watch`, pass explicit paths: `jobmon watch <id> --error /path/to/err --output /path/to/out`.

**Monitor log** — Check `logs/<jobid>.monitor.log` for detailed monitor output.
