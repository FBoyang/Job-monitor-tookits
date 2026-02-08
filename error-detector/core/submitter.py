"""Wraps sbatch to submit jobs and extract metadata."""

import os
import re
import subprocess


def submit_job(sbatch_args: list) -> tuple:
    """
    Run sbatch with given args.
    Returns (job_id, error_file, output_file, working_dir).
    """
    result = subprocess.run(
        ["sbatch"] + sbatch_args,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"sbatch failed (exit {result.returncode}):\n{result.stderr.strip()}")

    match = re.search(r"Submitted batch job (\d+)", result.stdout)
    if not match:
        raise RuntimeError(f"Could not parse job ID from sbatch output:\n{result.stdout.strip()}")

    job_id = match.group(1)
    script_path = _find_script_in_args(sbatch_args)
    working_dir = os.getcwd()
    error_file, output_file = _parse_sbatch_directives(script_path, job_id, sbatch_args, working_dir)

    return job_id, error_file, output_file, working_dir


def _find_script_in_args(args: list) -> str | None:
    """Find the sbatch script path from the argument list."""
    skip_next = False
    for arg in args:
        if skip_next:
            skip_next = False
            continue
        if arg.startswith("-"):
            # Flags that take a value
            if arg in ("-p", "--partition", "-o", "--output", "-e", "--error",
                       "-J", "--job-name", "-t", "--time", "-n", "--ntasks",
                       "-N", "--nodes", "--mem", "--gres", "-A", "--account",
                       "--wrap", "-w", "--nodelist", "--exclude", "-d", "--dependency",
                       "--cpus-per-task", "--ntasks-per-node", "--mail-type", "--mail-user",
                       "--array", "-q", "--qos", "--constraint", "-C"):
                if "=" not in arg:
                    skip_next = True
            continue
        # Non-flag argument is the script
        if os.path.isfile(arg):
            return arg
    return None


def _parse_sbatch_directives(script_path: str | None, job_id: str,
                              sbatch_args: list, working_dir: str) -> tuple:
    """Parse #SBATCH directives and CLI args for error/output file paths."""
    error_file = None
    output_file = None

    # First, check CLI args (they override script directives)
    cli_error, cli_output = _parse_cli_args(sbatch_args)

    # Then parse script directives
    script_error = None
    script_output = None
    if script_path:
        try:
            with open(script_path) as f:
                for line in f:
                    stripped = line.strip()
                    if not stripped.startswith("#"):
                        if stripped and not stripped.startswith("#"):
                            break
                        continue
                    if not stripped.startswith("#SBATCH"):
                        continue
                    if "--error" in stripped or " -e " in stripped:
                        script_error = _extract_directive_path(stripped)
                    if "--output" in stripped or " -o " in stripped:
                        script_output = _extract_directive_path(stripped)
        except (OSError, IOError):
            pass

    # CLI args override script directives
    error_file = cli_error or script_error
    output_file = cli_output or script_output

    # SLURM default: slurm-<jobid>.out for both
    if not error_file and not output_file:
        default = os.path.join(working_dir, f"slurm-{job_id}.out")
        error_file = default
        output_file = default
    elif not error_file:
        error_file = os.path.join(working_dir, f"slurm-{job_id}.out")
    elif not output_file:
        output_file = os.path.join(working_dir, f"slurm-{job_id}.out")

    # Substitute %j with job ID, %x with job name
    error_file = _substitute_slurm_vars(error_file, job_id)
    output_file = _substitute_slurm_vars(output_file, job_id)

    # Make paths absolute relative to working dir
    if not os.path.isabs(error_file):
        error_file = os.path.join(working_dir, error_file)
    if not os.path.isabs(output_file):
        output_file = os.path.join(working_dir, output_file)

    return error_file, output_file


def _parse_cli_args(args: list) -> tuple:
    """Extract --error and --output from CLI arguments."""
    error_file = None
    output_file = None
    for i, arg in enumerate(args):
        if arg.startswith("--error=") or arg.startswith("-e="):
            error_file = arg.split("=", 1)[1]
        elif arg in ("--error", "-e") and i + 1 < len(args):
            error_file = args[i + 1]
        elif arg.startswith("--output=") or arg.startswith("-o="):
            output_file = arg.split("=", 1)[1]
        elif arg in ("--output", "-o") and i + 1 < len(args):
            output_file = args[i + 1]
    return error_file, output_file


def _extract_directive_path(line: str) -> str | None:
    """Extract file path from a #SBATCH directive line."""
    # Handle: #SBATCH --error=path or #SBATCH -e path
    line = line.replace("#SBATCH", "").strip()
    if "=" in line:
        return line.split("=", 1)[1].strip()
    parts = line.split()
    if len(parts) >= 2:
        return parts[1].strip()
    return None


def _substitute_slurm_vars(path: str, job_id: str) -> str:
    """Replace SLURM filename pattern variables."""
    path = path.replace("%j", job_id)
    path = path.replace("%J", job_id)
    # %x (job name) - we don't have it readily, leave as-is or strip
    return path
