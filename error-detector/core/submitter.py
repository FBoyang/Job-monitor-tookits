"""Wraps sbatch to submit jobs and extract metadata."""

import os
import re
import subprocess


def submit_job(sbatch_args: list) -> tuple:
    """
    Run sbatch with given args.
    Returns (job_id, error_file, output_file, working_dir, job_name, is_array).
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
    error_file, output_file, job_name, is_array = _parse_sbatch_directives(
        script_path, job_id, sbatch_args, working_dir
    )

    return job_id, error_file, output_file, working_dir, job_name, is_array


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


def _extract_directive_value(line: str, key_options: list[str]) -> str | None:
    """Extract value for a directive (e.g. job name, array) from #SBATCH line."""
    line_clean = line.replace("#SBATCH", "").strip()
    for key in key_options:
        if key in line_clean:
            if "=" in line_clean:
                idx = line_clean.index("=")
                value = line_clean[idx + 1:].strip()
                return _strip_inline_comment(value)
            parts = line_clean.split()
            for i, p in enumerate(parts):
                if p in (key, key.replace("=", "")) and i + 1 < len(parts):
                    return _strip_inline_comment(parts[i + 1])
    return None


def _parse_sbatch_directives(script_path: str | None, job_id: str,
                              sbatch_args: list, working_dir: str) -> tuple:
    """Parse #SBATCH directives and CLI args for error/output paths, job_name, is_array."""
    error_file = None
    output_file = None
    job_name = ""
    is_array = False

    # First, check CLI args (they override script directives)
    cli_error, cli_output = _parse_cli_args(sbatch_args)
    cli_job_name, cli_array = _parse_cli_job_name_and_array(sbatch_args)

    # Then parse script directives
    script_error = None
    script_output = None
    script_job_name = None
    script_array = False
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
                    if "--job-name" in stripped or " -J " in stripped:
                        script_job_name = _extract_directive_value(
                            stripped, ["--job-name=", "-J", "--job-name"]
                        )
                    if "--array" in stripped or " -a " in stripped:
                        script_array = _extract_directive_value(
                            stripped, ["--array=", "-a", "--array"]
                        ) is not None
        except (OSError, IOError):
            pass

    # CLI overrides script
    error_file = cli_error or script_error
    output_file = cli_output or script_output
    job_name = cli_job_name or script_job_name or ""
    is_array = cli_array or script_array

    # SLURM default: slurm-<jobid>.out for both
    if not error_file and not output_file:
        default = os.path.join(working_dir, f"slurm-{job_id}.out")
        error_file = default
        output_file = default
    elif not error_file:
        error_file = os.path.join(working_dir, f"slurm-{job_id}.out")
    elif not output_file:
        output_file = os.path.join(working_dir, f"slurm-{job_id}.out")

    # Substitute SLURM vars; for array jobs keep %j/%a as template placeholders
    error_file = _substitute_slurm_vars(error_file, job_id, job_name=job_name, is_array=is_array)
    output_file = _substitute_slurm_vars(output_file, job_id, job_name=job_name, is_array=is_array)

    # Make paths absolute relative to working dir
    if not os.path.isabs(error_file):
        error_file = os.path.join(working_dir, error_file)
    if not os.path.isabs(output_file):
        output_file = os.path.join(working_dir, output_file)

    return error_file, output_file, job_name, is_array


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


def _parse_cli_job_name_and_array(args: list) -> tuple:
    """Extract --job-name/-J and --array from CLI arguments."""
    job_name = None
    has_array = False
    skip_next = False
    for i, arg in enumerate(args):
        if skip_next:
            skip_next = False
            continue
        if arg.startswith("--job-name=") or arg.startswith("-J="):
            job_name = arg.split("=", 1)[1]
        elif arg in ("--job-name", "-J") and i + 1 < len(args):
            job_name = args[i + 1]
            skip_next = True
        elif arg.startswith("--array=") or arg.startswith("-a="):
            has_array = True
        elif arg in ("--array", "-a") and i + 1 < len(args):
            has_array = True
            skip_next = True
    return job_name or "", has_array


def _strip_inline_comment(value: str) -> str:
    """Strip trailing # comment from directive value (use space before # to avoid paths with #)."""
    if " #" in value:
        value = value[:value.index(" #")].strip()
    return value


def _extract_directive_path(line: str) -> str | None:
    """Extract file path from a #SBATCH directive line."""
    # Handle: #SBATCH --error=path or #SBATCH -e path
    line = line.replace("#SBATCH", "").strip()
    if "=" in line:
        value = line.split("=", 1)[1].strip()
        return _strip_inline_comment(value)
    parts = line.split()
    if len(parts) >= 2:
        return _strip_inline_comment(parts[1].strip())
    return None


def _substitute_slurm_vars(path: str, job_id: str, job_name: str = "",
                          array_task_id: str | None = None,
                          is_array: bool = False) -> str:
    """Replace SLURM filename pattern variables.
    For array jobs, when array_task_id is None we keep %j/%a as placeholders (template).
    When array_task_id is set, we substitute per-sub-task.
    """
    path = path.replace("%J", job_id)
    path = path.replace("%A", job_id)
    path = path.replace("%x", job_name or "")
    if array_task_id is not None:
        path = path.replace("%j", f"{job_id}_{array_task_id}")
        path = path.replace("%a", array_task_id)
    else:
        if not is_array:
            path = path.replace("%j", job_id)
        # else: leave %j and %a as placeholders for template
    return path
