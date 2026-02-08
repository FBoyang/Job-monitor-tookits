"""Analyzes SLURM error files for known error patterns."""

import json
import re
from pathlib import Path


class ErrorAnalyzer:
    def __init__(self, patterns_file: str):
        with open(patterns_file) as f:
            data = json.load(f)
        self.patterns = data.get("patterns", [])

    def analyze(self, error_file: str, max_lines: int = 500) -> dict:
        """
        Analyze a SLURM error file for known error patterns.

        Returns dict with:
            has_errors: bool
            error_summary: str
            matched_patterns: list of pattern names
            relevant_lines: list of matching lines (up to 10)
            tail: last 20 lines of the file
        """
        path = Path(error_file)

        if not path.exists():
            return {
                "has_errors": True,
                "error_summary": f"Error file not found: {error_file}",
                "matched_patterns": ["File Missing"],
                "relevant_lines": [f"Expected error file at: {error_file}"],
                "tail": [],
            }

        try:
            text = path.read_text(errors="replace")
        except Exception as e:
            return {
                "has_errors": True,
                "error_summary": f"Could not read error file: {e}",
                "matched_patterns": ["Read Error"],
                "relevant_lines": [str(e)],
                "tail": [],
            }

        lines = text.splitlines()
        # Only check last max_lines to avoid OOM on huge files
        check_lines = lines[-max_lines:] if len(lines) > max_lines else lines

        matched = []
        relevant = []

        for pattern_def in self.patterns:
            try:
                regex = re.compile(pattern_def["regex"], re.IGNORECASE)
            except re.error:
                continue

            for line in check_lines:
                if regex.search(line):
                    name = pattern_def["name"]
                    if name not in matched:
                        matched.append(name)
                        relevant.append(line.strip()[:200])
                    break  # One match per pattern is enough

        tail = lines[-20:] if len(lines) >= 20 else lines

        has_errors = len(matched) > 0
        summary = ", ".join(matched) if matched else "No known error patterns detected"

        return {
            "has_errors": has_errors,
            "error_summary": summary,
            "matched_patterns": matched,
            "relevant_lines": relevant[:10],
            "tail": [line.strip()[:200] for line in tail],
        }
