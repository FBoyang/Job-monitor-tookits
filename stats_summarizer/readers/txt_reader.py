"""Plain text reader for summary statistics files."""

import re
from typing import Any

from readers.base import BaseReader


class TxtReader(BaseReader):
    """
    Reads plain text summary statistics files.

    Attempts to extract key-value pairs from lines matching patterns like:
      metric_name: value
      metric_name = value
      metric_name    value
    """

    # Patterns to match key-value lines (in priority order)
    KV_PATTERNS = [
        re.compile(r"^([A-Za-z_][\w\s/\-]*?)\s*[:=]\s*([0-9eE.+\-]+.*)$"),
        re.compile(r"^([A-Za-z_][\w\s/\-]*?)\s{2,}([0-9eE.+\-]+.*)$"),
    ]

    def read(self, file_path: str) -> dict[str, Any]:
        with open(file_path) as f:
            lines = f.readlines()

        metrics = {}
        unparsed = []

        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            matched = False
            for pat in self.KV_PATTERNS:
                m = pat.match(line)
                if m:
                    key = m.group(1).strip()
                    val = m.group(2).strip()
                    metrics[key] = self._try_numeric(val)
                    matched = True
                    break

            if not matched:
                unparsed.append(line)

        return {
            "file_path": file_path,
            "metrics": metrics,
            "metadata": {
                "format": "txt",
                "num_metrics": len(metrics),
                "unparsed_lines": len(unparsed),
            },
            "raw_unparsed": unparsed[:50],  # Keep some for agent inspection
        }

    @staticmethod
    def _try_numeric(val: str) -> int | float | str:
        try:
            return int(val)
        except ValueError:
            pass
        try:
            return float(val)
        except ValueError:
            return val
