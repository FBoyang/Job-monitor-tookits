"""JSON reader for summary statistics files."""

import json
from typing import Any

from readers.base import BaseReader


class JsonReader(BaseReader):
    """
    Reads JSON summary statistics files.

    Supports:
      - Flat dict: {"metric_name": value, ...}
      - Nested dict with "metrics" key
      - List of metric dicts
    """

    def read(self, file_path: str) -> dict[str, Any]:
        with open(file_path) as f:
            data = json.load(f)

        if isinstance(data, dict):
            # If it has a "metrics" key, use that
            if "metrics" in data:
                metrics = data["metrics"]
                metadata = {k: v for k, v in data.items() if k != "metrics"}
                return {
                    "file_path": file_path,
                    "metrics": metrics,
                    "metadata": {**metadata, "format": "json_nested"},
                }
            # Otherwise treat entire dict as flat metrics
            return {
                "file_path": file_path,
                "metrics": data,
                "metadata": {"format": "json_flat"},
            }

        if isinstance(data, list):
            return {
                "file_path": file_path,
                "metrics": {},
                "rows": data,
                "metadata": {"format": "json_list", "num_entries": len(data)},
            }

        return {
            "file_path": file_path,
            "metrics": {},
            "metadata": {"format": "json_unknown", "error": "Unrecognized structure"},
        }
