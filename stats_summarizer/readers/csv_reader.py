"""CSV/TSV reader for summary statistics files."""

import csv
import os
from typing import Any

from readers.base import BaseReader


class CsvReader(BaseReader):
    """
    Reads CSV or TSV summary statistics files.

    Expected formats (auto-detected):
      1. Key-value: two columns (metric_name, value)
      2. Tabular: header row with metric names, data rows per method/run

    The reader attempts to auto-detect the format. You can extend this
    class or register a custom reader for specialized CSV layouts.
    """

    def read(self, file_path: str) -> dict[str, Any]:
        _, ext = os.path.splitext(file_path.lower())
        delimiter = "\t" if ext == ".tsv" else ","

        with open(file_path, newline="") as f:
            # Sniff the first few lines to detect format
            sample = f.read(4096)
            f.seek(0)

            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample)
                delimiter = dialect.delimiter
            except csv.Error:
                pass

            reader = csv.reader(f, delimiter=delimiter)
            rows = [row for row in reader if row and not row[0].startswith("#")]

        if not rows:
            return {
                "file_path": file_path,
                "metrics": {},
                "metadata": {"format": "csv", "error": "Empty file"},
            }

        # Heuristic: if exactly 2 columns, treat as key-value pairs
        if all(len(row) == 2 for row in rows):
            return self._parse_key_value(file_path, rows)

        # Otherwise treat as tabular (header + data rows)
        return self._parse_tabular(file_path, rows)

    def _parse_key_value(self, file_path: str, rows: list[list[str]]) -> dict[str, Any]:
        metrics = {}
        for key, val in rows:
            key = key.strip()
            val = val.strip()
            metrics[key] = self._try_numeric(val)
        return {
            "file_path": file_path,
            "metrics": metrics,
            "metadata": {"format": "csv_key_value", "num_metrics": len(metrics)},
        }

    def _parse_tabular(self, file_path: str, rows: list[list[str]]) -> dict[str, Any]:
        header = [h.strip() for h in rows[0]]
        data_rows = []
        for row in rows[1:]:
            entry = {}
            for i, col in enumerate(header):
                val = row[i].strip() if i < len(row) else ""
                entry[col] = self._try_numeric(val)
            data_rows.append(entry)

        # If single data row, flatten to metrics dict
        if len(data_rows) == 1:
            return {
                "file_path": file_path,
                "metrics": data_rows[0],
                "metadata": {"format": "csv_tabular_single", "columns": header},
            }

        return {
            "file_path": file_path,
            "metrics": {},
            "rows": data_rows,
            "metadata": {"format": "csv_tabular_multi", "columns": header, "num_rows": len(data_rows)},
        }

    @staticmethod
    def _try_numeric(val: str) -> int | float | str:
        """Attempt to convert a string to int or float."""
        try:
            return int(val)
        except ValueError:
            pass
        try:
            return float(val)
        except ValueError:
            return val
