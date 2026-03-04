"""Abstract base reader for summary statistics files."""

from abc import ABC, abstractmethod
from typing import Any


class BaseReader(ABC):
    """
    Base class for all file readers.

    Subclasses must implement read() which returns a dict with at minimum:
      - "file_path": str
      - "metrics": dict[str, Any]  (metric_name -> value)
      - "metadata": dict[str, Any] (optional extra info)
    """

    @abstractmethod
    def read(self, file_path: str) -> dict[str, Any]:
        """
        Read and parse a summary statistics file.

        Args:
            file_path: Absolute path to the file.

        Returns:
            Dict with keys: file_path, metrics, metadata.
        """
        ...
