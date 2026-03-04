"""
Reader package for parsing various summary statistics file formats.

Readers are auto-selected based on file extension. To add support for
a new format, create a new reader class inheriting from BaseReader
and register it in READER_REGISTRY.
"""

from readers.base import BaseReader
from readers.csv_reader import CsvReader
from readers.json_reader import JsonReader
from readers.txt_reader import TxtReader

# Map file extensions to reader classes
READER_REGISTRY: dict[str, type[BaseReader]] = {
    ".csv": CsvReader,
    ".tsv": CsvReader,
    ".json": JsonReader,
    ".txt": TxtReader,
    ".log": TxtReader,
}


def load_reader(file_path: str) -> BaseReader:
    """Select and instantiate the appropriate reader for a file."""
    import os
    _, ext = os.path.splitext(file_path.lower())
    reader_cls = READER_REGISTRY.get(ext, TxtReader)
    return reader_cls()


def register_reader(extension: str, reader_cls: type[BaseReader]) -> None:
    """Register a custom reader for a file extension."""
    READER_REGISTRY[extension] = reader_cls
