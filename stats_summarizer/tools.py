"""
Tool definitions for the OpenAI function-calling agent.

Each tool is registered in TOOL_DEFINITIONS (OpenAI schema) and has a
corresponding Python implementation dispatched by execute_tool().

To add a new tool:
  1. Add the schema dict to TOOL_DEFINITIONS.
  2. Implement the function.
  3. Register it in _TOOL_REGISTRY.
"""

import os
import glob
import json
from typing import Any

from readers import load_reader
from comparator import compare_metrics
from reporter import format_table


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

def tool_list_files(directory: str, pattern: str = "*") -> str:
    """List files in a directory matching a glob pattern."""
    directory = os.path.expanduser(directory)
    if not os.path.isdir(directory):
        return json.dumps({"error": f"Directory not found: {directory}"})

    matches = sorted(glob.glob(os.path.join(directory, "**", pattern), recursive=True))
    files = [f for f in matches if os.path.isfile(f)]
    return json.dumps({
        "directory": directory,
        "pattern": pattern,
        "file_count": len(files),
        "files": files,
    })


def tool_read_stats(file_path: str, method_name: str = "") -> str:
    """Read and parse a summary statistics file."""
    file_path = os.path.expanduser(file_path)
    if not os.path.isfile(file_path):
        return json.dumps({"error": f"File not found: {file_path}"})

    reader = load_reader(file_path)
    data = reader.read(file_path)

    if method_name:
        data["method_name"] = method_name

    return json.dumps(data, default=str)


def tool_compare(
    metrics_collection: list[dict],
    metric_names: list[str] | None = None,
) -> str:
    """Compare metrics across multiple methods."""
    result = compare_metrics(metrics_collection, metric_names)
    return json.dumps(result, default=str)


def tool_generate_table(
    comparison_data: dict,
    title: str = "Metric Comparison",
    format: str = "markdown",
) -> str:
    """Generate a formatted comparison table."""
    table = format_table(comparison_data, title=title, fmt=format)
    return table


def tool_read_file_raw(file_path: str, max_lines: int = 200) -> str:
    """Read raw file content (for unstructured or unknown formats)."""
    file_path = os.path.expanduser(file_path)
    if not os.path.isfile(file_path):
        return json.dumps({"error": f"File not found: {file_path}"})
    try:
        with open(file_path) as f:
            lines = []
            for i, line in enumerate(f):
                if i >= max_lines:
                    lines.append(f"... (truncated at {max_lines} lines)")
                    break
                lines.append(line.rstrip("\n"))
        return "\n".join(lines)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

_TOOL_REGISTRY: dict[str, Any] = {
    "list_files": tool_list_files,
    "read_stats": tool_read_stats,
    "compare_metrics": tool_compare,
    "generate_table": tool_generate_table,
    "read_file_raw": tool_read_file_raw,
}


def execute_tool(name: str, arguments: dict[str, Any]) -> str:
    """Dispatch a tool call by name."""
    fn = _TOOL_REGISTRY.get(name)
    if fn is None:
        return json.dumps({"error": f"Unknown tool: {name}"})
    return fn(**arguments)


# ---------------------------------------------------------------------------
# OpenAI function-calling schema
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "list_files",
            "description": (
                "List files in a directory matching a glob pattern. "
                "Use this to discover summary statistics files."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "directory": {
                        "type": "string",
                        "description": "Absolute path to the directory to search.",
                    },
                    "pattern": {
                        "type": "string",
                        "description": "Glob pattern to filter files (e.g. *.csv, *.json, summary*).",
                        "default": "*",
                    },
                },
                "required": ["directory"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_stats",
            "description": (
                "Read and parse a summary statistics file. Supports CSV, TSV, "
                "JSON, and other formats. Returns structured metrics data."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Absolute path to the summary statistics file.",
                    },
                    "method_name": {
                        "type": "string",
                        "description": "Name/label for this method (e.g. scVI, Harmony).",
                        "default": "",
                    },
                },
                "required": ["file_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_metrics",
            "description": (
                "Compare metrics across multiple methods. Takes a list of "
                "parsed metrics dicts and returns a structured comparison."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "metrics_collection": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "List of metrics dicts from read_stats calls.",
                    },
                    "metric_names": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional subset of metric names to compare.",
                    },
                },
                "required": ["metrics_collection"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_table",
            "description": (
                "Generate a formatted comparison table from comparison data. "
                "Outputs markdown or CSV."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "comparison_data": {
                        "type": "object",
                        "description": "Comparison result from compare_metrics.",
                    },
                    "title": {
                        "type": "string",
                        "description": "Title for the table.",
                        "default": "Metric Comparison",
                    },
                    "format": {
                        "type": "string",
                        "enum": ["markdown", "csv"],
                        "description": "Output format.",
                        "default": "markdown",
                    },
                },
                "required": ["comparison_data"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_file_raw",
            "description": (
                "Read raw file content. Use this for files with unknown or "
                "unstructured formats to inspect their structure first."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Absolute path to the file.",
                    },
                    "max_lines": {
                        "type": "integer",
                        "description": "Maximum lines to read.",
                        "default": 200,
                    },
                },
                "required": ["file_path"],
            },
        },
    },
]
