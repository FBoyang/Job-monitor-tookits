"""
Report and table generation utilities.

Converts structured comparison data into human-readable formats
(markdown tables, CSV, and saved report files).
"""

import csv
import io
import os
from datetime import datetime
from typing import Any


def format_table(
    comparison_data: dict[str, Any],
    title: str = "Metric Comparison",
    fmt: str = "markdown",
) -> str:
    """
    Format comparison data as a table string.

    Args:
        comparison_data: Output from compare_metrics().
        title: Table title.
        fmt: "markdown" or "csv".

    Returns:
        Formatted table as a string.
    """
    if "error" in comparison_data:
        return f"Error: {comparison_data[error]}"

    methods = comparison_data["methods"]
    metrics = comparison_data["metrics"]
    table = comparison_data["table"]
    summary = comparison_data.get("summary", {})

    if fmt == "csv":
        return _format_csv(methods, metrics, table)

    return _format_markdown(methods, metrics, table, summary, title)


def _format_markdown(
    methods: list[str],
    metrics: list[str],
    table: dict,
    summary: dict,
    title: str,
) -> str:
    """Render a markdown comparison table."""
    lines = [f"## {title}", ""]

    # Header
    header = "| Metric | " + " | ".join(methods) + " | Best |"
    sep = "|" + "---|" * (len(methods) + 2)
    lines.extend([header, sep])

    # Data rows
    for metric in metrics:
        row_vals = []
        best_method = summary.get(metric, {}).get("best_method", "")
        for method in methods:
            val = table[metric].get(method, "N/A")
            cell = _format_value(val)
            # Bold the best value
            if method == best_method and isinstance(val, (int, float)):
                cell = f"**{cell}**"
            row_vals.append(cell)
        best_display = best_method if best_method else "-"
        lines.append(f"| {metric} | " + " | ".join(row_vals) + f" | {best_display} |")

    # Summary statistics
    lines.extend(["", "### Summary Statistics", ""])
    lines.append("| Metric | Mean | Std | Best | Worst |")
    lines.append("|---|---|---|---|---|")
    for metric in metrics:
        s = summary.get(metric, {})
        mean = _format_value(s.get("mean", "N/A"))
        std = _format_value(s.get("std", "N/A"))
        best = _format_value(s.get("best", "N/A"))
        worst = _format_value(s.get("worst", "N/A"))
        lines.append(f"| {metric} | {mean} | {std} | {best} | {worst} |")

    return "\n".join(lines)


def _format_csv(methods: list[str], metrics: list[str], table: dict) -> str:
    """Render a CSV comparison table."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Metric"] + methods)
    for metric in metrics:
        row = [metric]
        for method in methods:
            row.append(table[metric].get(method, "N/A"))
        writer.writerow(row)
    return output.getvalue()


def _format_value(val: Any) -> str:
    """Format a single value for display."""
    if isinstance(val, float):
        if abs(val) < 0.001 or abs(val) > 10000:
            return f"{val:.4e}"
        return f"{val:.4f}"
    return str(val)


def save_report(
    content: str,
    output_dir: str = "outputs",
    filename: str = "",
) -> str:
    """
    Save a report to a file.

    Args:
        content: Report content (markdown string).
        output_dir: Directory to save to.
        filename: Optional filename. Auto-generated if empty.

    Returns:
        Path to saved file.
    """
    os.makedirs(output_dir, exist_ok=True)
    if not filename:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"comparison_report_{ts}.md"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w") as f:
        f.write(content)
    return filepath
