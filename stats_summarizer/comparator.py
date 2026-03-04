"""
Metric comparison engine.

Takes parsed metrics from multiple methods and produces structured
comparison data suitable for table generation.
"""

from typing import Any, Optional
import statistics


def compare_metrics(
    metrics_collection: list[dict[str, Any]],
    metric_names: Optional[list[str]] = None,
) -> dict[str, Any]:
    """
    Compare metrics across multiple methods.

    Args:
        metrics_collection: List of dicts, each with at minimum:
            - "metrics": dict[str, numeric]
            - "method_name" or "file_path": identifier
        metric_names: Optional subset of metrics to compare.

    Returns:
        Structured comparison dict with:
            - methods: list of method names
            - metrics: list of metric names
            - table: dict[metric_name][method_name] -> value
            - summary: per-metric stats (best, worst, mean, std)
    """
    if not metrics_collection:
        return {"error": "No metrics data provided."}

    # Extract method names
    methods = []
    for entry in metrics_collection:
        name = entry.get("method_name", "")
        if not name:
            # Derive from file path
            fp = entry.get("file_path", f"method_{len(methods)}")
            name = _derive_method_name(fp)
        methods.append(name)

    # Collect all metric names
    all_metrics: set[str] = set()
    for entry in metrics_collection:
        m = entry.get("metrics", {})
        all_metrics.update(m.keys())

    # Filter to requested metrics if specified
    if metric_names:
        all_metrics = all_metrics.intersection(set(metric_names))

    sorted_metrics = sorted(all_metrics)

    # Build comparison table
    table: dict[str, dict[str, Any]] = {}
    for metric in sorted_metrics:
        table[metric] = {}
        for i, entry in enumerate(metrics_collection):
            val = entry.get("metrics", {}).get(metric, "N/A")
            table[metric][methods[i]] = val

    # Compute summary statistics per metric
    summary: dict[str, dict[str, Any]] = {}
    for metric in sorted_metrics:
        values = []
        for method in methods:
            v = table[metric][method]
            if isinstance(v, (int, float)):
                values.append(v)

        stats: dict[str, Any] = {"num_reported": len(values)}
        if values:
            stats["mean"] = round(statistics.mean(values), 6)
            stats["best"] = round(max(values), 6)
            stats["worst"] = round(min(values), 6)
            stats["best_method"] = methods[
                next(
                    i for i, entry in enumerate(metrics_collection)
                    if entry.get("metrics", {}).get(metric) == max(values)
                )
            ]
            if len(values) > 1:
                stats["std"] = round(statistics.stdev(values), 6)
        summary[metric] = stats

    return {
        "methods": methods,
        "metrics": sorted_metrics,
        "table": table,
        "summary": summary,
    }


def _derive_method_name(file_path: str) -> str:
    """Derive a method name from a file path."""
    import os
    basename = os.path.basename(file_path)
    name = os.path.splitext(basename)[0]
    # Try extracting from parent directory
    parent = os.path.basename(os.path.dirname(file_path))
    if parent and parent not in (".", ""):
        return parent
    return name
