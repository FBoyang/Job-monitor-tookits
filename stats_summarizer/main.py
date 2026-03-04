#!/usr/bin/env python3
"""
stats_summarizer - CLI entry point.

An agentic framework for comparing cell evaluation summary statistics
across different methods.

Usage:
    python main.py --paths /path/to/method1 /path/to/method2
    python main.py --paths /path/to/results --query "Compare ARI and NMI"
    python main.py --config config.json
"""

import argparse
import logging
import sys
import os

from config import Config, validate_no_secrets_in_dir
from agent import StatsAgent
from reporter import save_report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare cell evaluation summary statistics across methods.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare all methods in two directories
  python main.py --paths /data/scVI/results /data/Harmony/results

  # Custom query
  python main.py --paths /data/results --query "Compare only clustering metrics"

  # Use a config file for paths
  python main.py --config myconfig.json

  # Save output to file
  python main.py --paths /data/results --output report.md
        """,
    )

    parser.add_argument(
        "--paths", "-p",
        nargs="+",
        help="Directories containing summary statistics files.",
    )
    parser.add_argument(
        "--query", "-q",
        default=(
            "Find all summary statistics files in the provided paths. "
            "Read each file and identify the method it corresponds to. "
            "Then produce a comprehensive metric-by-metric comparison "
            "table across all methods. Include ALL metrics found."
        ),
        help="Natural language query for the agent.",
    )
    parser.add_argument(
        "--config", "-c",
        help="Path to JSON config file (must not contain API keys).",
    )
    parser.add_argument(
        "--output", "-o",
        help="Save the report to this file path.",
    )
    parser.add_argument(
        "--model", "-m",
        default=None,
        help="OpenAI model to use (default: gpt-4o).",
    )
    parser.add_argument(
        "--max-steps",
        type=int,
        default=None,
        help="Maximum agent steps (default: 20).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging.",
    )
    parser.add_argument(
        "--check-secrets",
        action="store_true",
        help="Scan project directory for potential API key leaks and exit.",
    )

    args = parser.parse_args()

    # Secret scanning mode
    if args.check_secrets:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        warnings = validate_no_secrets_in_dir(script_dir)
        if warnings:
            print("WARNING: Potential secrets found:")
            for w in warnings:
                print(f"  - {w}")
            sys.exit(1)
        else:
            print("No potential secrets found in project directory.")
            sys.exit(0)

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Load config
    try:
        config = Config.from_env(config_file=args.config)
    except EnvironmentError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)

    # Override config with CLI args
    if args.paths:
        config.data_paths = args.paths
    if args.model:
        config.openai_model = args.model
    if args.max_steps:
        config.max_agent_steps = args.max_steps
    if args.verbose:
        config.verbose = True

    if not config.data_paths:
        print("Error: No data paths provided. Use --paths or --config.", file=sys.stderr)
        sys.exit(1)

    # Run the agent
    agent = StatsAgent(config)
    print("Starting analysis...\n")

    report = agent.run(query=args.query, data_paths=config.data_paths)

    # Display report
    print(report)

    # Save report if requested
    if args.output:
        output_dir = os.path.dirname(args.output) or "outputs"
        filename = os.path.basename(args.output)
        saved_path = save_report(report, output_dir=output_dir, filename=filename)
        print(f"\nReport saved to: {saved_path}")


if __name__ == "__main__":
    main()
