# stats_summarizer

An **agentic framework** for comparing cell evaluation summary statistics across different methods. Powered by OpenAI function-calling, the agent autonomously discovers, reads, and compares metrics from summary statistics files, producing comprehensive comparison reports.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Setup](#setup)
- [API Key Management](#api-key-management)
- [Usage](#usage)
- [Supported File Formats](#supported-file-formats)
- [Adding Custom Readers](#adding-custom-readers)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [Examples](#examples)
- [Extending the Framework](#extending-the-framework)
- [Troubleshooting](#troubleshooting)

---

## Overview

`stats_summarizer` automates the tedious process of collecting evaluation metrics from multiple methods and producing side-by-side comparisons. Instead of manually parsing files and building tables, you point the agent at your results directories and it:

1. **Discovers** all summary statistics files in the given paths
2. **Reads and parses** each file (CSV, TSV, JSON, or plain text)
3. **Identifies** which method each file corresponds to
4. **Compares** all metrics across methods
5. **Generates** formatted markdown tables with summary statistics (mean, std, best/worst)

The agent uses OpenAI function-calling to plan and execute these steps autonomously, adapting to whatever file formats and directory structures it encounters.

---

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌────────────────┐
│   main.py   │────>│   agent.py   │────>│  OpenAI API    │
│  (CLI)      │     │ (agentic     │     │  (gpt-4o with  │
│             │     │  loop)       │<────│  tool calling) │
└─────────────┘     └──────┬───────┘     └────────────────┘
                           │
                    ┌──────┴───────┐
                    │   tools.py   │
                    │  (dispatch)  │
                    └──┬───┬───┬───┘
                       │   │   │
              ┌────────┘   │   └────────┐
              v            v            v
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ readers/ │ │comparator│ │ reporter  │
        │(CSV,JSON,│ │  .py     │ │  .py      │
        │ TXT)     │ │          │ │           │
        └──────────┘ └──────────┘ └──────────┘
```

**Agent loop**: The agent receives a query, calls tools iteratively (list files → read stats → compare → generate table), and returns a final markdown report. It self-terminates when satisfied or after `max_steps`.

---

## Setup

### Prerequisites

- Python 3.10+
- An OpenAI API key or Azure OpenAI credentials

### Installation

```bash
cd /path/to/stats_summarizer

# Create a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## API Key Management

**API keys are NEVER stored in this directory.** They are loaded exclusively from environment variables.

### Option 1: OpenAI (standard)

```bash
# Add to ~/.bashrc or ~/.bash_profile
export OPENAI_API_KEY=sk-your-openai-key-here
source ~/.bashrc
```

### Option 2: Azure OpenAI (e.g. HMS azure-ai.hms.edu)

Azure uses the `api-key` header. Set both variables:

```bash
export AZURE_OPENAI_API_KEY=your-azure-key-here
export AZURE_OPENAI_ENDPOINT=https://your-endpoint.example.com
```

For Azure, use `--model` to specify your deployment name (e.g. `gpt-5`):

```bash
python main.py --paths /path/to/results --model gpt-5 --output report.md
```

### Option 3: Set per-session

```bash
# OpenAI
export OPENAI_API_KEY="sk-your-key-here"
python main.py --paths /path/to/results

# Azure
export AZURE_OPENAI_API_KEY="your-key" AZURE_OPENAI_ENDPOINT="https://..."
python main.py --paths /path/to/results --model gpt-5
```

### Option 4: SLURM job script

```bash
#\!/bin/bash
#SBATCH --job-name=stats_summarizer
#SBATCH ...
export OPENAI_API_KEY="sk-your-key-here"   # or AZURE_OPENAI_* for Azure
python main.py --paths /path/to/results --output report.md
```

### Verify no secrets are leaked

```bash
python main.py --check-secrets
```

This scans all project files for potential API key patterns (OpenAI `sk-` and Azure-style keys).

---

## Usage

### Basic usage

```bash
# Compare methods across directories
python main.py --paths /path/to/method1/results /path/to/method2/results

# Custom analysis query
python main.py --paths /path/to/results \
  --query "Compare only clustering metrics (ARI, NMI, ASW) across all methods"

# Save report to file
python main.py --paths /path/to/results --output comparison_report.md

# Verbose mode (shows agent reasoning)
python main.py --paths /path/to/results --verbose
```

### Using a config file

Create a `myconfig.json` (no API keys\!):

```json
{
    "data_paths": [
        "/n/holylfs06/path/to/scVI/results",
        "/n/holylfs06/path/to/Harmony/results",
        "/n/holylfs06/path/to/scanorama/results"
    ],
    "openai_model": "gpt-4o",
    "max_agent_steps": 30,
    "output_dir": "outputs"
}
```

```bash
python main.py --config myconfig.json
```

### CLI options

| Option | Short | Description |
|---|---|---|
| `--paths` | `-p` | Directories containing summary statistics files |
| `--query` | `-q` | Natural language query for the agent |
| `--config` | `-c` | Path to JSON config file |
| `--output` | `-o` | Save report to this file |
| `--model` | `-m` | Model or Azure deployment name (default: gpt-4o) |
| `--max-steps` | | Max agent iterations (default: 20) |
| `--verbose` | `-v` | Enable debug logging |
| `--check-secrets` | | Scan for leaked API keys |

---

## Supported File Formats

| Format | Extensions | Detection |
|---|---|---|
| CSV | `.csv` | Auto-detects delimiter, key-value vs tabular |
| TSV | `.tsv` | Tab-delimited, same logic as CSV |
| JSON | `.json` | Flat dict, nested with "metrics" key, or list |
| Plain text | `.txt`, `.log` | Extracts `key: value` or `key = value` pairs |

The agent can also use the `read_file_raw` tool to inspect unknown formats and adapt its parsing.

---

## Adding Custom Readers

1. Create a new reader class in `readers/`:

```python
# readers/my_reader.py
from readers.base import BaseReader
from typing import Any

class MyReader(BaseReader):
    def read(self, file_path: str) -> dict[str, Any]:
        # Parse your file format
        metrics = {"ari": 0.85, "nmi": 0.72}
        return {
            "file_path": file_path,
            "metrics": metrics,
            "metadata": {"format": "my_format"},
        }
```

2. Register it in `readers/__init__.py`:

```python
from readers.my_reader import MyReader
READER_REGISTRY[".myext"] = MyReader
```

---

## Configuration

### Environment variables

| Variable | Required | Description |
|---|---|---|
| `OPENAI_API_KEY` | Yes* | Your OpenAI API key |
| `AZURE_OPENAI_API_KEY` | Yes* | Azure OpenAI API key (use with Azure) |
| `AZURE_OPENAI_ENDPOINT` | Yes* | Azure endpoint URL (e.g. `https://azure-ai.hms.edu`) |
| `AZURE_OPENAI_API_VERSION` | No | Azure API version (default: `2025-03-01-preview`) |

*Provide either `OPENAI_API_KEY` (for OpenAI) or both `AZURE_OPENAI_API_KEY` and `AZURE_OPENAI_ENDPOINT` (for Azure).

### Config file fields

| Field | Type | Default | Description |
|---|---|---|---|
| `data_paths` | list[str] | `[]` | Directories to search |
| `openai_model` | str | `"gpt-4o"` | Model for the agent |
| `max_agent_steps` | int | `20` | Max tool-calling iterations |
| `temperature` | float | `0.0` | LLM temperature |
| `output_dir` | str | `"outputs"` | Default output directory |
| `verbose` | bool | `false` | Enable debug logging |

---

## Project Structure

```
stats_summarizer/
├── main.py              # CLI entry point
├── agent.py             # OpenAI agentic orchestrator (function-calling loop)
├── tools.py             # Tool definitions + dispatch registry
├── config.py            # Configuration & env variable management
├── comparator.py        # Metric comparison engine
├── reporter.py          # Table/report generation (markdown, CSV)
├── readers/
│   ├── __init__.py      # Reader registry & auto-selection
│   ├── base.py          # Abstract base reader
│   ├── csv_reader.py    # CSV/TSV parser
│   ├── json_reader.py   # JSON parser
│   └── txt_reader.py    # Plain text key-value parser
├── examples/            # Example data files (for testing)
├── outputs/             # Generated reports (gitignored)
├── requirements.txt
├── .env.example         # Instructions for setting up API key
├── .gitignore
└── README.md
```

### Module responsibilities

- **`main.py`**: Parses CLI args, loads config, runs the agent, saves output
- **`agent.py`**: Implements the agentic loop — sends messages to OpenAI, processes tool calls, accumulates context, and produces a final report
- **`tools.py`**: Defines the 5 tools the agent can call (list_files, read_stats, compare_metrics, generate_table, read_file_raw) with both OpenAI schemas and Python implementations
- **`config.py`**: Loads API key from env, merges with optional config file, includes a secret-scanning utility
- **`comparator.py`**: Pure Python metric comparison — aligns metrics across methods, computes summary stats (mean, std, best, worst)
- **`reporter.py`**: Formats comparison data into markdown tables or CSV; handles file saving
- **`readers/`**: Pluggable readers for different file formats, auto-selected by extension

---

## Examples

### Example: comparing scRNA-seq integration methods

```bash
python main.py \
  --paths /results/scVI /results/Harmony /results/scanorama /results/LIGER \
  --query "Compare all batch correction and bio-conservation metrics" \
  --output integration_comparison.md
```

### Example output

```markdown
## Metric Comparison

| Metric           | scVI     | Harmony  | scanorama | LIGER    | Best      |
|------------------|----------|----------|-----------|----------|-----------|
| ARI              | **0.82** | 0.75     | 0.71      | 0.68     | scVI      |
| NMI              | **0.79** | 0.73     | 0.70      | 0.65     | scVI      |
| ASW_label        | 0.55     | **0.61** | 0.52      | 0.48     | Harmony   |
| Batch_ASW        | 0.72     | 0.68     | **0.75**  | 0.70     | scanorama |
| iLISI            | 0.85     | 0.82     | **0.88**  | 0.80     | scanorama |

### Summary Statistics

| Metric      | Mean   | Std    | Best   | Worst  |
|-------------|--------|--------|--------|--------|
| ARI         | 0.7400 | 0.0616 | 0.8200 | 0.6800 |
| NMI         | 0.7175 | 0.0585 | 0.7900 | 0.6500 |
| ...
```

---

## Extending the Framework

### Adding new agent tools

1. Add the Python function in `tools.py`
2. Add the OpenAI schema to `TOOL_DEFINITIONS`
3. Register in `_TOOL_REGISTRY`

### Customizing the agent behavior

Edit `StatsAgent.SYSTEM_PROMPT` in `agent.py` to change how the agent approaches the analysis (e.g., prioritize certain metrics, change comparison style).

### Using a different model

```bash
python main.py --paths /data/results --model gpt-4o-mini  # cheaper, faster
python main.py --paths /data/results --model gpt-4-turbo  # more capable
```

---

## Troubleshooting

| Issue | Solution |
|---|---|
| `No API credentials found` | Set `OPENAI_API_KEY` or `AZURE_OPENAI_API_KEY` + `AZURE_OPENAI_ENDPOINT` |
| Agent loops without producing output | Increase `--max-steps` or simplify `--query` |
| File format not recognized | Add a custom reader (see above) or use `--query` to instruct the agent |
| Permission denied on data paths | Check file permissions with `ls -la` |
| Rate limit errors | Use `gpt-4o-mini` or reduce `--max-steps` |
