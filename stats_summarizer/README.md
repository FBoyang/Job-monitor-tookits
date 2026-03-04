# stats_summarizer

An **agentic framework** for comparing cell evaluation summary statistics across different methods. Powered by OpenAI function-calling (via HMS Azure OpenAI), the agent autonomously discovers, reads, and compares metrics from summary statistics files, producing comprehensive comparison reports.

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
┌─────────────┐     ┌──────────────┐     ┌────────────────────┐
│   main.py   │────>│   agent.py   │────>│  Azure OpenAI      │
│  (CLI)      │     │ (agentic     │     │  (gpt-5 with       │
│             │     │  loop)       │<────│  function calling)  │
└─────────────┘     └──────┬───────┘     └────────────────────┘
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

**Agent loop**: The agent receives a query, calls tools iteratively (list files -> read stats -> compare -> generate table), and returns a final markdown report. It self-terminates when satisfied or after `max_steps`.

---

## Setup

### Prerequisites

- Python 3.10+
- HMS Azure OpenAI API key (get from https://hu.sharepoint.com/sites/azureai)

### Installation

```bash
cd /n/home12/bof695/holylfs06/Users/bof695/auxiliary/Job-monitor-tookits/stats_summarizer

# Option A: use an existing conda env with openai installed
conda activate borzoi_env
pip install openai>=1.0.0

# Option B: create a virtual environment
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## API Key Management

**The API key is NEVER stored in this directory.** It is loaded exclusively from environment variables.

### HMS Azure OpenAI setup (recommended)

Add these to your `~/.bashrc`:

```bash
# HMS Azure OpenAI
export AZURE_OPENAI_API_KEY="your-32-character-key"
export AZURE_OPENAI_ENDPOINT="https://azure-ai.hms.edu"
```

Then reload:

```bash
source ~/.bashrc
```

> **IMPORTANT**: Use the **production** endpoint `https://azure-ai.hms.edu` (NOT `azure-ai-dev.hms.edu`).
> The dev endpoint is blocked by WAF from cluster IPs.

### Available HMS Azure deployments

| Deployment | Description |
|---|---|
| `gpt-5` | Most capable (default) |
| `gpt-5-mini` | Faster and cheaper |

See the [HMS Azure AI Model Catalog](https://hu.sharepoint.com/sites/azureai) for the latest.

### Alternative: direct OpenAI

```bash
export OPENAI_API_KEY="sk-..."
python main.py --paths /data/results --model gpt-4o
```

### Verify no secrets are leaked

```bash
python main.py --check-secrets
```

### SLURM job script example

```bash
#\!/bin/bash
#SBATCH --job-name=stats_summarizer
#SBATCH --output=stats_%j.out
#SBATCH --time=00:30:00
#SBATCH --mem=4G
#SBATCH --partition=gpu

source ~/.bashrc          # loads AZURE_OPENAI_API_KEY + AZURE_OPENAI_ENDPOINT
conda activate borzoi_env

python /n/holylfs06/LABS/mzitnik_lab/Users/bof695/auxiliary/Job-monitor-tookits/stats_summarizer/main.py \
    --paths /path/to/method1/results /path/to/method2/results \
    --output comparison_report.md
```

---

## Usage

### Basic usage

```bash
# Compare methods across directories (uses gpt-5 by default)
python main.py --paths /path/to/method1/results /path/to/method2/results

# Custom analysis query
python main.py --paths /path/to/results \
  --query "Compare only clustering metrics (ARI, NMI, ASW) across all methods"

# Save report to file
python main.py --paths /path/to/results --output comparison_report.md

# Use gpt-5-mini for faster/cheaper runs
python main.py --paths /path/to/results --model gpt-5-mini

# Verbose mode (shows agent reasoning and tool calls)
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
    "azure_deployment": "gpt-5",
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
| `--model` | `-m` | Deployment name (default: gpt-5 for Azure) |
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

The agent can also use the `read_file_raw` tool to inspect unknown formats and adapt its parsing strategy.

---

## Adding Custom Readers

1. Create a new reader class in `readers/`:

```python
# readers/my_reader.py
from readers.base import BaseReader
from typing import Any

class MyReader(BaseReader):
    def read(self, file_path: str) -> dict[str, Any]:
        # Parse your file format here
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

| Variable | Required | Default | Description |
|---|---|---|---|
| `AZURE_OPENAI_API_KEY` | Yes* | | HMS Azure API key (32-char) |
| `AZURE_OPENAI_ENDPOINT` | No | `https://azure-ai.hms.edu` | Azure endpoint |
| `AZURE_OPENAI_DEPLOYMENT` | No | `gpt-5` | Model deployment name |
| `AZURE_OPENAI_API_VERSION` | No | `2025-03-01-preview` | API version |
| `OPENAI_API_KEY` | Yes* | | Direct OpenAI key (alternative) |

*One of `AZURE_OPENAI_API_KEY` or `OPENAI_API_KEY` must be set.

### Config file fields (JSON)

| Field | Type | Default | Description |
|---|---|---|---|
| `data_paths` | list[str] | `[]` | Directories to search |
| `azure_deployment` | str | `"gpt-5"` | Azure model deployment |
| `openai_model` | str | `"gpt-4o"` | Model (direct OpenAI only) |
| `max_agent_steps` | int | `20` | Max tool-calling iterations |
| `temperature` | float | `0.0` | LLM temperature (ignored for reasoning models) |
| `output_dir` | str | `"outputs"` | Default output directory |

---

## Project Structure

```
stats_summarizer/
├── main.py              # CLI entry point
├── agent.py             # OpenAI agentic orchestrator (function-calling loop)
├── tools.py             # 5 tools: list_files, read_stats, compare_metrics,
│                        #          generate_table, read_file_raw
├── config.py            # Config, env variable management, secret scanner
├── comparator.py        # Metric alignment + summary stats (mean/std/best/worst)
├── reporter.py          # Markdown & CSV table generation
├── readers/
│   ├── __init__.py      # Reader registry & auto-selection
│   ├── base.py          # Abstract interface
│   ├── csv_reader.py    # CSV/TSV (key-value & tabular)
│   ├── json_reader.py   # JSON (flat, nested, list)
│   └── txt_reader.py    # Plain text key=value extraction
├── examples/            # Example data files (for testing)
├── outputs/             # Generated reports (gitignored)
├── requirements.txt     # openai>=1.0.0
├── .env.example         # Env var setup instructions (no real keys)
├── .gitignore
└── README.md
```

### Module responsibilities

| Module | Role |
|---|---|
| `main.py` | Parses CLI args, loads config, runs agent, saves output |
| `agent.py` | Agentic loop: sends messages to OpenAI, processes tool calls, produces report |
| `tools.py` | Defines 5 agent tools with OpenAI schemas + Python implementations |
| `config.py` | Loads API key from env only, merges optional config file, scans for secrets |
| `comparator.py` | Aligns metrics across methods, computes mean/std/best/worst |
| `reporter.py` | Formats comparison data into markdown or CSV tables |
| `readers/` | Pluggable readers auto-selected by file extension |

---

## Examples

### Comparing scRNA-seq integration methods

```bash
python main.py \
  --paths /results/scVI /results/Harmony /results/scanorama /results/LIGER \
  --query "Compare all batch correction and bio-conservation metrics" \
  --output integration_comparison.md
```

### Example output

```markdown
## Metric Comparison

| Metric      | scVI     | Harmony  | scanorama | LIGER    | Best      |
|-------------|----------|----------|-----------|----------|-----------|
| ARI         | **0.82** | 0.75     | 0.71      | 0.68     | scVI      |
| NMI         | **0.79** | 0.73     | 0.70      | 0.65     | scVI      |
| ASW_label   | 0.55     | **0.61** | 0.52      | 0.48     | Harmony   |
| Batch_ASW   | 0.72     | 0.68     | **0.75**  | 0.70     | scanorama |

### Summary Statistics

| Metric | Mean   | Std    | Best   | Worst  |
|--------|--------|--------|--------|--------|
| ARI    | 0.7400 | 0.0616 | 0.8200 | 0.6800 |
| NMI    | 0.7175 | 0.0585 | 0.7900 | 0.6500 |
```

---

## Extending the Framework

### Adding new agent tools

1. Write the Python function in `tools.py`
2. Add the OpenAI function schema to `TOOL_DEFINITIONS`
3. Register in `_TOOL_REGISTRY`

### Customizing agent behavior

Edit `StatsAgent.SYSTEM_PROMPT` in `agent.py` to change how the agent approaches analysis (e.g., prioritize certain metrics, change reporting style).

### Switching models

```bash
python main.py --paths /data/results --model gpt-5-mini   # faster/cheaper
python main.py --paths /data/results --model gpt-5         # most capable (default)
```

---

## Troubleshooting

| Issue | Solution |
|---|---|
| `No API credentials found` | Set `AZURE_OPENAI_API_KEY` in `~/.bashrc` and `source ~/.bashrc` |
| `503 Service Unavailable` / Incapsula | Use production endpoint: `export AZURE_OPENAI_ENDPOINT=https://azure-ai.hms.edu` (NOT `azure-ai-dev.hms.edu`) |
| `Unsupported parameter: temperature` | gpt-5 is a reasoning model; the code auto-detects this and skips temperature |
| `Unsupported parameter: max_tokens` | gpt-5 uses `max_completion_tokens`; the openai SDK handles this |
| Agent loops without producing output | Increase `--max-steps` or simplify `--query` |
| File format not recognized | Add a custom reader (see above) or use `--query` to guide the agent |
| Permission denied on data paths | Check file permissions with `ls -la` |
| Rate limit errors | Use `gpt-5-mini` or reduce `--max-steps` |
| Works on laptop but not cluster | Cluster may need production endpoint; dev endpoint is WAF-blocked |
