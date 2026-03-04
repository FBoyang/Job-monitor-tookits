"""
Configuration management for stats_summarizer.

API keys are loaded exclusively from environment variables.
Never store API keys in files within this directory.

Required environment variables for Azure OpenAI (HMS):
    export AZURE_OPENAI_API_KEY="your-32-char-key"
    export AZURE_OPENAI_ENDPOINT="https://azure-ai.hms.edu"
"""

import os
import json
import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


# -- Defaults -----------------------------------------------------------------
DEFAULT_AZURE_ENDPOINT = "https://azure-ai.hms.edu"
DEFAULT_AZURE_API_VERSION = "2025-03-01-preview"
DEFAULT_AZURE_DEPLOYMENT = "gpt-5"
DEFAULT_OPENAI_MODEL = "gpt-4o"


@dataclass
class Config:
    """Application configuration."""
    openai_api_key: str = ""
    openai_model: str = DEFAULT_OPENAI_MODEL
    max_agent_steps: int = 20
    temperature: float = 0.0
    data_paths: list[str] = field(default_factory=list)
    output_dir: str = "outputs"
    verbose: bool = False

    # Azure OpenAI
    azure_api_key: Optional[str] = None
    azure_endpoint: Optional[str] = None
    azure_api_version: str = DEFAULT_AZURE_API_VERSION
    azure_deployment: str = DEFAULT_AZURE_DEPLOYMENT

    @property
    def use_azure(self) -> bool:
        """True if Azure OpenAI credentials are configured."""
        return bool(self.azure_api_key and self.azure_endpoint)

    @property
    def is_reasoning_model(self) -> bool:
        """True if the model is a reasoning model (o1, o3, gpt-5, etc.)."""
        model = self.azure_deployment if self.use_azure else self.openai_model
        reasoning_prefixes = ("o1", "o3", "o4", "gpt-5")
        return any(model.startswith(p) for p in reasoning_prefixes)

    @classmethod
    def from_env(cls, config_file: Optional[str] = None) -> "Config":
        """
        Load config from environment variables, optionally supplemented
        by a JSON config file (which must NOT contain API keys).

        Azure is preferred if AZURE_OPENAI_API_KEY is set.
        Falls back to OPENAI_API_KEY for direct OpenAI usage.
        """
        azure_key = os.environ.get("AZURE_OPENAI_API_KEY")
        azure_endpoint = os.environ.get(
            "AZURE_OPENAI_ENDPOINT", DEFAULT_AZURE_ENDPOINT
        )
        openai_key = os.environ.get("OPENAI_API_KEY")

        if azure_key:
            kwargs = {
                "openai_api_key": "",
                "azure_api_key": azure_key,
                "azure_endpoint": azure_endpoint.rstrip("/"),
                "azure_api_version": os.environ.get(
                    "AZURE_OPENAI_API_VERSION", DEFAULT_AZURE_API_VERSION
                ),
                "azure_deployment": os.environ.get(
                    "AZURE_OPENAI_DEPLOYMENT", DEFAULT_AZURE_DEPLOYMENT
                ),
            }
        elif openai_key:
            kwargs = {"openai_api_key": openai_key}
        else:
            raise EnvironmentError(
                "No API credentials found. Set one of:\n"
                "  Azure (HMS):  export AZURE_OPENAI_API_KEY=your-key\n"
                "                export AZURE_OPENAI_ENDPOINT=https://azure-ai.hms.edu\n"
                "  OpenAI:       export OPENAI_API_KEY=sk-...\n"
                "Add to ~/.bashrc or ~/.bash_profile for persistence."
            )

        if config_file:
            cfg_path = Path(config_file)
            if cfg_path.exists():
                with open(cfg_path) as f:
                    file_cfg = json.load(f)
                for secret_key in (
                    "openai_api_key", "api_key",
                    "azure_api_key", "azure_endpoint",
                ):
                    file_cfg.pop(secret_key, None)
                kwargs.update(file_cfg)

        return cls(**kwargs)

    def print_status(self) -> None:
        """Print current configuration (without secrets)."""
        if self.use_azure:
            print("  Backend:    Azure OpenAI")
            print(f"  Endpoint:   {self.azure_endpoint}")
            print(f"  Deployment: {self.azure_deployment}")
            print(f"  API ver:    {self.azure_api_version}")
            print(f"  Reasoning:  {self.is_reasoning_model}")
        else:
            print("  Backend:    OpenAI (direct)")
            print(f"  Model:      {self.openai_model}")
        print(f"  Max steps:  {self.max_agent_steps}")
        print(f"  Data paths: {len(self.data_paths)}")


def validate_no_secrets_in_dir(directory: str = ".") -> list[str]:
    """Scan directory for potential API key leaks. Returns list of warnings."""
    warnings = []
    openai_key_re = re.compile(r"sk-[a-zA-Z0-9]{20,}")
    azure_key_re = re.compile(
        r"AZURE_OPENAI_API_KEY\s*=\s*[a-fA-F0-9]{32}"
    )
    suspicious_patterns = [openai_key_re, azure_key_re]
    skip_dirs = {".git", "__pycache__", ".venv", "venv", "node_modules"}
    skip_files = {".env.example"}

    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d not in skip_dirs]
        for fname in files:
            if fname in skip_files:
                continue
            if fname.endswith(
                (".py", ".json", ".yaml", ".yml", ".toml",
                 ".cfg", ".ini", ".txt", ".md", ".sh")
            ):
                fpath = os.path.join(root, fname)
                try:
                    with open(fpath) as f:
                        content = f.read()
                    for pat in suspicious_patterns:
                        if pat.search(content):
                            warnings.append(
                                f"Potential API key found in {fpath}"
                            )
                            break
                except (OSError, UnicodeDecodeError):
                    pass
    return warnings
