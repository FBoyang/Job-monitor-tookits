"""
Configuration management for stats_summarizer.

API keys are loaded exclusively from environment variables.
Never store API keys in files within this directory.
"""

import os
import json
import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Config:
    """Application configuration."""
    openai_api_key: str
    openai_model: str = "gpt-4o"
    max_agent_steps: int = 20
    temperature: float = 0.0
    data_paths: list[str] = field(default_factory=list)
    output_dir: str = "outputs"
    verbose: bool = False
    # Azure OpenAI (optional; when set, use AzureOpenAI instead of OpenAI)
    azure_api_key: Optional[str] = None
    azure_endpoint: Optional[str] = None
    azure_api_version: str = "2025-03-01-preview"

    @property
    def use_azure(self) -> bool:
        """True if Azure OpenAI credentials are configured."""
        return bool(self.azure_api_key and self.azure_endpoint)

    @classmethod
    def from_env(cls, config_file: Optional[str] = None) -> "Config":
        """
        Load config from environment variables, optionally supplemented
        by a JSON config file (which must NOT contain API keys).
        Supports both OpenAI and Azure OpenAI.
        """
        # Prefer Azure if both endpoint and key are set
        azure_key = os.environ.get("AZURE_OPENAI_API_KEY")
        azure_endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT")
        openai_key = os.environ.get("OPENAI_API_KEY")

        if azure_key and azure_endpoint:
            kwargs = {
                "openai_api_key": azure_key,  # agent uses this for Azure too
                "azure_api_key": azure_key,
                "azure_endpoint": azure_endpoint.rstrip("/"),
                "azure_api_version": os.environ.get(
                    "AZURE_OPENAI_API_VERSION", "2025-03-01-preview"
                ),
            }
        elif openai_key:
            kwargs = {"openai_api_key": openai_key}
        else:
            raise EnvironmentError(
                "No API credentials found. Set one of:\n"
                "  OpenAI:   export OPENAI_API_KEY=your-key-here\n"
                "  Azure:    export AZURE_OPENAI_API_KEY=... AZURE_OPENAI_ENDPOINT=https://...\n"
                "Add to ~/.bashrc or ~/.bash_profile"
            )

        # Load non-secret settings from optional config file
        if config_file:
            cfg_path = Path(config_file)
            if cfg_path.exists():
                with open(cfg_path) as f:
                    file_cfg = json.load(f)
                # Safety: never allow API keys from file
                for key in (
                    "openai_api_key", "api_key",
                    "azure_api_key", "azure_endpoint",
                ):
                    file_cfg.pop(key, None)
                kwargs.update(file_cfg)

        return cls(**kwargs)


def validate_no_secrets_in_dir(directory: str = ".") -> list[str]:
    """Scan directory for potential API key leaks. Returns list of warnings."""
    warnings = []
    # Real keys: sk- followed by 20+ alphanumeric (not placeholders like sk-your-key-here)
    openai_key_re = re.compile(r"sk-[a-zA-Z0-9]{20,}")
    # Azure-style: 32-char hex key in assignment (not placeholders like your-azure-key-here)
    azure_key_re = re.compile(
        r"AZURE_OPENAI_API_KEY\s*=\s*['\"]?[a-fA-F0-9]{32}['\"]?"
    )
    suspicious_patterns = [openai_key_re, azure_key_re]
    skip_dirs = {".git", "__pycache__", ".venv", "venv", "node_modules"}
    skip_files = {".env.example", ".env"}

    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d not in skip_dirs]
        for fname in files:
            if fname in skip_files:
                continue
            if fname.endswith((".py", ".json", ".yaml", ".yml", ".toml", ".cfg", ".ini", ".txt", ".md", ".sh")):
                fpath = os.path.join(root, fname)
                try:
                    with open(fpath) as f:
                        content = f.read()
                    for pat in suspicious_patterns:
                        if pat.search(content):
                            warnings.append(f"Potential API key found in {fpath}")
                            break
                except (OSError, UnicodeDecodeError):
                    pass
    return warnings
