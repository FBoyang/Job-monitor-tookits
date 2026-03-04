"""
Agentic orchestrator using OpenAI function calling.

The agent iteratively reads summary statistics files, compares metrics
across methods, and generates structured comparison reports.

Supports both direct OpenAI and Azure OpenAI (HMS).
Handles reasoning models (gpt-5, o1, o3) which do not accept temperature.
"""

import json
import logging
from typing import Any, Optional
from openai import AzureOpenAI, OpenAI

from config import Config
from tools import TOOL_DEFINITIONS, execute_tool

logger = logging.getLogger(__name__)


class StatsAgent:
    """
    An agentic loop powered by OpenAI function calling.

    The agent receives a user query, plans which tools to call,
    executes them, and synthesizes the results into a comparison report.
    """

    SYSTEM_PROMPT = """You are an expert biostatistics analyst specializing in cell evaluation benchmarks.

Your job is to:
1. Discover and read summary statistics files from the provided data paths.
2. Parse metrics for each method/approach.
3. Produce a comprehensive metric-by-metric comparison across all methods.
4. Format the results as clear markdown tables.

Guidelines:
- Always start by listing available files in the data paths.
- Read all relevant summary statistics files before making comparisons.
- Report ALL metrics found, not just a subset.
- Clearly label which method/approach each result comes from.
- If a metric is missing for a method, note it as "N/A".
- Provide a brief interpretation after each comparison table.
- Be precise with numbers; do not round unless asked.
"""

    def __init__(self, config: Config):
        self.config = config

        if config.use_azure:
            self.client = AzureOpenAI(
                api_key=config.azure_api_key,
                azure_endpoint=config.azure_endpoint,
                api_version=config.azure_api_version,
            )
            self.model = config.azure_deployment  # e.g. "gpt-5"
            logger.info(
                f"Using Azure OpenAI: {config.azure_endpoint} / {self.model}"
            )
        else:
            self.client = OpenAI(api_key=config.openai_api_key)
            self.model = config.openai_model
            logger.info(f"Using OpenAI: {self.model}")

        self.is_reasoning = config.is_reasoning_model
        self.max_steps = config.max_agent_steps
        self.messages: list[dict[str, Any]] = []
        self._step_count = 0

    def _build_create_kwargs(self, *, force_answer: bool = False) -> dict:
        """
        Build kwargs for chat.completions.create(), handling differences
        between reasoning models (gpt-5, o-series) and standard models.
        """
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": self.messages,
        }

        if not force_answer:
            kwargs["tools"] = TOOL_DEFINITIONS
            kwargs["tool_choice"] = "auto"

        if self.is_reasoning:
            # Reasoning models do not accept temperature parameter
            pass
        else:
            kwargs["temperature"] = self.config.temperature

        return kwargs

    def _init_messages(self, user_query: str, data_paths: list[str]) -> None:
        """Initialize the conversation with system prompt and user query."""
        path_info = "\n".join(f"  - {p}" for p in data_paths)

        system_content = (
            f"{self.SYSTEM_PROMPT}\n\n"
            f"Available data paths to search:\n{path_info}"
        )

        if self.is_reasoning:
            # Reasoning models (gpt-5, o1, o3): use "developer" role
            self.messages = [
                {"role": "developer", "content": system_content},
                {"role": "user", "content": user_query},
            ]
        else:
            self.messages = [
                {"role": "system", "content": system_content},
                {"role": "user", "content": user_query},
            ]

    def run(
        self,
        query: str,
        data_paths: Optional[list[str]] = None,
    ) -> str:
        """
        Execute the agentic loop.

        Args:
            query: Natural language query describing what to compare.
            data_paths: List of directories to search for stats files.
                Falls back to config.data_paths if not provided.

        Returns:
            Final markdown report as a string.
        """
        paths = data_paths or self.config.data_paths
        if not paths:
            return "Error: No data paths provided. Specify paths via --paths or config."

        self._init_messages(query, paths)
        self._step_count = 0

        while self._step_count < self.max_steps:
            self._step_count += 1
            logger.info(f"Agent step {self._step_count}/{self.max_steps}")

            kwargs = self._build_create_kwargs()
            response = self.client.chat.completions.create(**kwargs)

            choice = response.choices[0]
            message = choice.message

            # Append assistant message to history
            self.messages.append(message.model_dump())

            # If no tool calls, the agent is done
            if not message.tool_calls:
                logger.info("Agent finished (no more tool calls)")
                return message.content or ""

            # Process each tool call
            for tool_call in message.tool_calls:
                fn_name = tool_call.function.name
                fn_args = json.loads(tool_call.function.arguments)

                logger.info(f"  Calling tool: {fn_name}({fn_args})")

                try:
                    result = execute_tool(fn_name, fn_args)
                except Exception as e:
                    result = f"Error executing {fn_name}: {e}"
                    logger.error(result)

                self.messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": str(result),
                })

        return self._force_final_answer()

    def _force_final_answer(self) -> str:
        """Force the agent to produce a final answer if max steps reached."""
        logger.warning("Max steps reached, forcing final answer")
        self.messages.append({
            "role": "user",
            "content": (
                "You have reached the maximum number of tool-calling steps. "
                "Please synthesize all the information gathered so far into "
                "a final comparison report with markdown tables."
            ),
        })
        kwargs = self._build_create_kwargs(force_answer=True)
        response = self.client.chat.completions.create(**kwargs)
        return response.choices[0].message.content or ""
