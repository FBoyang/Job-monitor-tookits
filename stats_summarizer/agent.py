"""
Agentic orchestrator using OpenAI function calling.

The agent iteratively reads summary statistics files, compares metrics
across methods, and generates structured comparison reports.
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
        else:
            self.client = OpenAI(api_key=config.openai_api_key)
        self.model = config.openai_model
        self.max_steps = config.max_agent_steps
        self.messages: list[dict[str, Any]] = []
        self._step_count = 0

    def _init_messages(self, user_query: str, data_paths: list[str]) -> None:
        """Initialize the conversation with system prompt and user query."""
        path_info = "\n".join(f"  - {p}" for p in data_paths)
        system_msg = (
            f"{self.SYSTEM_PROMPT}\n\n"
            f"Available data paths to search:\n{path_info}"
        )
        self.messages = [
            {"role": "system", "content": system_msg},
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

            response = self.client.chat.completions.create(
                model=self.model,
                messages=self.messages,
                tools=TOOL_DEFINITIONS,
                tool_choice="auto",
                temperature=self.config.temperature,
            )

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

                self.messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": str(result),
                })

        return self._force_final_answer()

    def _force_final_answer(self) -> str:
        """Force the agent to produce a final answer if max steps reached."""
        self.messages.append({
            "role": "user",
            "content": (
                "You have reached the maximum number of tool-calling steps. "
                "Please synthesize all the information gathered so far into "
                "a final comparison report with markdown tables."
            ),
        })
        response = self.client.chat.completions.create(
            model=self.model,
            messages=self.messages,
            temperature=self.config.temperature,
        )
        return response.choices[0].message.content or ""
