import logging
import re
from typing import Any, Union

from global_config import GlobalConfig
from langfuse import observe
from langfuse.langchain import CallbackHandler
from opentelemetry.trace import SpanKind

logger = logging.getLogger(__name__)


def clean_json_response(response: str) -> str:
    """
    Clean LLM response by removing markdown code block formatting.

    Handles responses like:
    ```json
    {"key": "value"}
    ```

    Or:
    ```
    {"key": "value"}
    ```

    Args:
        response: The LLM response string that may contain markdown code blocks

    Returns:
        Cleaned JSON string without markdown formatting
    """
    if not response:
        return response

    # Remove ```json or ``` markers at the start and end
    # Pattern matches optional whitespace, code fence with optional 'json' (case-insensitive),
    # captures content, then matches closing code fence
    pattern = r"^\s*```(?:json)?\s*\n?(.*?)\n?```\s*$"
    match = re.match(pattern, response.strip(), re.DOTALL | re.IGNORECASE)

    if match:
        return match.group(1).strip()

    # If no markdown blocks found, return original response stripped
    return response.strip()


class TokenUsageLogger(CallbackHandler):
    """Callback handler to capture and log token usage from LangChain LLM calls."""

    def __init__(self):
        super().__init__()
        self.token_usage = {}

    def on_llm_end(self, response, **kwargs):
        """Extract token usage from LLM response."""
        if hasattr(response, "llm_output") and response.llm_output and "token_usage" in response.llm_output:
            self.token_usage = response.llm_output["token_usage"]


def openai_langfuse(service_name, input_data, tag: str, chat_llm, prompt, output_parser, llm_token_usage_counter=None, **observe_kwargs):
    @observe(name=service_name)
    def run(input_data: dict, **inner_kwargs):
        with GlobalConfig().tracer.start_as_current_span(
            service_name + "_" + tag, kind=SpanKind.CLIENT, attributes={"service.name": service_name}
        ) as span:
            span.set_attribute("llm.model", chat_llm.model_name)
            span.set_attribute("llm.provider", chat_llm.openai_api_base)
            span.set_attribute("llm.temperature", chat_llm.temperature)

            # Create a callback handler to capture token usage
            token_logger = TokenUsageLogger()

            # Create Langfuse callback handler
            langfuse_handler = CallbackHandler()

            chain = prompt | chat_llm | output_parser
            llm_response = chain.bind(extra_body={"metadata": {"tags": [service_name, tag, "chain_1"]}}).invoke(
                input_data,
                config={
                    "callbacks": [token_logger, langfuse_handler],
                    "metadata": {"langfuse_tags": [service_name, tag]},
                    "tags": [service_name, tag, "chain_2"],
                },
                model_kwargs={"tags": [service_name, tag, "chain_3"]},
            )

            if token_logger.token_usage:
                logger.info(f"🎯 Token usage: {token_logger.token_usage}")
                span.set_attribute("llm.prompt_tokens", token_logger.token_usage["prompt_tokens"])
                span.set_attribute("llm.completion_tokens", token_logger.token_usage["completion_tokens"])
                span.set_attribute("llm.total_tokens", token_logger.token_usage["total_tokens"])

                if llm_token_usage_counter:
                    llm_token_usage_counter.add(token_logger.token_usage["total_tokens"])

            return llm_response

    return run(input_data, **observe_kwargs)


def format_prompt(prompt: Union[str, list[Any]], kwargs: dict) -> Union[str, list[Any]]:
    from langchain_core.prompts import ChatPromptTemplate

    # Handle different prompt template formats
    # Check if it's a ChatPromptTemplate (from langchain)
    if isinstance(prompt, ChatPromptTemplate):
        # Convert ChatPromptTemplate to messages format
        messages = prompt.format_messages(**kwargs)
    # Check if it's a string (simple prompt template)
    elif isinstance(prompt, str):
        # Format the string directly
        formatted_prompt = prompt.format(**kwargs)
        messages = formatted_prompt
    # Check if it's a list of tuples (role, content)
    elif isinstance(prompt, list):
        # Format each message in the prompt template
        messages = []
        for role, content in prompt:
            try:
                formatted_content = content.format(**kwargs)
                messages.append((role, formatted_content))
            except KeyError as e:
                logger.error(
                    f"💥 KeyError formatting prompt: "
                    f"Missing key: {e}, Available keys: {list(kwargs.keys())}, "
                    f"Content preview: {content[:200] if content else 'None'}"
                )
                raise
            except Exception as e:
                logger.error(f"💥 Error formatting prompt: " f"{type(e).__name__}: {str(e)}, Content preview: {content[:200] if content else 'None'}")
                raise
    else:
        raise ValueError(f"Unexpected prompt template type: {type(prompt)} ")

    return messages
