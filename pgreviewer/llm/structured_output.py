from __future__ import annotations

import json
import logging
from json import JSONDecodeError
from typing import TYPE_CHECKING

from pydantic import BaseModel, ValidationError

from pgreviewer.exceptions import StructuredOutputError

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pgreviewer.llm.client import LLMClient


def _strip_code_fences(text: str) -> str:
    """Remove markdown code fences that LLMs add despite being told not to."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        # drop opening fence (```json or ```)
        lines = lines[1:]
        # drop closing fence if present
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


def generate_structured[T: BaseModel](
    client: LLMClient,
    prompt: str,
    response_model: type[T],
    category: str,
    estimated_tokens: int,
) -> T:
    schema = json.dumps(response_model.model_json_schema(), sort_keys=True)
    base_prompt = (
        f"{prompt}\n\n"
        "Respond ONLY with a JSON object matching this schema: "
        f"{schema}. No markdown, no explanation."
    )

    current_prompt = base_prompt
    last_error: ValidationError | JSONDecodeError | None = None
    for attempt in range(3):
        response_text = client.generate(
            current_prompt,
            category=category,
            estimated_tokens=estimated_tokens,
        )
        logger.info(
            "[structured_output] attempt=%d raw=%r", attempt, response_text[:200]
        )
        response_text = _strip_code_fences(response_text)
        try:
            payload = json.loads(response_text)
            return response_model.model_validate(payload)
        except (ValidationError, JSONDecodeError) as error:
            last_error = error
            if attempt == 2:
                break
            current_prompt = (
                f"{base_prompt}\n\n"
                "Your previous response could not be parsed against the schema. "
                f"Error: {error}. "
                "Return ONLY a corrected JSON object."
            )

    raise StructuredOutputError(
        f"Failed to generate structured output after retries: {last_error}"
    )
