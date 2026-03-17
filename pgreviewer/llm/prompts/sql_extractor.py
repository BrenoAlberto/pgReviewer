from __future__ import annotations

from pydantic import BaseModel

from pgreviewer.core.models import ExtractedQuery
from pgreviewer.llm.client import LLMClient
from pgreviewer.llm.structured_output import generate_structured

OUTPUT_TOKENS = 700


class ExtractedSQL(BaseModel):
    sql: str
    confidence: float
    notes: str


class SQLExtractionResult(BaseModel):
    queries: list[ExtractedSQL]


def build_sql_extractor_prompt(code_snippet: str, *, file_context: str) -> str:
    return (
        "You extract SQL statements from Python code snippets.\n"
        "Return only SQL statements that are reasonably identifiable.\n"
        "Set confidence between 0 and 1 and include short notes "
        '(example: "f-string template", "dynamic WHERE clause").\n\n'
        "<file_context>\n"
        f"{file_context}\n"
        "</file_context>\n\n"
        "<code>\n"
        f"{code_snippet}\n"
        "</code>"
    )


def extract_sql_with_llm(
    code_snippet: str,
    *,
    file_context: str = "unknown",
    client: LLMClient | None = None,
) -> SQLExtractionResult:
    llm_client = client or LLMClient()
    prompt = build_sql_extractor_prompt(code_snippet, file_context=file_context)
    return generate_structured(
        client=llm_client,
        prompt=prompt,
        response_model=SQLExtractionResult,
        category="extraction",
        estimated_tokens=OUTPUT_TOKENS,
    )


def map_to_extracted_queries(
    result: SQLExtractionResult,
    *,
    source_file: str,
    line_number: int,
) -> list[ExtractedQuery]:
    return [
        ExtractedQuery(
            sql=item.sql,
            source_file=source_file,
            line_number=line_number,
            extraction_method="llm",
            confidence=item.confidence,
            notes=item.notes,
        )
        for item in result.queries
    ]
