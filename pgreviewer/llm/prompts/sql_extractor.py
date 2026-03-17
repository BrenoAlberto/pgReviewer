from __future__ import annotations

import re
from collections import Counter

from pydantic import BaseModel

from pgreviewer.core.models import ExtractedQuery
from pgreviewer.llm.client import LLMClient
from pgreviewer.llm.structured_output import generate_structured
from pgreviewer.parsing.param_substitutor import make_notes, substitute_params

OUTPUT_TOKENS = 700
DYNAMIC_WHERE_NOTE = "dynamic WHERE clause"
_FSTRING_CONFIDENCE = 0.65
_FSTRING_VAR_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
_TABLE_PLACEHOLDER_RE = re.compile(r"(?:^|_)table(?:_name)?(?:$|_)")
_COLUMN_PLACEHOLDER_RE = re.compile(r"(?:^|_)column(?:_name)?(?:$|_)")
_MODEL_IMPORT_RE = re.compile(
    r"^\s*from\s+[^\n]*models[^\n]*\s+import\s+([A-Za-z0-9_, ]+)$",
    re.MULTILINE,
)
_SQL_TABLE_RE = re.compile(
    r"\b(?:FROM|JOIN|UPDATE|INTO)\s+([A-Za-z_][A-Za-z0-9_]*)\b(?!\s+import\b)",
    re.IGNORECASE,
)
_SQL_COLUMN_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*=", re.IGNORECASE)


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
        "When SQL is built with string concatenation/query-builder patterns, "
        "reconstruct the most likely final SQL by combining the known fragments, "
        "and document dynamic/conditional parts in notes.\n\n"
        "For f-string SQL, extract the SQL template first, then substitute "
        "placeholders using variable names and nearby usage context. "
        "If the table placeholder is dynamic (e.g. {table_name}), substitute "
        "using the most commonly referenced table in context, or 'example_table' "
        "if unknown. For id-like value placeholders, prefer 42. "
        "Use lower confidence (0.60-0.75) and explain substitutions in notes.\n\n"
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
    source_context: str = "",
) -> list[ExtractedQuery]:
    extracted: list[ExtractedQuery] = []
    for item in result.queries:
        notes = item.notes
        sql = item.sql
        if DYNAMIC_WHERE_NOTE.lower() in (notes or "").lower():
            sql, substitutions = substitute_params(sql)
            note_parts = [part for part in [notes, "parameterized query"] if part]
            substitution_notes = make_notes(substitutions)
            if substitution_notes:
                note_parts.append(substitution_notes)
            notes = "; ".join(note_parts)
        sql, notes, confidence = _apply_fstring_substitutions(
            sql=sql,
            notes=notes,
            confidence=item.confidence,
            source_context=source_context,
        )
        extracted.append(
            ExtractedQuery(
                sql=sql,
                source_file=source_file,
                line_number=line_number,
                extraction_method="llm",
                confidence=confidence,
                notes=notes,
            )
        )
    return extracted


def _apply_fstring_substitutions(
    *,
    sql: str,
    notes: str | None,
    confidence: float,
    source_context: str,
) -> tuple[str, str | None, float]:
    if "{" not in sql or "}" not in sql:
        return sql, notes, confidence

    table_name = _most_common_table(source_context)
    likely_column = _most_common_column(source_context)
    substitutions_made = False
    table_placeholder: str | None = None

    def _replace(match: re.Match[str]) -> str:
        nonlocal substitutions_made, table_placeholder
        name = match.group(1)
        lowered = name.lower()
        if _TABLE_PLACEHOLDER_RE.search(lowered):
            substitutions_made = True
            table_placeholder = name
            return table_name
        if _COLUMN_PLACEHOLDER_RE.search(lowered):
            substitutions_made = True
            return likely_column
        substitutions_made = True
        return "42" if "id" in lowered else "'placeholder'"

    substituted_sql = _FSTRING_VAR_RE.sub(_replace, sql)
    if not substitutions_made:
        return sql, notes, confidence

    note_parts = [part for part in [notes] if part]
    if table_placeholder:
        note_parts.append(
            f"f-string: table='{{{table_placeholder}}}' substituted from context"
        )
    final_notes = "; ".join(note_parts) if note_parts else None
    return substituted_sql, final_notes, _FSTRING_CONFIDENCE


def _most_common_table(source_context: str) -> str:
    table_counts: Counter[str] = Counter()
    for match in _SQL_TABLE_RE.finditer(source_context):
        table_counts[match.group(1).lower()] += 1
    for match in _MODEL_IMPORT_RE.finditer(source_context):
        for raw_name in match.group(1).split(","):
            model_name = raw_name.strip()
            if not model_name:
                continue
            table_counts[_model_to_table(model_name)] += 1
    if not table_counts:
        return "example_table"
    return table_counts.most_common(1)[0][0]


def _most_common_column(source_context: str) -> str:
    column_counts: Counter[str] = Counter()
    for match in _SQL_COLUMN_RE.finditer(source_context):
        column_counts[match.group(1).lower()] += 1
    return column_counts.most_common(1)[0][0] if column_counts else "id"


def _model_to_table(model_name: str) -> str:
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", model_name).lower()
    if snake.endswith("s"):
        return snake
    return f"{snake}s"
