from pgreviewer.core.models import ColumnInfo, SchemaInfo, TableInfo
from pgreviewer.parsing.param_substitutor import make_notes, substitute_params

# ---------------------------------------------------------------------------
# No-op: SQL without parameters
# ---------------------------------------------------------------------------


def test_no_params_returns_original():
    sql = "SELECT * FROM users WHERE id = 1"
    result, subs = substitute_params(sql)
    assert result == sql
    assert subs == []


# ---------------------------------------------------------------------------
# PostgreSQL positional: $1, $2, …
# ---------------------------------------------------------------------------


def test_pg_positional_single():
    sql = "SELECT * FROM orders WHERE user_id = $1"
    result, subs = substitute_params(sql)
    assert "$1" not in result
    assert "user_id" in result or "42" in result
    # user_id → 42
    assert result == "SELECT * FROM orders WHERE user_id = 42"
    assert len(subs) == 1
    assert "$1=42" in subs[0]
    assert "user_id" in subs[0]


def test_pg_positional_multiple_heuristics():
    """$1/$2 should pick up column context for each placeholder."""
    sql = "SELECT * FROM orders WHERE user_id = $1 AND status = $2"
    result, subs = substitute_params(sql)
    # user_id → 42, status → 'placeholder'
    expected = "SELECT * FROM orders WHERE user_id = 42 AND status = 'placeholder'"
    assert result == expected
    assert len(subs) == 2
    assert "$1=42" in subs[0]
    assert "$2='placeholder'" in subs[1]


def test_pg_positional_date_column():
    sql = "SELECT * FROM events WHERE created_at = $1"
    result, subs = substitute_params(sql)
    assert result == "SELECT * FROM events WHERE created_at = NOW()"
    assert "$1=NOW()" in subs[0]


def test_pg_positional_no_context_uses_position_default():
    """When there is no column context, fall back to position-cycling defaults."""
    sql = "SELECT $1, $2, $3"
    result, subs = substitute_params(sql)
    assert result == "SELECT 42, 'placeholder', NOW()"
    assert len(subs) == 3


# ---------------------------------------------------------------------------
# psycopg2 positional: %s
# ---------------------------------------------------------------------------


def test_psycopg_single():
    sql = "SELECT * FROM orders WHERE user_id = %s"
    result, subs = substitute_params(sql)
    assert result == "SELECT * FROM orders WHERE user_id = 42"
    assert len(subs) == 1
    assert "%s[1]=42" in subs[0]
    assert "user_id" in subs[0]


def test_psycopg_multiple():
    sql = "SELECT * FROM orders WHERE user_id = %s AND status = %s"
    result, subs = substitute_params(sql)
    expected = "SELECT * FROM orders WHERE user_id = 42 AND status = 'placeholder'"
    assert result == expected
    assert len(subs) == 2


def test_psycopg_no_context():
    sql = "SELECT %s, %s"
    result, subs = substitute_params(sql)
    assert result == "SELECT 42, 'placeholder'"
    assert len(subs) == 2


# ---------------------------------------------------------------------------
# SQLAlchemy named: :param_name
# ---------------------------------------------------------------------------


def test_named_user_id():
    sql = "SELECT * FROM orders WHERE user_id = :user_id"
    result, subs = substitute_params(sql)
    assert result == "SELECT * FROM orders WHERE user_id = 42"
    assert ":user_id=42" in subs[0]


def test_named_name_param():
    sql = "SELECT * FROM users WHERE name = :name"
    result, subs = substitute_params(sql)
    assert result == "SELECT * FROM users WHERE name = 'placeholder'"
    assert ":name='placeholder'" in subs[0]


def test_named_created_at():
    sql = "SELECT * FROM logs WHERE created_at > :created_at"
    result, subs = substitute_params(sql)
    assert result == "SELECT * FROM logs WHERE created_at > NOW()"
    assert ":created_at=NOW()" in subs[0]


def test_named_multiple():
    sql = "SELECT * FROM orders WHERE user_id = :user_id AND status = :status"
    result, subs = substitute_params(sql)
    expected = "SELECT * FROM orders WHERE user_id = 42 AND status = 'placeholder'"
    assert result == expected
    assert len(subs) == 2


def test_named_cast_not_matched():
    """PostgreSQL ::cast syntax must NOT be treated as a named parameter."""
    sql = "SELECT $1::text"
    result, subs = substitute_params(sql)
    # $1 replaced, ::text left intact
    assert "$1" not in result
    assert "::text" in result
    assert ":text" not in subs


# ---------------------------------------------------------------------------
# Schema-based type inference
# ---------------------------------------------------------------------------


def _make_schema(table: str, columns: list[tuple[str, str]]) -> SchemaInfo:
    col_infos = [ColumnInfo(name=c, type=t) for c, t in columns]
    return SchemaInfo(tables={table: TableInfo(columns=col_infos)})


def test_schema_integer_column():
    schema = _make_schema("orders", [("user_id", "integer")])
    sql = "SELECT * FROM orders WHERE user_id = $1"
    result, subs = substitute_params(sql, schema)
    assert result == "SELECT * FROM orders WHERE user_id = 42"


def test_schema_text_column():
    schema = _make_schema("users", [("email", "text")])
    sql = "SELECT * FROM users WHERE email = $1"
    result, subs = substitute_params(sql, schema)
    assert result == "SELECT * FROM users WHERE email = 'placeholder'"


def test_schema_timestamp_column():
    schema = _make_schema("events", [("occurred_at", "timestamptz")])
    sql = "SELECT * FROM events WHERE occurred_at > $1"
    result, subs = substitute_params(sql, schema)
    assert result == "SELECT * FROM events WHERE occurred_at > NOW()"


def test_schema_boolean_column():
    schema = _make_schema("users", [("is_active", "boolean")])
    sql = "SELECT * FROM users WHERE is_active = $1"
    result, subs = substitute_params(sql, schema)
    assert result == "SELECT * FROM users WHERE is_active = TRUE"


def test_schema_numeric_column():
    schema = _make_schema("products", [("price", "numeric")])
    sql = "SELECT * FROM products WHERE price > $1"
    result, subs = substitute_params(sql, schema)
    assert result == "SELECT * FROM products WHERE price > 1.0"


# ---------------------------------------------------------------------------
# make_notes helper
# ---------------------------------------------------------------------------


def test_make_notes_with_substitutions():
    subs = ["$1=42 (column: user_id)", "$2='placeholder'"]
    notes = make_notes(subs)
    assert notes is not None
    assert notes.startswith("analyzed with dummy parameters:")
    assert "$1=42" in notes
    assert "$2='placeholder'" in notes


def test_make_notes_empty_returns_none():
    assert make_notes([]) is None


# ---------------------------------------------------------------------------
# Issue example (spec check)
# ---------------------------------------------------------------------------


def test_spec_example():
    """Verify the exact example from the issue description."""
    result, subs = substitute_params(
        "SELECT * FROM orders WHERE user_id = $1 AND status = $2"
    )
    expected = "SELECT * FROM orders WHERE user_id = 42 AND status = 'placeholder'"
    assert result == expected
    assert len(subs) == 2
