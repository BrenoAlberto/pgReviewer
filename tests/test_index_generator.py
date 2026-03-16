from pgreviewer.analysis.index_generator import generate_create_index
from pgreviewer.core.models import IndexRecommendation


def test_generate_create_index_simple():
    rec = IndexRecommendation(
        table="users",
        columns=["email"],
        index_type="btree",
        is_unique=True,
        partial_predicate=None,
        create_statement="",  # To be filled
        cost_before=100.0,
        cost_after=10.0,
        improvement_pct=0.9,
        estimated_size_bytes=1024,
        validated=True,
        rationale="test",
    )
    sql = generate_create_index(rec)

    assert "CREATE UNIQUE INDEX CONCURRENTLY idx_users_email" in sql
    assert "ON users (email)" in sql
    assert "-- Estimated cost reduction: 100.00 → 10.00 (90.0%)" in sql
    assert sql.endswith(";")


def test_generate_create_index_composite():
    rec = IndexRecommendation(
        table="orders",
        columns=["user_id", "created_at"],
        index_type="btree",
        is_unique=False,
        partial_predicate=None,
        create_statement="",
        cost_before=500.0,
        cost_after=50.0,
        improvement_pct=0.9,
        estimated_size_bytes=None,
        validated=True,
        rationale="test",
    )
    sql = generate_create_index(rec)

    assert "CREATE INDEX CONCURRENTLY idx_orders_user_id_created_at" in sql
    assert "ON orders (user_id, created_at)" in sql


def test_generate_create_index_partial():
    rec = IndexRecommendation(
        table="logs",
        columns=["level"],
        index_type="btree",
        is_unique=False,
        partial_predicate="level = 'error'",
        create_statement="",
        cost_before=1000.0,
        cost_after=5.0,
        improvement_pct=0.995,
        estimated_size_bytes=None,
        validated=True,
        rationale="test",
    )
    sql = generate_create_index(rec)

    assert "WHERE level = 'error'" in sql
    assert "idx_logs_level" in sql


def test_generate_create_index_long_name():
    rec = IndexRecommendation(
        table="a_very_long_table_name_that_might_cause_issues_with_postgres_limits",
        columns=["a_very_long_column_name_as_well_to_make_it_even_longer_and_longer"],
        index_type="btree",
        is_unique=False,
        partial_predicate=None,
        create_statement="",
        cost_before=10.0,
        cost_after=1.0,
        improvement_pct=0.9,
        estimated_size_bytes=None,
        validated=True,
        rationale="test",
    )
    sql = generate_create_index(rec)

    # Extract index name
    import re

    match = re.search(r"INDEX CONCURRENTLY (\w+)", sql)
    assert match is not None
    index_name = match.group(1)
    assert len(index_name) <= 63


def test_generate_create_index_gist():
    rec = IndexRecommendation(
        table="locations",
        columns=["geom"],
        index_type="gist",
        is_unique=False,
        partial_predicate=None,
        create_statement="",
        cost_before=200.0,
        cost_after=20.0,
        improvement_pct=0.9,
        estimated_size_bytes=None,
        validated=True,
        rationale="test",
    )
    sql = generate_create_index(rec)

    assert "USING gist (geom)" in sql
