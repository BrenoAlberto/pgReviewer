from pgreviewer.parsing.sql_extractor_migration import (
    extract_from_alembic_file,
    extract_from_sql_file,
)


def test_extract_from_sql_file(tmp_path):
    sql_content = """
    -- This is a comment
    CREATE TABLE users (id int);

    INSERT INTO users VALUES (1, 'with; semicolon');

    START TRANSACTION;
    UPDATE users SET id = 2;
    COMMIT;
    """
    sql_file = tmp_path / "test.sql"
    sql_file.write_text(sql_content)

    queries = extract_from_sql_file(sql_file)

    # 1. CREATE TABLE
    # 2. INSERT (semicolon inside string respected)
    # 3. UPDATE (BEGIN/COMMIT filtered)
    assert len(queries) == 3
    assert "CREATE TABLE users" in queries[0].sql
    assert "INSERT INTO users" in queries[1].sql
    assert "UPDATE users" in queries[2].sql
    assert "with; semicolon" in queries[1].sql

    # Check if line numbers are somewhat correct
    # CREATE TABLE is on line 3
    # INSERT is on line 5
    # UPDATE is on line 8
    assert queries[0].line_number == 3
    assert queries[1].line_number == 5
    assert queries[2].line_number == 8


def test_extract_from_alembic_file(tmp_path):
    alembic_content = """
from alembic import op
from sqlalchemy import text

def upgrade():
    op.execute("CREATE TABLE products (id int)")
    op.execute(text("INSERT INTO products VALUES (1)"))

    op.execute(\"\"\"
        UPDATE products SET id = 10;
        ANALYZE products;
    \"\"\")

    # Should ignore other calls
    print("hello")
"""
    alembic_file = tmp_path / "migration.py"
    alembic_file.write_text(alembic_content)

    queries = extract_from_alembic_file(alembic_file)

    # 1. CREATE TABLE
    # 2. INSERT
    # 3. UPDATE
    # 4. ANALYZE (from the same op.execute)
    assert len(queries) == 4
    assert "CREATE TABLE products" in queries[0].sql
    assert "INSERT INTO products" in queries[1].sql
    assert "UPDATE products" in queries[2].sql
    assert "ANALYZE products" in queries[3].sql

    # Check line numbers
    # CREATE TABLE starts on line 6
    # INSERT starts on line 7
    # Multi-stmt starts on line 9, but the content starts on line 10
    assert queries[0].line_number == 6
    assert queries[1].line_number == 7
    assert queries[2].line_number == 10
    assert queries[3].line_number == 11
