from pgreviewer.parsing.sql_extractor_raw import extract_raw_sql


def test_extract_basic_execute():
    source = """
def my_func():
    cursor.execute("SELECT * FROM users")
    db.session.execute("UPDATE products SET price = 10")
    """

    queries = extract_raw_sql(source, file_path="test.py")
    assert len(queries) == 2

    assert queries[0].sql == "SELECT * FROM users"
    assert queries[0].confidence == 0.9
    assert queries[0].extraction_method == "treesitter"
    assert queries[0].line_number == 3

    assert queries[1].sql == "UPDATE products SET price = 10"
    assert queries[1].confidence == 0.9


def test_extract_with_text_wrapper():
    source = """
from sqlalchemy import text

def check():
    conn.fetch(text("SELECT id FROM orders LIMIT 1"))
    """
    queries = extract_raw_sql(source)
    assert len(queries) == 1
    assert queries[0].sql == "SELECT id FROM orders LIMIT 1"
    assert queries[0].confidence == 0.9


def test_extract_string_assignment():
    source = """
def test():
    sql = "SELECT * FROM my_table"
    # some comment
    cursor.execute(sql)
    """
    queries = extract_raw_sql(source)
    assert len(queries) == 1
    assert queries[0].sql == "SELECT * FROM my_table"
    assert queries[0].confidence == 0.9
    assert queries[0].line_number == 5


def test_extract_low_confidence_complex_args():
    source = """
def complex_stuff(table_name, user_id):
    cursor.execute(f"SELECT * FROM {table_name}")
    cursor.execute("SELECT * FROM " + table_name)

    query = f"DELETE FROM users WHERE id = {user_id}"
    cursor.execute(query) # Assigned variable but it's not a simple string

    # fetchone usually takes no args, but if it did... or fetched from somewhere
    cursor.fetchrow(get_query_from_db())
    """
    queries = extract_raw_sql(source)
    assert len(queries) == 4

    # f-string directly
    assert queries[0].confidence == 0.3
    assert queries[0].sql.startswith('f"SELECT')

    # concatenation
    assert queries[1].confidence == 0.3

    # query is not a simple string assignment found before
    assert queries[2].confidence == 0.3

    # function call
    assert queries[3].confidence == 0.3


def test_ignore_empty_or_no_sql():
    source = """
def hello_world():
    print("execute this!")
    """
    queries = extract_raw_sql(source)
    assert len(queries) == 0

    assert len(extract_raw_sql("")) == 0
