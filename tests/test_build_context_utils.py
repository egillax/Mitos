from mitos.build_context import (
    CohortBuildOptions,
    _materialize_codesets,
    _qualify_name,
    _table,
)


class FakeTable:
    def __init__(self, value: str):
        self.value = value

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"FakeTable({self.value})"


class FakeExpr:
    def __init__(self, sql: str = "SELECT 1"):
        self._sql = sql

    def compile(self) -> str:
        return self._sql


class DummyBackend:
    """
    Minimal stand-in for an ibis backend that records calls.
    table_behavior controls which table call succeeds:
      - "database": succeed when database is provided
      - "schema": succeed when schema is provided
      - any other value -> always raise
    """

    def __init__(self, table_behavior: str = "raise"):
        self.table_behavior = table_behavior
        self.calls: list[tuple] = []

    def table(self, name, *, database=None, schema=None):
        self.calls.append(("table", name, database, schema))
        if self.table_behavior == "database" and database:
            return FakeTable(f"db:{database}.{name}")
        if self.table_behavior == "schema" and schema:
            return FakeTable(f"schema:{schema}.{name}")
        raise Exception("table resolution failed")

    def sql(self, query: str):
        self.calls.append(("sql", query))
        return FakeTable(query)

    def raw_sql(self, sql: str):
        self.calls.append(("raw_sql", sql))

    def create_table(self, name, expr, temp=False, overwrite=False):  # pragma: no cover
        self.calls.append(("create_table", name, temp, overwrite, expr))
        return FakeTable(name)


def test_qualify_name_handles_catalog_and_schema():
    assert _qualify_name(None, "events") == '"events"'
    assert _qualify_name("public", "events") == '"public"."events"'
    assert _qualify_name("catalog.schema", "events") == '"catalog"."schema"."events"'


def test_table_prefers_database_then_schema_then_sql_fallback():
    conn = DummyBackend(table_behavior="database")
    tbl = _table(conn, "cat.schema", "concept")
    assert tbl.value == "db:cat.schema.concept"
    assert conn.calls == [("table", "concept", "cat.schema", None)]

    conn_schema = DummyBackend(table_behavior="schema")
    tbl_schema = _table(conn_schema, "public", "concept")
    assert tbl_schema.value == "schema:public.concept"
    assert conn_schema.calls[0] == ("table", "concept", "public", None)
    assert conn_schema.calls[1] == ("table", "concept", None, "public")

    conn_sql = DummyBackend(table_behavior="raise")
    tbl_sql = _table(conn_sql, "cat.db", "concept")
    assert '"cat"."db"."concept"' in tbl_sql.value
    assert conn_sql.calls[0] == ("table", "concept", "cat.db", None)
    assert conn_sql.calls[1] == ("table", "concept", None, "cat.db")
    assert conn_sql.calls[2][0] == "sql"


def test_materialize_codesets_qualifies_temp_emulation_schema():
    options = CohortBuildOptions(temp_emulation_schema="catalog.schema")
    expr = FakeExpr("SELECT 1")
    conn = DummyBackend(table_behavior="raise")

    resource = _materialize_codesets(conn, expr, options)

    create_calls = [call[1] for call in conn.calls if call[0] == "raw_sql" and "CREATE TABLE" in call[1]]
    assert create_calls, "expected a CREATE TABLE call"
    assert '"catalog"."schema"' in create_calls[0]

    sql_calls = [call[1] for call in conn.calls if call[0] == "sql"]
    assert any('"catalog"."schema"' in sql for sql in sql_calls)

    # Ensure drop is qualified and callable
    resource.cleanup()
    drop_calls = [call[1] for call in conn.calls if call[0] == "raw_sql" and "DROP TABLE" in call[1]]
    assert drop_calls
    assert '"catalog"."schema"' in drop_calls[-1]
