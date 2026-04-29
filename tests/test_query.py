import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from gsheets_sql.exceptions import QuerySyntaxError, TableNotFound
from gsheets_sql.query.engine import execute_dml, execute_query


def _make_db(tables: dict[str, list[dict]]) -> MagicMock:
    db = MagicMock()
    _cache: dict[str, MagicMock] = {}

    def getitem(name):
        if name in _cache:
            return _cache[name]
        if name not in tables:
            raise TableNotFound(name)
        table = MagicMock()
        rows = tables[name]
        df = pd.DataFrame(rows)
        table.all.return_value = df
        table.name = name
        table._header_row = 1
        headers = list(rows[0].keys()) if rows else []
        table._fetch_raw.return_value = (headers, [])
        table._raw_to_dicts.return_value = rows
        table._invalidate = MagicMock()
        _cache[name] = table
        return table

    db.__getitem__.side_effect = getitem
    return db


# ── SELECT ────────────────────────────────────────────────────────────────────

def test_select_all():
    db = _make_db({"clientes": [{"nome": "Ana", "idade": 28}, {"nome": "Bob", "idade": 35}]})
    results = execute_query("SELECT * FROM clientes", db)
    assert len(results) == 2


def test_select_where():
    db = _make_db({"clientes": [{"nome": "Ana", "idade": 28}, {"nome": "Bob", "idade": 35}]})
    results = execute_query("SELECT nome FROM clientes WHERE idade > 30", db)
    assert len(results) == 1
    assert results[0]["nome"] == "Bob"


def test_select_limit():
    db = _make_db({"clientes": [{"nome": f"User{i}", "idade": i} for i in range(10)]})
    results = execute_query("SELECT * FROM clientes LIMIT 3", db)
    assert len(results) == 3


def test_select_order_by():
    db = _make_db({"clientes": [{"nome": "Zé", "idade": 40}, {"nome": "Ana", "idade": 28}]})
    results = execute_query("SELECT nome FROM clientes ORDER BY nome", db)
    assert results[0]["nome"] == "Ana"
    assert results[1]["nome"] == "Zé"


def test_select_aggregate():
    db = _make_db({"vendas": [{"produto": "A", "valor": 10}, {"produto": "A", "valor": 20}]})
    results = execute_query("SELECT produto, SUM(valor) AS total FROM vendas GROUP BY produto", db)
    assert results[0]["total"] == 30


def test_select_returns_dataframe():
    db = _make_db({"clientes": [{"nome": "Ana", "idade": 28}]})
    result = execute_query("SELECT * FROM clientes", db, as_dataframe=True)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1


def test_select_table_not_found():
    db = _make_db({})
    with pytest.raises(TableNotFound):
        execute_query("SELECT * FROM inexistente", db)


def test_dml_in_query_raises():
    db = _make_db({})
    with pytest.raises(QuerySyntaxError):
        execute_query("INSERT INTO t VALUES (1)", db)


def test_invalid_sql_raises():
    db = _make_db({})
    with pytest.raises(QuerySyntaxError):
        execute_query("NOT VALID SQL !!!", db)


# ── INSERT ────────────────────────────────────────────────────────────────────

def test_execute_insert():
    rows: list[dict] = []

    db = MagicMock()
    table = MagicMock()
    table.name = "clientes"
    table._header_row = 1
    table._fetch_raw.return_value = (["nome", "idade"], [])

    def insert_side_effect(data):
        rows.append(data)

    table.insert.side_effect = insert_side_effect
    db.__getitem__.return_value = table

    count = execute_dml("INSERT INTO clientes (nome, idade) VALUES ('Ana', 28)", db)
    assert count == 1
    assert rows[0] == {"nome": "Ana", "idade": 28}


# ── UPDATE ────────────────────────────────────────────────────────────────────

def test_execute_update():
    db = _make_db({"clientes": [{"nome": "Ana", "idade": 28}, {"nome": "Bob", "idade": 35}]})
    table = db["clientes"]
    table._ws = MagicMock()

    count = execute_dml("UPDATE clientes SET idade = 29 WHERE nome = 'Ana'", db)
    assert count == 1
    table._ws.update_cell.assert_called()


# ── DELETE ────────────────────────────────────────────────────────────────────

def test_execute_delete():
    db = _make_db({"clientes": [{"nome": "Ana", "idade": 28}, {"nome": "Bob", "idade": 35}]})
    table = db["clientes"]
    table._ws = MagicMock()

    count = execute_dml("DELETE FROM clientes WHERE nome = 'Ana'", db)
    assert count == 1
    table._ws.delete_rows.assert_called_once()
