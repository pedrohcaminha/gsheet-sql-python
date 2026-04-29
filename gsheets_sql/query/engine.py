from typing import Any

import duckdb
import pandas as pd
import sqlglot
import sqlglot.expressions as exp

from ..exceptions import ColumnNotFound, QuerySyntaxError, TableNotFound
from ..schema import _to_cell_value


def _parse(sql: str):
    try:
        return sqlglot.parse_one(sql)
    except sqlglot.errors.ParseError as e:
        raise QuerySyntaxError(str(e))


def execute_query(sql: str, db, as_dataframe: bool = False):
    ast = _parse(sql)
    if not isinstance(ast, exp.Select):
        raise QuerySyntaxError(
            "Only SELECT is supported in query(); use execute() for INSERT/UPDATE/DELETE"
        )

    table_names = [t.name for t in ast.find_all(exp.Table)]
    conn = duckdb.connect()
    for name in table_names:
        df = db[name].all()  # raises TableNotFound if absent
        conn.register(name, df)

    try:
        result_df = conn.execute(sql).df()
    except duckdb.Error as e:
        raise QuerySyntaxError(str(e))

    return result_df if as_dataframe else result_df.to_dict("records")


def execute_dml(sql: str, db) -> int:
    ast = _parse(sql)
    if isinstance(ast, exp.Insert):
        return _execute_insert(ast, db)
    if isinstance(ast, exp.Update):
        return _execute_update(ast, db)
    if isinstance(ast, exp.Delete):
        return _execute_delete(ast, db)
    raise QuerySyntaxError(
        f"Unsupported statement: {type(ast).__name__}. "
        "execute() supports INSERT, UPDATE, DELETE."
    )


# ── INSERT ────────────────────────────────────────────────────────────────────

def _execute_insert(ast: exp.Insert, db) -> int:
    table_node = ast.find(exp.Table)
    table_name = table_node.name
    table = db[table_name]

    headers, _ = table._fetch_raw()

    if isinstance(ast.this, exp.Schema):
        columns = [c.name for c in ast.this.expressions]
    else:
        columns = headers

    values_node = ast.find(exp.Values)
    if not values_node:
        raise QuerySyntaxError("INSERT requires a VALUES clause")

    rows_inserted = 0
    for tup in values_node.find_all(exp.Tuple):
        vals = [_literal_value(v) for v in tup.expressions]
        table.insert(dict(zip(columns, vals)))
        rows_inserted += 1

    return rows_inserted


# ── UPDATE ────────────────────────────────────────────────────────────────────

def _execute_update(ast: exp.Update, db) -> int:
    table_name = ast.find(exp.Table).name
    table = db[table_name]

    set_data: dict[str, Any] = {}
    for eq in ast.expressions:
        col = eq.left.name if hasattr(eq.left, "name") else str(eq.left)
        set_data[col] = _literal_value(eq.right)

    headers, _ = table._fetch_raw()
    dicts = table._raw_to_dicts()
    if not dicts:
        return 0

    where_node = ast.find(exp.Where)
    where_condition = where_node.this.sql(dialect="duckdb") if where_node else "TRUE"
    indices = _matching_indices(dicts, where_condition)

    for idx in indices:
        sheet_row = table._header_row + 1 + idx
        for col, val in set_data.items():
            if col not in headers:
                raise ColumnNotFound(col)
            col_idx = headers.index(col) + 1
            table._ws.update_cell(sheet_row, col_idx, _to_cell_value(val))

    if indices:
        table._invalidate()
    return len(indices)


# ── DELETE ────────────────────────────────────────────────────────────────────

def _execute_delete(ast: exp.Delete, db) -> int:
    table_name = ast.find(exp.Table).name
    table = db[table_name]

    dicts = table._raw_to_dicts()
    if not dicts:
        return 0

    where_node = ast.find(exp.Where)
    where_condition = where_node.this.sql(dialect="duckdb") if where_node else "TRUE"
    indices = _matching_indices(dicts, where_condition)

    for idx in sorted(indices, reverse=True):
        sheet_row = table._header_row + 1 + idx
        table._ws.delete_rows(sheet_row)

    if indices:
        table._invalidate()
    return len(indices)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _matching_indices(dicts: list[dict], where_condition: str) -> list[int]:
    df = pd.DataFrame(dicts)
    df["__idx__"] = range(len(df))
    conn = duckdb.connect()
    conn.register("_t", df)
    try:
        result = conn.execute(
            f"SELECT __idx__ FROM _t WHERE {where_condition}"
        ).fetchall()
    except duckdb.Error as e:
        raise QuerySyntaxError(str(e))
    return [r[0] for r in result]


def _literal_value(node) -> Any:
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        try:
            return int(node.this)
        except ValueError:
            return float(node.this)
    if isinstance(node, exp.Boolean):
        return node.this if isinstance(node.this, bool) else str(node.this).lower() == "true"
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal):
        try:
            return -int(node.this.this)
        except ValueError:
            return -float(node.this.this)
    return str(node)
