# gsheet-sql-python

**Query Google Sheets with SQL.** Treat each spreadsheet as a database, each tab as a table — run `SELECT`, `INSERT`, `UPDATE`, `DELETE`, filter with a Python API, and explore data in an interactive REPL.

```bash
pip install gsheet-sql-python
```

---

## Getting started

```python
from gsheets_sql import connect

db = connect(
    spreadsheet_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms",
    credentials="credentials.json",
)
```

`credentials.json` can be a **service account** key or **OAuth 2.0** client secrets — a browser window opens on first OAuth use and the token is cached locally.

---

## SQL queries

```python
# Returns a list of dicts by default
rows = db.query("SELECT nome, idade FROM clientes WHERE idade > 30 ORDER BY nome LIMIT 10")

# Or a pandas DataFrame
df = db.query("SELECT * FROM clientes", as_dataframe=True)
```

Supported: `WHERE`, `ORDER BY`, `GROUP BY`, `LIMIT`, `OFFSET`, `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `JOIN` between tabs.

## DML

```python
db.execute("INSERT INTO clientes (nome, idade) VALUES ('Ana', 28)")
db.execute("UPDATE clientes SET idade = 29 WHERE nome = 'Ana'")
db.execute("DELETE FROM clientes WHERE nome = 'Ana'")
```

## Python API

```python
table = db["clientes"]

table.all()                                  # → DataFrame
table.filter(idade__gt=30, ativo=True)       # ORM-style filters
table.get(nome="Ana")                        # first match

table.insert({"nome": "Ana", "idade": 28})
table.insert_many([{"nome": "Bob"}, {"nome": "Clara"}])
table.update({"idade": 29}, where={"nome": "Ana"})
table.delete(where={"nome": "Ana"})

table.schema()     # inferred column types
table.count()
```

Supported filter suffixes: `__gt`, `__gte`, `__lt`, `__lte`, `__ne`, `__like`, `__ilike`, `__in`, `__isnull`.

## Pandas integration

```python
db.from_dataframe("clientes", df, if_exists="replace")  # replace | append | fail
```

## Schema management

```python
db.tables()                                         # list tabs
db.create_table("pedidos", ["id", "produto", "valor"])
db.drop_table("pedidos")
table.rename_column("preco", "valor")
```

---

## Interactive REPL

```bash
gsheets-sql --id <spreadsheet_id> --credentials credentials.json
```

```
MinhaBase> \dt
 Tables
--------
 clientes
 pedidos

MinhaBase> \d clientes
 Column | Type
--------+------
 id     | int
 nome   | str
 idade  | int
 ativo  | bool

MinhaBase> SELECT * FROM clientes WHERE idade > 30;
 nome | idade | ativo
------+-------+-------
 Bob  |    35 | false
(1 row)

MinhaBase> \q
```

Meta-commands: `\dt` list tables · `\d <table>` describe schema · `\q` quit · `\?` help.

---

## Configuration

```python
db = connect(
    spreadsheet_id="...",
    credentials="credentials.json",
    cache_ttl=60,    # seconds; 0 disables cache
    header_row=1,    # row that contains column names
)
```

## License

MIT
