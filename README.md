# gsheet-sql-python

Use Google Sheets like a PostgreSQL database — query with SQL, write Python ORM-style filters, and browse data in an interactive REPL.

## Install

```bash
pip install gsheet-sql-python
```

## Quick start

```python
from gsheets_sql import connect

db = connect(
    spreadsheet_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms",
    credentials="credentials.json",  # service account or OAuth client secrets
)

# SQL query
results = db.query("SELECT nome, idade FROM clientes WHERE idade > 30 ORDER BY nome")

# Python API
table = db["clientes"]
table.filter(idade__gt=30)
table.insert({"nome": "Ana", "idade": 28})
table.update({"idade": 29}, where={"nome": "Ana"})
table.delete(where={"nome": "Ana"})

# Pandas
df = db.query("SELECT * FROM clientes", as_dataframe=True)
db.from_dataframe("clientes", df, if_exists="replace")
```

## REPL

```bash
gsheets-sql --id <spreadsheet_id> --credentials credentials.json
```

```
MinhaBase> \dt          # list tables
MinhaBase> \d clientes  # describe schema
MinhaBase> SELECT * FROM clientes LIMIT 10;
```

## Authentication

- **Service Account**: pass the JSON file path to `credentials`
- **OAuth 2.0**: pass the client secrets JSON — a browser window will open on first use; token is cached locally

## Features

- SQL `SELECT` with `WHERE`, `ORDER BY`, `GROUP BY`, `LIMIT`, `OFFSET`, aggregations (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)
- `INSERT`, `UPDATE`, `DELETE` via SQL or Python API
- ORM-style filters: `col__gt`, `col__lt`, `col__like`, `col__in`, `col__isnull`, …
- Automatic type inference (int, float, bool, date, datetime, str)
- In-memory cache with configurable TTL
- Interactive REPL with rich table output

## License

MIT
