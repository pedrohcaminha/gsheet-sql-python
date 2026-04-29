# gsheets-sql — Especificação Técnica

## Visão Geral

Biblioteca Python que expõe Google Planilhas como um banco de dados relacional, permitindo consultas SQL-like, operações CRUD e uma interface tabular similar ao pgAdmin.

---

## Objetivos

- Tratar cada **aba** (sheet) como uma **tabela**
- Tratar cada **planilha** (spreadsheet) como um **banco de dados**
- Permitir consultas com sintaxe próxima ao SQL padrão
- Oferecer uma interface de linha de comando (REPL) estilo psql/pgAdmin
- Integrar com o ecossistema Python (pandas, SQLAlchemy dialect opcional)

---

## Funcionalidades Core

### 1. Conexão

```python
from gsheets_sql import connect

db = connect(
    spreadsheet_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms",
    credentials="credentials.json",   # service account ou OAuth
)
```

- Suporte a **Service Account** (JSON) e **OAuth 2.0** (browser flow)
- Cache de credenciais local (`.gsheets_sql_token`)
- Reconexão automática em caso de expiração do token

---

### 2. Consultas SQL-like

```python
# SELECT com WHERE, ORDER BY, LIMIT
results = db.query("SELECT nome, idade FROM clientes WHERE idade > 30 ORDER BY nome LIMIT 10")

# INSERT
db.execute("INSERT INTO clientes (nome, idade) VALUES ('Ana', 28)")

# UPDATE
db.execute("UPDATE clientes SET idade = 29 WHERE nome = 'Ana'")

# DELETE
db.execute("DELETE FROM clientes WHERE nome = 'Ana'")
```

Operadores suportados na cláusula WHERE:
- Comparação: `=`, `!=`, `<`, `>`, `<=`, `>=`
- Lógicos: `AND`, `OR`, `NOT`
- String: `LIKE`, `ILIKE`
- Conjunto: `IN`, `NOT IN`
- Nulo: `IS NULL`, `IS NOT NULL`

Funções de agregação:
- `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`

Cláusulas suportadas:
- `SELECT`, `FROM`, `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, `GROUP BY`

---

### 3. API Python Nativa (sem SQL)

```python
table = db["clientes"]

# Leitura
df = table.all()                              # retorna DataFrame pandas
row = table.get(id=1)                         # linha por chave primária
rows = table.filter(idade__gt=30, ativo=True) # ORM-style filters

# Escrita
table.insert({"nome": "Ana", "idade": 28})
table.insert_many([{"nome": "Bob"}, {"nome": "Clara"}])
table.update({"idade": 29}, where={"nome": "Ana"})
table.delete(where={"nome": "Ana"})

# Schema
table.schema()   # retorna colunas e tipos inferidos
table.count()
```

---

### 4. Gerenciamento de Schema

```python
# Criar aba (tabela)
db.create_table("pedidos", columns=["id", "produto", "quantidade", "preco"])

# Remover aba
db.drop_table("pedidos")

# Listar tabelas (abas)
db.tables()

# Renomear coluna (atualiza cabeçalho)
table.rename_column("preco", "valor")
```

---

### 5. Tipos de Dados e Inferência

A lib infere tipos automaticamente a partir dos valores na planilha:

| Valor na célula | Tipo Python inferido |
|---|---|
| `123`, `45.6` | `int` / `float` |
| `TRUE`, `FALSE` | `bool` |
| `2024-01-15` | `datetime.date` |
| `2024-01-15 10:30` | `datetime.datetime` |
| Qualquer outro | `str` |

Schema pode ser fixado manualmente:

```python
table.set_schema({
    "id": "int",
    "nome": "str",
    "criado_em": "datetime",
})
```

---

### 6. Interface REPL (CLI)

```bash
$ gsheets-sql --id 1BxiMVs0XRA5... --credentials credentials.json

gsheets-sql (spreadsheet: MinhaBase)
Type \? for help, \q to quit.

MinhaBase> \dt
 Tabelas
---------
 clientes
 pedidos
 produtos

MinhaBase> SELECT * FROM clientes LIMIT 5;
 id | nome  | idade | ativo
----+-------+-------+-------
  1 | Ana   |    28 | true
  2 | Bob   |    35 | false
(2 rows)

MinhaBase> \d clientes
 Coluna | Tipo    | Nullable
--------+---------+---------
 id     | int     | false
 nome   | str     | true
 idade  | int     | true
 ativo  | bool    | true

MinhaBase> \q
```

Comandos meta (`\`):
- `\dt` — lista tabelas
- `\d <tabela>` — descreve schema da tabela
- `\c <spreadsheet_id>` — troca de spreadsheet
- `\q` — sair
- `\?` — ajuda

---

### 7. Integração com Pandas

```python
# Query retorna DataFrame diretamente
df = db.query("SELECT * FROM clientes", as_dataframe=True)

# Escrever DataFrame na planilha
db.from_dataframe("clientes", df, if_exists="replace")  # replace | append | fail
```

---

### 8. Integração SQLAlchemy (fase 2)

Dialect customizado para uso com SQLAlchemy e ferramentas que o suportam (Alembic, dbt, etc.):

```python
from sqlalchemy import create_engine

engine = create_engine(
    "gsheets+api:///?spreadsheet_id=1BxiMVs0...&credentials=credentials.json"
)
```

---

## Arquitetura Interna

```
gsheets_sql/
├── __init__.py           # ponto de entrada: connect()
├── connection.py         # autenticação e client Google Sheets API
├── database.py           # classe Database (spreadsheet)
├── table.py              # classe Table (aba/sheet)
├── query/
│   ├── parser.py         # parse SQL → AST interno
│   ├── planner.py        # AST → plano de execução
│   └── executor.py       # executa plano sobre dados da sheet
├── schema.py             # inferência e gestão de tipos
├── cache.py              # cache em memória com TTL configurável
├── repl.py               # CLI interativo
└── exceptions.py         # hierarquia de erros customizados
```

---

## Cache e Performance

- Cache em memória com TTL configurável (padrão: 60s)
- Leitura lazy: só busca dados quando necessário
- Escrita em batch: agrupa múltiplos `INSERT`/`UPDATE` antes de fazer a chamada à API
- Respeita os limites da Google Sheets API (quota: 300 req/min por projeto)

```python
db = connect(..., cache_ttl=120)  # segundos; 0 desativa
```

---

## Tratamento de Erros

```python
from gsheets_sql.exceptions import (
    AuthError,          # falha na autenticação
    TableNotFound,      # aba não existe
    ColumnNotFound,     # coluna referenciada não existe
    QuerySyntaxError,   # SQL malformado
    QuotaExceeded,      # limite de API atingido
    SchemaError,        # conflito de tipos
)
```

---

## Configuração

```python
db = connect(
    spreadsheet_id="...",
    credentials="credentials.json",
    cache_ttl=60,          # TTL do cache em segundos
    batch_size=100,        # linhas por chamada de leitura
    header_row=1,          # linha que contém os cabeçalhos (1-indexed)
    locale="pt_BR",        # locale para formatação de datas/números
)
```

---

## Requisitos e Dependências

| Pacote | Uso |
|---|---|
| `google-auth` | Autenticação OAuth / Service Account |
| `google-auth-oauthlib` | Fluxo OAuth para uso interativo |
| `google-api-python-client` | Cliente da Sheets API v4 |
| `pandas` | Retorno de resultados como DataFrame |
| `sqlparse` | Parse e validação de SQL |
| `rich` | Renderização tabular no REPL |
| `click` | CLI (`gsheets-sql` command) |

Dependências opcionais:
| Pacote | Uso |
|---|---|
| `sqlalchemy` | Dialect customizado (fase 2) |

---

## Roadmap de Desenvolvimento

### Fase 1 — Core
- [ ] Autenticação (Service Account + OAuth)
- [ ] Leitura/escrita básica (CRUD sem SQL)
- [ ] Inferência de tipos
- [ ] Cache em memória

### Fase 2 — Query Engine
- [ ] Parser SQL (SELECT com WHERE, ORDER BY, LIMIT)
- [ ] INSERT / UPDATE / DELETE via SQL
- [ ] Funções de agregação
- [ ] JOIN entre abas da mesma planilha

### Fase 3 — DX e Integrações
- [ ] REPL interativo (CLI)
- [ ] Integração Pandas (from_dataframe)
- [ ] SQLAlchemy dialect
- [ ] Documentação e exemplos

---

## Não está no escopo

- Transações ACID (Google Sheets não oferece atomicidade)
- Subqueries aninhadas complexas
- Índices ou otimização de busca (todas as leituras varrem a sheet)
- Suporte a fórmulas do Sheets como valores persistidos
- Multi-usuário / controle de concorrência
