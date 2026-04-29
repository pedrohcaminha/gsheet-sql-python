from .cache import Cache
from .connection import build_client
from .database import Database
from .exceptions import (
    AuthError,
    ColumnNotFound,
    GSheetsSQLError,
    QuerySyntaxError,
    QuotaExceeded,
    SchemaError,
    TableNotFound,
)

__version__ = "0.1.1"

__all__ = [
    "connect",
    "Database",
    "GSheetsSQLError",
    "AuthError",
    "TableNotFound",
    "ColumnNotFound",
    "QuerySyntaxError",
    "QuotaExceeded",
    "SchemaError",
]


def connect(
    spreadsheet_id: str,
    credentials: str = "credentials.json",
    cache_ttl: int = 60,
    header_row: int = 1,
) -> Database:
    client = build_client(credentials)
    spreadsheet = client.open_by_key(spreadsheet_id)
    cache = Cache(ttl=cache_ttl)
    return Database(spreadsheet, cache, header_row=header_row)
