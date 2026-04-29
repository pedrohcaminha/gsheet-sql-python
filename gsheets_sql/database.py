import gspread.exceptions
import pandas as pd

from .cache import Cache
from .exceptions import TableNotFound
from .table import Table


class Database:
    def __init__(self, spreadsheet, cache: Cache, header_row: int = 1):
        self._ss = spreadsheet
        self._cache = cache
        self._header_row = header_row

    def __getitem__(self, name: str) -> Table:
        try:
            ws = self._ss.worksheet(name)
        except gspread.exceptions.WorksheetNotFound:
            raise TableNotFound(f"Table '{name}' not found")
        return Table(ws, self._cache, self._header_row)

    def tables(self) -> list[str]:
        return [ws.title for ws in self._ss.worksheets()]

    def create_table(self, name: str, columns: list[str]) -> Table:
        ws = self._ss.add_worksheet(title=name, rows=1000, cols=max(len(columns), 1))
        ws.append_row(columns)
        return Table(ws, self._cache, self._header_row)

    def drop_table(self, name: str) -> None:
        try:
            ws = self._ss.worksheet(name)
            self._ss.del_worksheet(ws)
        except gspread.exceptions.WorksheetNotFound:
            raise TableNotFound(f"Table '{name}' not found")

    def query(self, sql: str, as_dataframe: bool = False):
        from .query.engine import execute_query
        return execute_query(sql, self, as_dataframe=as_dataframe)

    def execute(self, sql: str) -> int:
        from .query.engine import execute_dml
        return execute_dml(sql, self)

    def from_dataframe(
        self, table_name: str, df: pd.DataFrame, if_exists: str = "fail"
    ) -> None:
        tables = self.tables()
        if table_name in tables:
            if if_exists == "fail":
                raise ValueError(
                    f"Table '{table_name}' already exists. "
                    "Use if_exists='replace' or 'append'."
                )
            elif if_exists == "replace":
                self.drop_table(table_name)
                table = self.create_table(table_name, list(df.columns))
            else:  # append
                table = self[table_name]
        else:
            table = self.create_table(table_name, list(df.columns))
        table.insert_many(df.to_dict("records"))

    def __repr__(self) -> str:
        return f"Database(name={self._ss.title!r})"
