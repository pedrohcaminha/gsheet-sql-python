from typing import Any, Optional

import pandas as pd

from .cache import Cache
from .exceptions import ColumnNotFound, TableNotFound
from .schema import _to_cell_value, cast_value, infer_schema


class Table:
    def __init__(self, worksheet, cache: Cache, header_row: int = 1):
        self._ws = worksheet
        self._cache = cache
        self._header_row = header_row
        self._schema_override: dict[str, str] = {}

    @property
    def name(self) -> str:
        return self._ws.title

    def _cache_key(self) -> str:
        return f"table:{self.name}"

    def _fetch_raw(self) -> tuple[list[str], list[list[str]]]:
        cached = self._cache.get(self._cache_key())
        if cached is not None:
            return cached
        all_values = self._ws.get_all_values()
        if not all_values:
            result = ([], [])
        else:
            headers = all_values[self._header_row - 1]
            data = all_values[self._header_row :]
            result = (headers, data)
        self._cache.set(self._cache_key(), result)
        return result

    def _invalidate(self) -> None:
        self._cache.invalidate(self._cache_key())

    def schema(self) -> dict[str, str]:
        headers, rows = self._fetch_raw()
        if not headers:
            return {}
        raw_dicts = [
            dict(zip(headers, row + [""] * (len(headers) - len(row)))) for row in rows
        ]
        return {**infer_schema(raw_dicts), **self._schema_override}

    def set_schema(self, schema: dict[str, str]) -> None:
        self._schema_override = schema

    def _raw_to_dicts(self) -> list[dict[str, Any]]:
        headers, rows = self._fetch_raw()
        if not headers:
            return []
        schema = self.schema()
        result = []
        for row in rows:
            padded = row + [""] * (len(headers) - len(row))
            d = {
                headers[i]: cast_value(padded[i], schema.get(headers[i]))
                for i in range(len(headers))
            }
            result.append(d)
        return result

    def all(self) -> pd.DataFrame:
        return pd.DataFrame(self._raw_to_dicts())

    def count(self) -> int:
        _, rows = self._fetch_raw()
        return len(rows)

    def get(self, **kwargs) -> Optional[dict[str, Any]]:
        for row in self._raw_to_dicts():
            if all(row.get(k) == v for k, v in kwargs.items()):
                return row
        return None

    def filter(self, **kwargs) -> list[dict[str, Any]]:
        return [r for r in self._raw_to_dicts() if _matches_filter(r, kwargs)]

    def insert(self, data: dict[str, Any]) -> None:
        headers, _ = self._fetch_raw()
        if not headers:
            raise TableNotFound(f"Table '{self.name}' has no headers")
        _check_columns(data.keys(), headers)
        row = [_to_cell_value(data.get(h)) for h in headers]
        self._ws.append_row(row, value_input_option="USER_ENTERED")
        self._invalidate()

    def insert_many(self, rows: list[dict[str, Any]]) -> None:
        headers, _ = self._fetch_raw()
        if not headers:
            raise TableNotFound(f"Table '{self.name}' has no headers")
        values = [[_to_cell_value(r.get(h)) for h in headers] for r in rows]
        self._ws.append_rows(values, value_input_option="USER_ENTERED")
        self._invalidate()

    def update(self, data: dict[str, Any], where: dict[str, Any]) -> int:
        headers, raw_rows = self._fetch_raw()
        schema = self.schema()
        updated = 0
        for i, raw in enumerate(raw_rows):
            padded = raw + [""] * (len(headers) - len(raw))
            row_dict = {
                headers[j]: cast_value(padded[j], schema.get(headers[j]))
                for j in range(len(headers))
            }
            if all(row_dict.get(k) == v for k, v in where.items()):
                for col, val in data.items():
                    if col not in headers:
                        raise ColumnNotFound(col)
                    sheet_row = self._header_row + 1 + i
                    col_idx = headers.index(col) + 1
                    self._ws.update_cell(sheet_row, col_idx, _to_cell_value(val))
                updated += 1
        if updated:
            self._invalidate()
        return updated

    def delete(self, where: dict[str, Any]) -> int:
        headers, raw_rows = self._fetch_raw()
        schema = self.schema()
        to_delete = []
        for i, raw in enumerate(raw_rows):
            padded = raw + [""] * (len(headers) - len(raw))
            row_dict = {
                headers[j]: cast_value(padded[j], schema.get(headers[j]))
                for j in range(len(headers))
            }
            if all(row_dict.get(k) == v for k, v in where.items()):
                to_delete.append(i)
        for i in sorted(to_delete, reverse=True):
            sheet_row = self._header_row + 1 + i
            self._ws.delete_rows(sheet_row)
        if to_delete:
            self._invalidate()
        return len(to_delete)

    def rename_column(self, old_name: str, new_name: str) -> None:
        headers, _ = self._fetch_raw()
        if old_name not in headers:
            raise ColumnNotFound(old_name)
        col_idx = headers.index(old_name) + 1
        self._ws.update_cell(self._header_row, col_idx, new_name)
        self._invalidate()

    def __repr__(self) -> str:
        return f"Table(name={self.name!r})"


def _check_columns(cols, headers: list[str]) -> None:
    for col in cols:
        if col not in headers:
            raise ColumnNotFound(col)


def _matches_filter(row: dict, filters: dict) -> bool:
    for key, val in filters.items():
        if "__" in key:
            col, op = key.rsplit("__", 1)
            row_val = row.get(col)
            if op == "gt" and not (row_val is not None and row_val > val):
                return False
            elif op == "gte" and not (row_val is not None and row_val >= val):
                return False
            elif op == "lt" and not (row_val is not None and row_val < val):
                return False
            elif op == "lte" and not (row_val is not None and row_val <= val):
                return False
            elif op == "ne" and row_val == val:
                return False
            elif op == "like" and not (
                isinstance(row_val, str) and _like_match(row_val, val, case_sensitive=True)
            ):
                return False
            elif op == "ilike" and not (
                isinstance(row_val, str) and _like_match(row_val, val, case_sensitive=False)
            ):
                return False
            elif op == "in" and row_val not in val:
                return False
            elif op == "isnull" and (row_val is None) != val:
                return False
        else:
            if row.get(key) != val:
                return False
    return True


def _like_match(value: str, pattern: str, case_sensitive: bool = True) -> bool:
    import re

    regex = re.escape(pattern).replace(r"\%", ".*").replace(r"\_", ".")
    flags = 0 if case_sensitive else re.IGNORECASE
    return bool(re.fullmatch(regex, value, flags))
