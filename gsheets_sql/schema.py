import re
from datetime import date, datetime
from typing import Any

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATE_BR_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")
_BOOL_TRUE = {"true", "false", "sim", "não", "yes", "no", "1", "0"}


def infer_type(value: str) -> str:
    if value is None or value == "":
        return "null"
    v = value.strip()
    if v.lower() in _BOOL_TRUE and v.lower() in ("true", "false", "sim", "não", "yes", "no"):
        return "bool"
    try:
        int(v)
        return "int"
    except ValueError:
        pass
    try:
        float(v.replace(",", "."))
        return "float"
    except ValueError:
        pass
    if _DATETIME_RE.match(v):
        return "datetime"
    if _DATE_RE.match(v):
        return "date"
    if _DATE_BR_RE.match(v):
        return "date"
    return "str"


def _to_cell_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return str(v)


def cast_value(value: str, type_hint: str = None) -> Any:
    if value is None or value == "":
        return None
    v = value.strip()
    t = type_hint or infer_type(v)

    if t == "bool":
        return v.lower() in ("true", "sim", "yes", "1")
    if t == "int":
        try:
            return int(v)
        except ValueError:
            return v
    if t == "float":
        try:
            return float(v.replace(",", "."))
        except ValueError:
            return v
    if t == "date":
        if _DATE_RE.match(v):
            return datetime.strptime(v, "%Y-%m-%d").date()
        if _DATE_BR_RE.match(v):
            return datetime.strptime(v, "%d/%m/%Y").date()
        return v
    if t == "datetime":
        try:
            return datetime.fromisoformat(v.replace("T", " "))
        except ValueError:
            return v
    return v


def infer_schema(rows: list[dict[str, str]]) -> dict[str, str]:
    if not rows:
        return {}
    columns = list(rows[0].keys())
    schema = {}
    for col in columns:
        types: set[str] = set()
        for row in rows:
            val = row.get(col, "")
            if val != "":
                types.add(infer_type(str(val)))
        types.discard("null")
        if not types:
            schema[col] = "str"
        elif types == {"int"}:
            schema[col] = "int"
        elif types <= {"int", "float"}:
            schema[col] = "float"
        elif types == {"bool"}:
            schema[col] = "bool"
        elif types == {"date"}:
            schema[col] = "date"
        elif types <= {"date", "datetime"}:
            schema[col] = "datetime"
        else:
            schema[col] = "str"
    return schema
