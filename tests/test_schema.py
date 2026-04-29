from datetime import date

import pytest

from gsheets_sql.schema import cast_value, infer_schema, infer_type


class TestInferType:
    def test_int(self):
        assert infer_type("42") == "int"
        assert infer_type("-7") == "int"

    def test_float(self):
        assert infer_type("3.14") == "float"
        assert infer_type("1,5") == "float"

    def test_bool(self):
        assert infer_type("TRUE") == "bool"
        assert infer_type("false") == "bool"
        assert infer_type("sim") == "bool"
        assert infer_type("yes") == "bool"

    def test_date(self):
        assert infer_type("2024-01-15") == "date"
        assert infer_type("15/01/2024") == "date"

    def test_datetime(self):
        assert infer_type("2024-01-15 10:30") == "datetime"
        assert infer_type("2024-01-15T10:30:00") == "datetime"

    def test_str(self):
        assert infer_type("hello world") == "str"
        assert infer_type("R$ 100") == "str"

    def test_null(self):
        assert infer_type("") == "null"
        assert infer_type(None) == "null"


class TestCastValue:
    def test_int(self):
        assert cast_value("42") == 42
        assert isinstance(cast_value("42"), int)

    def test_float(self):
        assert cast_value("3.14") == pytest.approx(3.14)

    def test_bool_true(self):
        assert cast_value("TRUE") is True
        assert cast_value("sim") is True

    def test_bool_false(self):
        assert cast_value("false") is False

    def test_date(self):
        assert cast_value("2024-01-15") == date(2024, 1, 15)

    def test_date_br(self):
        assert cast_value("15/01/2024") == date(2024, 1, 15)

    def test_empty_returns_none(self):
        assert cast_value("") is None
        assert cast_value(None) is None

    def test_explicit_type_hint(self):
        assert cast_value("42", "str") == "42"
        assert cast_value("42", "float") == pytest.approx(42.0)


class TestInferSchema:
    def test_mixed_columns(self):
        rows = [
            {"id": "1", "nome": "Ana", "idade": "28", "ativo": "TRUE"},
            {"id": "2", "nome": "Bob", "idade": "35", "ativo": "FALSE"},
        ]
        schema = infer_schema(rows)
        assert schema["id"] == "int"
        assert schema["nome"] == "str"
        assert schema["idade"] == "int"
        assert schema["ativo"] == "bool"

    def test_empty_rows(self):
        assert infer_schema([]) == {}

    def test_all_empty_cells_default_str(self):
        rows = [{"col": ""}, {"col": ""}]
        assert infer_schema(rows)["col"] == "str"

    def test_int_and_float_promotes_to_float(self):
        rows = [{"val": "1"}, {"val": "1.5"}]
        assert infer_schema(rows)["val"] == "float"
