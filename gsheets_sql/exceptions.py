class GSheetsSQLError(Exception):
    pass


class AuthError(GSheetsSQLError):
    pass


class TableNotFound(GSheetsSQLError):
    pass


class ColumnNotFound(GSheetsSQLError):
    pass


class QuerySyntaxError(GSheetsSQLError):
    pass


class QuotaExceeded(GSheetsSQLError):
    pass


class SchemaError(GSheetsSQLError):
    pass
