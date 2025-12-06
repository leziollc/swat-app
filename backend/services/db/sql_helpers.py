from typing import Any, Dict, List, Optional, Tuple


class FilterCondition:
    """Simple filter condition for structured filtering.

    Attributes:
        column: column name (identifier)
        op: SQL operator as string, e.g. '=', '>', '<', 'LIKE', 'IN'
        value: value or list of values for the operator
    """

    def __init__(self, column: str, op: str, value: Any):
        self.column = column
        self.op = op.upper()
        self.value = value


IDENT_OPS = {"=", "!=", ">", "<", ">=", "<=", "LIKE", "IN"}


def _validate_identifier(name: Optional[str]) -> None:
    if not isinstance(name, str) or not name:
        raise ValueError("Invalid identifier")
    # conservative check
    if not name[0].isalpha() and name[0] != "_":
        raise ValueError("Invalid identifier")
    for ch in name:
        if not (ch.isalnum() or ch == "_"):
            raise ValueError("Invalid identifier")


def build_where_clause(filters: Optional[List[Dict[str, Any]]]) -> Tuple[str, List[Any]]:
    """Build a parameterized WHERE clause from structured filters.

    filters: list of dicts with keys: column, op, value
    Returns: (where_clause_sql, params_list)
    """
    if not filters:
        return "", []

    parts: List[str] = []
    params: List[Any] = []

    for cond in filters:
        column = cond.get("column")
        op = cond.get("op", "=")
        value = cond.get("value")

        _validate_identifier(column)

        if op.upper() not in IDENT_OPS:
            raise ValueError(f"Unsupported operator: {op}")

        if op.upper() == "IN":
            if not isinstance(value, (list, tuple)):
                raise ValueError("IN operator requires a list/tuple value")
            placeholders = ", ".join(["?"] * len(value))
            parts.append(f"{column} IN ({placeholders})")
            params.extend(list(value))
        else:
            parts.append(f"{column} {op} ?")
            params.append(value)

    where_clause = "WHERE " + " AND ".join(parts) if parts else ""
    return where_clause, params
