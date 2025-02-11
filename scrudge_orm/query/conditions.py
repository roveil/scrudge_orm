from enum import Enum
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from sqlalchemy import Table


class BaseCondition:
    def __init__(self, *conditions: "BaseCondition", **fields_values: Any):
        self.conditions = conditions
        self.fields_values = fields_values


class AndCondition(BaseCondition):
    pass


class OrCondition(BaseCondition):
    pass


class F:
    """
    Realises the option to use expressions using columns itself.
    For example: updated__gte=F('created')
    It will filter records with updated >= created
    """

    def __init__(self, column_name: str) -> None:
        self.column_name = column_name
        self.value_to_sum = None

    def get_expression(self, table: "Table") -> Any:
        column = table.c[self.column_name]

        return column if self.value_to_sum is None else column + self.value_to_sum

    def __add__(self, other: Any) -> Self:
        if self.value_to_sum is None:
            self.value_to_sum = other
        else:
            self.value_to_sum += other

        return self

    def __sub__(self, other: Any) -> Self:
        if self.value_to_sum is None:
            self.value_to_sum = -other
        else:
            self.value_to_sum -= other

        return self


class SupportedOperator(str, Enum):
    eq = "eq"
    neq = "neq"
    ge = "ge"
    gt = "gt"
    lt = "lt"
    le = "le"
    between = "between"
    in_ = "in"
    not_in = "not_in"
    is_ = "is"
    is_not = "is_not"
    like = "like"
    ilike = "ilike"
    not_like = "not_like"
    not_ilike = "not_ilike"
    startswith = "startswith"  # type: ignore
    endswith = "endswith"  # type: ignore
    contains = "contains"
    concat = "concat"

    @classmethod
    def get_operator(cls, value: str) -> Self:
        try:
            return cls(value)
        except ValueError as exc:
            raise ValueError(f"Unsupported operator - '{value}'") from exc
