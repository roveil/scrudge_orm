from functools import lru_cache
from typing import TYPE_CHECKING

from sqlalchemy import func

from scrudge_orm.utils.imports import get_all_subclasses

if TYPE_CHECKING:
    from sqlalchemy import Column
    from sqlalchemy.sql.elements import ColumnElement


class BaseSetFunction:
    name: str

    def __init__(self, set_func_args: str) -> None:
        self.set_func_args = set_func_args

    @classmethod
    @lru_cache
    def get_instance_by_name(cls, name: str) -> "BaseSetFunction":
        from .postgres import set_functions  # noqa

        for klass in get_all_subclasses(cls):
            set_func_name, _, set_func_args = name.partition("__")

            if klass.name == set_func_name:
                return klass(set_func_args)

        raise NotImplementedError(f"SetFunction klass not implemented for '{name}'")

    def get_expression(self, column: "Column", column_with_value_to_set: "Column") -> "ColumnElement":
        """
        Generate sqlalchemy expression for column
        :param column: destination column to set new value
        :param column_with_value_to_set: column with value from query to change in destination column
        :return: sqlalchemy expression
        """
        raise NotImplementedError()


class IncrementSetFunction(BaseSetFunction):
    name = "+"

    def get_expression(self, column: "Column", column_with_value_to_set: "Column") -> "ColumnElement":
        source_column_expression = func.coalesce(column, 0) if column.nullable else column

        return source_column_expression + column_with_value_to_set


class DecrementSetFunction(BaseSetFunction):
    name = "-"

    def get_expression(self, column: "Column", column_with_value_to_set: "Column") -> "ColumnElement":
        source_column_expression = func.coalesce(column, 0) if column.nullable else column

        return source_column_expression - column_with_value_to_set


class GreatestSetFunction(BaseSetFunction):
    name = "greatest"

    def get_expression(self, column: "Column", column_with_value_to_set: "Column") -> "ColumnElement":
        return func.greatest(column, column_with_value_to_set)


class EQNotNULLSetFunction(BaseSetFunction):
    name = "eq_not_null"

    def get_expression(self, column: "Column", column_with_value_to_set: "Column") -> "ColumnElement":
        # column to set is null, existing value will set
        return func.coalesce(column_with_value_to_set, column)
