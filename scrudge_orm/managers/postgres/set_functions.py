from typing import TYPE_CHECKING

from sqlalchemy import func, select, text

from scrudge_orm.managers.set_functions import BaseSetFunction

if TYPE_CHECKING:
    from sqlalchemy import Column
    from sqlalchemy.sql.elements import ColumnElement


class PGArrayUnionSetFunction(BaseSetFunction):
    name = "pg_array_union"

    def get_expression(self, column: "Column", column_with_value_to_set: "Column") -> "ColumnElement":
        source_column_expression = (
            func.coalesce(text(str(column)), func.cast("{}", column.type)) if column.nullable else text(str(column))
        )
        expression = (
            select(func.unnest(func.array_cat(source_column_expression, text(str(column_with_value_to_set)))))  # type: ignore
            .distinct()
            .scalar_subquery()
        )

        if self.set_func_args:
            expression = expression.limit(self.set_func_args)

        return func.array(expression)
