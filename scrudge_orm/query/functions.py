from typing import TYPE_CHECKING, Any

from sqlalchemy import func

if TYPE_CHECKING:
    from sqlalchemy import Table


class BaseFunction:
    name: str

    def __init__(self, column_name: str, *args: Any) -> None:
        self.column_name = column_name
        self.args = args

    def get_expression(self, table: "Table") -> Any:
        column = table.c[self.column_name]

        return getattr(func, self.name)(column, *self.args)


class ArrayRemove(BaseFunction):
    name: str = "array_remove"
