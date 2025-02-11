from functools import lru_cache
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Type, TypeVar

from sqlalchemy.ext.declarative import declarative_base

if TYPE_CHECKING:
    from sqlalchemy import Column, Table

    from scrudge_orm.models.base import DatabaseModelTypeVar

T = TypeVar("T")


def to_declarative_model(model: Type["DatabaseModelTypeVar"]) -> Any:
    base = declarative_base()

    attrs = {"__table__": model.objects.table}

    return type(f"{model.__name__}", (base,), attrs)


def get_table_unique_constraints_fields(table: "Table") -> Dict[Tuple[str, ...], Optional[str]]:
    unique_with_where_part: Dict[Tuple[str, ...], Optional[str]] = {
        (column.name,): None for column in table.c if column.unique
    }

    for index in table.indexes:
        if index.unique:
            where_part = index.dialect_options["postgresql"]._non_defaults.get("where")
            unique_with_where_part[tuple(column.name for column in index.columns)] = where_part

    return unique_with_where_part


@lru_cache
def find_foreign_key_relation(from_table: "Table", to_table: "Table") -> Tuple["Column", "Column"]:
    for fk in from_table.foreign_keys:
        if fk.column.table.name == to_table.name:
            from_column = fk.parent
            to_column = fk.column
            break
    else:
        raise AttributeError(f"There is no foreign key found from '{from_table.name}' to {to_table.name}")

    return from_column, to_column
