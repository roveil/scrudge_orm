from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, Tuple, Type, Union

from pydantic import Field
from sqlalchemy import BIGINT, Column, ForeignKey

from scrudge_orm.fields.consts import DatabaseFieldTypes
from scrudge_orm.fields.fields import DatabaseWithValidationField

if TYPE_CHECKING:
    from re import Pattern

    from sqlalchemy.sql import ClauseElement
    from sqlalchemy.sql.elements import TextClause as SQLAlchemyTextClause
    from sqlalchemy.sql.functions import Function as SQLAlchemyFunction

    from scrudge_orm.models.base import DatabaseModel


def database_field(
    field_type: DatabaseFieldTypes,
    *sqlalchemy_args: Any,
    primary_key: bool = False,
    index: bool = False,
    unique: bool = False,
    nullable: bool = True,
    server_default: Optional[Union[str, "SQLAlchemyTextClause", "SQLAlchemyFunction"]] = None,
    onupdate: Optional[Union[Callable, "SQLAlchemyFunction", "ClauseElement"]] = None,
    default: Any = Ellipsis,
    default_factory: Optional[Callable] = None,
    gt: Optional[Union[int, float]] = None,
    ge: Optional[Union[int, float]] = None,
    lt: Optional[Union[int, float]] = None,
    le: Optional[Union[int, float]] = None,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    regex: Optional[Union[str, "Pattern"]] = None,
    autoincrement: bool = False,
    to_model: Optional[Union[Type["DatabaseModel"], str]] = None,
    to_model_column: Optional[str] = None,
    related_name: Optional[str] = None,
) -> DatabaseWithValidationField:
    if (nullable and default == Ellipsis) or primary_key:
        default = None

    # A name for a column will set up later in model metaclass
    return DatabaseWithValidationField(
        sqlalchemy_column=Column(
            field_type.value,
            *sqlalchemy_args,
            autoincrement=True if autoincrement else "auto",
            primary_key=primary_key,
            index=index,
            unique=unique,
            nullable=nullable,
            server_default=server_default,
            onupdate=onupdate,
            default=default if default != Ellipsis else None,
        ),
        pydantic_field=Field(  # type: ignore
            default=default,
            default_factory=default_factory,
            gt=gt,
            ge=ge,
            lt=lt,
            le=le,
            min_length=min_length,
            max_length=max_length,
            regex=regex,
        ),
        to_model=to_model,
        to_model_column=to_model_column,
        related_name=related_name,
    )


def get_foreign_key_for_model(
    to_model: Union[Type["DatabaseModel"], str],
    to_model_column: Optional[str] = None,
    onupdate: Optional[Literal["CASCADE", "RESTRICT"]] = None,
    ondelete: Optional[Literal["CASCADE", "RESTRICT", "SET NULL"]] = None,
    deferrable: Optional[bool] = None,
    initially: Optional[Literal["IMMEDIATE", "DEFERRED"]] = None,
) -> Tuple[ForeignKey, Type]:
    from scrudge_orm.models.base import model_register

    # HACK. Related model is not imported yet.
    # This parameters will be changed in the future, when related class will be imported by import callback logic
    if isinstance(to_model, str) and to_model not in model_register:
        # set some default parameters
        field_type = BIGINT
        sqlalchemy_fk = ForeignKey(
            to_model, onupdate=onupdate, ondelete=ondelete, deferrable=deferrable, initially=initially
        )

        return sqlalchemy_fk, field_type

    if isinstance(to_model, str):
        to_model = model_register[to_model]

    to_model_column = to_model_column or to_model.get_pk_column_name()
    field_type = type(to_model.scrudge_db_fields[to_model_column].sqlalchemy_column.type)
    foreign_key_field = f"{to_model.get_table_name()}.{to_model_column}"
    sqlalchemy_fk = ForeignKey(
        foreign_key_field, onupdate=onupdate, ondelete=ondelete, deferrable=deferrable, initially=initially
    )

    return sqlalchemy_fk, field_type
