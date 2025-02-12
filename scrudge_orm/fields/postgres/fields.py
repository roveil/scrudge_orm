from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, Type, Union

from scrudge_orm.fields.base import database_field, get_foreign_key_for_model
from scrudge_orm.fields.fields import ManyToManyRelationField as ManyToManyRelationFieldInternal
from scrudge_orm.fields.fields import OneToManyRelationField as OneToManyRelationFieldInternal
from scrudge_orm.fields.fields import OneToOneRelationField as OneToOneRelationFieldInternal
from scrudge_orm.fields.postgres.consts import PostgresFieldTypes

if TYPE_CHECKING:
    from re import Pattern

    from sqlalchemy.sql import ClauseElement
    from sqlalchemy.sql.elements import TextClause as SQLAlchemyTextClause
    from sqlalchemy.sql.functions import Function as SQLAlchemyFunction

    from scrudge_orm.models.postgres import PostgresModel


def PostgresField(
    field_type: PostgresFieldTypes,
    *sqlalchemy_args: Any,
    primary_key: bool = False,
    index: bool = False,
    unique: bool = False,
    nullable: bool = False,
    server_default: Optional[Union[str, "SQLAlchemyTextClause", "SQLAlchemyFunction", Any]] = None,
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
    to_model: Optional[Union[Type["PostgresModel"], str]] = None,
    to_model_column: Optional[str] = None,
    related_name: Optional[str] = None,
) -> Any:
    return database_field(
        field_type,
        *sqlalchemy_args,
        primary_key=primary_key,
        index=index,
        unique=unique,
        nullable=nullable,
        server_default=server_default,
        onupdate=onupdate,
        default=default,
        default_factory=default_factory,
        gt=gt,
        ge=ge,
        lt=lt,
        le=le,
        min_length=min_length,
        max_length=max_length,
        regex=regex,
        autoincrement=autoincrement,
        to_model=to_model,
        to_model_column=to_model_column,
        related_name=related_name,
    )


def PostgresForeignKey(
    to_model: Type["PostgresModel"] | str,
    to_model_column: Optional[str] = None,
    primary_key: bool = False,
    index: bool = True,
    unique: bool = False,
    nullable: bool = True,
    onupdate: Optional[Literal["CASCADE", "RESTRICT"]] = None,
    ondelete: Optional[Literal["CASCADE", "RESTRICT", "SET NULL"]] = None,
    deferrable: Optional[bool] = None,
    initially: Optional[Literal["IMMEDIATE", "DEFERRED"]] = None,
    default: Any = Ellipsis,
    related_name: Optional[str] = None,
) -> Any:
    sqlalchemy_fk, field_type = get_foreign_key_for_model(
        to_model,
        to_model_column=to_model_column,
        onupdate=onupdate,
        ondelete=ondelete,
        deferrable=deferrable,
        initially=initially,
    )

    return PostgresField(
        PostgresFieldTypes(field_type),
        sqlalchemy_fk,
        primary_key=primary_key,
        index=index,
        unique=unique,
        nullable=nullable,
        default=default,
        to_model=to_model,
        to_model_column=to_model_column,
        related_name=related_name,
    )


def ManyToManyRelationField(to_model: str | Type["PostgresModel"], through_model: str | Type["PostgresModel"]) -> Any:
    return ManyToManyRelationFieldInternal(to_model=to_model, through_model=through_model)


def OneToManyRelationField(to_model: str | Type["PostgresModel"]) -> Any:
    return OneToManyRelationFieldInternal(to_model=to_model)


def OneToOneRelationField(to_model: str | Type["PostgresModel"]) -> Any:
    return OneToOneRelationFieldInternal(to_model=to_model)
