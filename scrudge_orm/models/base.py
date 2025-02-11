import logging
from collections import defaultdict
from copy import deepcopy
from functools import lru_cache, partial
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    DefaultDict,
    Dict,
    Final,
    ForwardRef,
    Iterable,
    List,
    Literal,
    Optional,
    Self,
    Set,
    Tuple,
    Type,
    TypeVar,
)

from pydantic import BaseModel, ConfigDict, Field
from pydantic._internal._model_construction import ModelMetaclass
from pydantic.errors import PydanticUndefinedAnnotation
from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy import Index as SQLAlchemyIndex

from scrudge_orm.fields.fields import DatabaseWithValidationField, RelatedFieldBase
from scrudge_orm.models.indexes.base import Index, UniqueIndex
from scrudge_orm.utils.convert_to_model import convert_to_model
from scrudge_orm.utils.naming import get_register_model_name, get_table_name_for_class
from scrudge_orm.utils.typehint import find_annotation_on_class_creation, is_forward_ref, is_optional

if TYPE_CHECKING:
    from scrudge_orm.backends.base import DatabaseBackend
    from scrudge_orm.managers.base import DatabaseManager
    from scrudge_orm.serialization.base_serializers import BaseModelSerializer

logger = logging.getLogger(__name__)

model_register: Dict[str, Type["DatabaseModel"]] = {}

_record_not_ready: Final = type("ObjectsNotFetchedFromDatabase", (object,), {})()

_models_with_forward_refs: Set[str] = set()
_not_ready_sql_models: Set[str] = set()

_rebuilded_models: Set[str] = set()
_create_model_callbacks: DefaultDict[str, List[Callable]] = defaultdict(list)


def _create_model_callback_function(
    forward_model: Type["DatabaseModel"],
    model: Type["DatabaseModel"],
    related_field: DatabaseWithValidationField | RelatedFieldBase,
) -> None:
    if model.register_name in _rebuilded_models:
        return None

    field_related_name: str = related_field.related_name  # type: ignore
    related_field.to_model = forward_model

    model.__annotations__[field_related_name] = related_field.get_typing()
    model.model_fields[field_related_name] = Field(default=_record_not_ready)
    model.model_fields[field_related_name].annotation = model.__annotations__[field_related_name]

    try:
        model.model_rebuild()
    except PydanticUndefinedAnnotation:
        return None

    if model.register_name in _not_ready_sql_models:
        DatabaseModelMeta.modify_foreign_keys_for_model(model)
        DatabaseModelMeta.setup_database_structure_and_manager(model)
        _not_ready_sql_models.remove(model.register_name)

    _rebuilded_models.add(model.register_name)

    # model is ready
    for callback_func in _create_model_callbacks[model.register_name]:
        try:
            callback_func(model)
        except PydanticUndefinedAnnotation:
            pass


class MetaBaseType(type):
    def __new__(mcs: Type, name: str, bases: Tuple[Type, ...], attrs: Dict) -> Type:
        klass: Type[MetaBase] = super().__new__(mcs, name, bases, attrs)
        klass_name = attrs["__qualname__"].split(".")[-1]
        is_inheritance_class = attrs.get(f"_{klass_name}__is_inheritance_class", False)

        if not is_inheritance_class:
            if not klass.is_proxy and klass.db_backend is None:
                raise AttributeError("'db_backend' should be defined")

        return klass


class MetaBase(metaclass=MetaBaseType):
    is_proxy: bool = False  # can be used for create mixin classes

    db_backend: Optional["DatabaseBackend"] = None
    manager_cls: Optional[Type["DatabaseManager"]] = None

    unique_indexes: Tuple[UniqueIndex, ...] = ()
    indexes: Tuple[Index, ...] = ()
    table_name: Optional[str] = None
    visible_name: Optional[str] = None

    serializer_omit_fields: Optional[Iterable[str]] = None

    __is_inheritance_class: bool = True  # skip validation for inheritance classes

    @classmethod
    def update_sqlalchemy_metadata(cls, model: Type["DatabaseModel"]) -> Optional[Table]:
        if cls.db_backend is None:
            return None

        class_name = model.__name__
        table_name = model.get_table_name()

        assert (
            len(table_name) <= 63
        ), f"Generated table name '{table_name}' for class '{class_name}' more than allowed 63 symbols"

        for attr_name, v in model.scrudge_db_fields.items():
            v.sqlalchemy_column.name = attr_name

        sql_alchemy_table = Table(
            table_name, cls.db_backend.metadata, *[v.sqlalchemy_column for v in model.scrudge_db_fields.values()]
        )

        for index in chain(cls.indexes, cls.unique_indexes):
            index_name = f"{table_name}_{index.name}_idx"
            assert (
                len(index_name) <= 63
            ), f"Generated index name '{index_name}' for class '{class_name}' more than allowed 63 symbols"

            SQLAlchemyIndex(
                index_name,
                *[getattr(sql_alchemy_table.c, col_name) for col_name in index.fields],
                **index.get_create_index_kwargs(),
            )

        return sql_alchemy_table


class DatabaseModelMeta(ModelMetaclass):
    def __new__(mcs, name: str, bases: Tuple[Type, ...], attrs: Dict) -> Type:
        meta_cls: Optional[Type[MetaBase]] = attrs.get("Meta")
        assert meta_cls is not None and issubclass(
            meta_cls, MetaBase
        ), f"class Meta should be defined and being subclass of '{MetaBase}'"

        # save information about DatabaseWithValidationField into special dictionary
        database_fields: Dict[str, DatabaseWithValidationField] = {}
        related_fields: Dict[str, DatabaseWithValidationField | RelatedFieldBase] = {}
        additional_attrs = {}

        for attr_name, value in attrs.items():
            annotation = find_annotation_on_class_creation(attr_name, attrs, bases)

            if isinstance(value, DatabaseWithValidationField):
                database_fields[attr_name] = value
                additional_attrs[attr_name] = value.pydantic_field

                try:
                    assert annotation is not None, f"Annotation must be provided for field '{attr_name}'"
                except AssertionError as exc:
                    raise exc

                field_is_optional = is_optional(annotation)
                col_nullable = value.sqlalchemy_column.nullable

                if field_is_optional and not col_nullable:
                    raise AttributeError("Optional field must be Nullable")

                if col_nullable and not field_is_optional:
                    raise AttributeError("Nullable column must be Optional")

                # setup support of select_related joins
                if value.to_model is not None:
                    if value.related_name is None:
                        value.related_name = "_".join(attr_name.split("_")[:-1])

                    if isinstance(value.to_model, str) and value.to_model not in model_register:
                        # foreign key model is not ready
                        attrs["__annotations__"][value.related_name] = ForwardRef(value.to_model)
                        value.ready_for_sql = False
                    else:
                        to_model = model_register[value.to_model] if isinstance(value.to_model, str) else value.to_model

                        # in this case model is ready
                        attrs["__annotations__"][value.related_name] = to_model

                    additional_attrs[value.related_name] = Field(default=_record_not_ready)
                    related_fields[value.related_name] = value

            elif isinstance(value, RelatedFieldBase):
                value.related_name = attr_name
                related_fields[attr_name] = value
                additional_attrs[attr_name] = Field(default=_record_not_ready)

        attrs.update(additional_attrs)

        is_root_database_model = mcs.__module__ == attrs.get("__module__") and name == "DatabaseModel"

        # DatabaseModel is base class for all models. Skip
        if not is_root_database_model:
            for base_cls in reversed(bases):
                if issubclass(base_cls, DatabaseModel):
                    # need to deepcopy sqlalchemy column, we can't same instance of column between tables
                    database_fields.update(
                        {
                            k: DatabaseWithValidationField(
                                pydantic_field=v.pydantic_field, sqlalchemy_column=deepcopy(v.sqlalchemy_column)
                            )
                            for k, v in base_cls.scrudge_db_fields.items()
                        }
                    )
                    related_fields.update(base_cls.related_fields)

        klass: Type["DatabaseModel"] = super().__new__(mcs, name, bases, attrs)

        type.__setattr__(klass, "scrudge_db_fields", database_fields)
        type.__setattr__(klass, "related_fields", related_fields)
        type.__setattr__(klass, "register_name", get_register_model_name(klass.__module__, klass.__name__))

        model_register[klass.register_name] = klass

        for related_field_name, related_field in related_fields.items():
            forward_model_name: Optional[str] = None

            if isinstance(related_field, RelatedFieldBase):
                type.__setattr__(klass, related_field_name, related_field)
                annotation = klass.__annotations__.get(related_field_name)

                if is_forward_ref(annotation):
                    # support of forward ref future annotations
                    forward_model_name = (
                        related_field.to_model.register_name  # type: ignore
                        if isinstance(related_field.to_model, DatabaseModel)
                        else related_field.to_model
                    )
                    # Support for partial initialization for debug purposes (in python console for example)
                    # to avoid model_rebuild() requirement
                    klass.__annotations__[related_field_name] = Tuple
                    klass.model_fields[related_field_name].annotation = Tuple  # type: ignore
            else:
                # DatabaseWithValidationField foreign key for not imported model
                if isinstance(related_field.to_model, str) and related_field.to_model not in model_register:
                    forward_model_name = related_field.to_model

                    # this is the only one case, when model not ready for building sql
                    _not_ready_sql_models.add(klass.register_name)

            if forward_model_name is not None:
                _create_model_callbacks[forward_model_name].append(
                    partial(_create_model_callback_function, model=klass, related_field=related_field)
                )
                _models_with_forward_refs.add(klass.register_name)

        if klass.register_name not in _not_ready_sql_models:
            mcs.setup_database_structure_and_manager(klass)

        if not is_root_database_model:
            from scrudge_orm.serialization.base_serializers import BaseModelSerializer

            type.__setattr__(
                klass, "serializer_class", convert_to_model(klass, BaseModelSerializer, meta_cls.serializer_omit_fields)
            )

        return klass

    @staticmethod
    def modify_foreign_keys_for_model(model: Type["DatabaseModel"]) -> None:
        for related_field in model.related_fields.values():
            if isinstance(related_field, DatabaseWithValidationField) and not related_field.ready_for_sql:
                from scrudge_orm.fields.base import get_foreign_key_for_model

                fk_old: ForeignKey = list(related_field.sqlalchemy_column.foreign_keys)[0]
                sqlalchemy_fk, field_type = get_foreign_key_for_model(
                    related_field.to_model,  # type: ignore
                    related_field.to_model_column,
                    onupdate=fk_old.onupdate,  # type: ignore
                    ondelete=fk_old.ondelete,  # type: ignore
                    deferrable=fk_old.deferrable,
                    initially=fk_old.initially,  # type: ignore
                )
                column_old = related_field.sqlalchemy_column

                related_field.sqlalchemy_column = Column(
                    field_type,
                    sqlalchemy_fk,
                    primary_key=column_old.primary_key,
                    index=column_old.index,
                    unique=column_old.unique,
                    nullable=column_old.nullable,
                    default=column_old.default,
                )

    @staticmethod
    def setup_database_structure_and_manager(model: Type["DatabaseModel"]) -> None:
        if not model.Meta.is_proxy:
            sql_alchemy_table = model.Meta.update_sqlalchemy_metadata(model)

            if (
                model.Meta.manager_cls is not None
                and sql_alchemy_table is not None
                and model.Meta.db_backend is not None
            ):
                manager = model.Meta.manager_cls(sql_alchemy_table, model, model.Meta.db_backend)
                type.__setattr__(model, "objects", manager)
                type.__setattr__(model, "connection", model.objects.pool)

                for related_field in model.related_fields.values():
                    if isinstance(related_field, RelatedFieldBase):
                        related_field.current_manager = manager

    def __init__(cls, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        model_register_name = cls.register_name  # type: ignore

        # support of ForwardRef annotations. Need to call model_rebuild() method
        # if there is no ForwardRefs in model and other model are linked to this model, will try to rebuild
        if model_register_name not in _models_with_forward_refs:
            for callback_func in _create_model_callbacks[model_register_name]:
                callback_func(cls)


DatabaseModelTypeVar = TypeVar("DatabaseModelTypeVar", bound="DatabaseModel")


class DatabaseModel(BaseModel, metaclass=DatabaseModelMeta):
    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=True)

    def __getattribute__(self, item: str) -> Any:
        if (
            item in object.__getattribute__(self, "related_fields")
            and object.__getattribute__(self, "__dict__").get(item) == _record_not_ready
        ):
            raise AttributeError(f"Attribute '{item}' hasn't fetched yet. Use await {item}__qs to fetch.")
        else:
            return super().__getattribute__(item)

    def __getattr__(self, item: str) -> Any:
        if len((splitted_item := item.split("__qs"))) == 2:
            related_name, _ = splitted_item
            related_field = self.related_fields.get(related_name)

            if related_field is None or related_field.related_name is None or related_field.to_manager is None:
                raise AttributeError(f"{self.__class__.__name__} object has no attribute {item}")

            queryset = related_field.get_queryset_for_instance(self)

            # callback to set returning values into self attributes
            queryset = queryset.add_callback(partial(object.__setattr__, self, related_field.related_name))

            object.__setattr__(self, f"{related_field.related_name}__qs", queryset)

            return queryset
        elif item in self.related_fields:
            raise AttributeError(f"Attribute '{item}' hasn't fetched yet. Use await {item}__qs to fetch.")
        else:
            return super().__getattr__(item)  # type: ignore

    # populated by the metaclass, defined here to help IDEs only
    if TYPE_CHECKING:
        objects: ClassVar[DatabaseManager]
        scrudge_db_fields: ClassVar[Dict[str, DatabaseWithValidationField]]
        related_fields: ClassVar[Dict[str, DatabaseWithValidationField | RelatedFieldBase]]

        class serializer_class(BaseModelSerializer):
            pass

        connection: ClassVar[DatabaseBackend]
        register_name: ClassVar[str]

    class Meta(MetaBase):
        is_proxy = True

    @classmethod
    @lru_cache
    def get_visible_name(cls) -> str:
        return cls.Meta.visible_name or cls.__name__

    @classmethod
    @lru_cache
    def get_table_name(cls) -> str:
        table_name = cls.Meta.table_name

        if table_name is None:
            table_name = get_table_name_for_class(cls.__name__, cls.__module__)

        return table_name

    @classmethod
    @lru_cache
    def get_pk_column_name(cls) -> str:
        pk_columns = [
            col_name
            for col_name, field_value in cls.scrudge_db_fields.items()
            if field_value.sqlalchemy_column.primary_key
        ]

        if not pk_columns:
            raise AttributeError(f"Can't find any postgresql pk for {cls.__name__}")
        if len(pk_columns) > 1:
            logger.warning(f"Found several postgresql pk for " f"table {cls.__name__}, using one of them...")

        return pk_columns[0]

    @classmethod
    def serialize_bulk(cls, data: Iterable[Self | Dict]) -> Any:
        return tuple(item.model_dump() if isinstance(item, cls) else item for item in data)

    @classmethod
    def to_models_bulk(cls, data: Iterable[Dict]) -> Tuple[Self, ...]:
        return tuple(cls(**item) for item in data)

    async def save(self) -> Self:
        pk_field = self.get_pk_column_name()
        pk = getattr(self, pk_field)

        update_data = {item: getattr(self, item) for item in self.scrudge_db_fields}

        if pk is not None:
            update_data.pop(pk_field)
            await self.update(**update_data)
        else:
            result: Self = await self.objects.create(**update_data)
            setattr(self, pk_field, getattr(result, pk_field))

        return self

    async def update(self, **field_values: Any) -> Self:
        pk_field = self.get_pk_column_name()
        pk_field_value = getattr(self, pk_field)

        field_values.update(self.objects.get_table_onupdate_defaults)

        updated_reply_data = (
            await self.objects.filter(**{pk_field: pk_field_value})
            .update(**field_values, returning=field_values)
            .fetch_single()
        )

        for attr_name in field_values:
            setattr(self, attr_name, updated_reply_data[attr_name])

        return self

    async def delete(self, returning: Optional[str | Iterable[str] | Literal["*"]] = None) -> "DatabaseModelTypeVar":
        pk_field = self.get_pk_column_name()
        pk_field_value = getattr(self, pk_field)

        return await self.objects.filter(**{pk_field: pk_field_value}).delete(returning=returning).fetch_single()
