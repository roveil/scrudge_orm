from typing import TYPE_CHECKING, Any, DefaultDict, Dict, Iterable, Optional, Tuple, Type, Union

from pydantic import BaseModel, ConfigDict
from pydantic.fields import FieldInfo
from sqlalchemy.sql.schema import Column

from scrudge_orm.managers.base import DatabaseManager
from scrudge_orm.serialization.base_serializer_fields import (
    AutoPrefetchField,
    PrefetchFieldManyToMany,
    PrefetchFieldOneToMany,
    PrefetchFieldOneToOne,
)
from scrudge_orm.utils.imports import lazy_import
from scrudge_orm.utils.sqalchemy import find_foreign_key_relation

if TYPE_CHECKING:
    from scrudge_orm.models.base import DatabaseModel
    from scrudge_orm.query.queryset import QuerySet


def get_manager_by_model(model: Union[str, Type["DatabaseModel"]]) -> "DatabaseManager":
    from scrudge_orm.models.base import DatabaseModel, model_register

    if isinstance(model, str):
        try:
            model_obj = lazy_import(model)
        except (ValueError, ImportError) as e:
            if model not in model_register:
                raise ValueError(
                    f"{model} hasn't registered in database model registry. "
                    f"Supported models are: {','.join(model_register)}"
                ) from e
            result = model_register[model].objects
        else:
            if not issubclass(model_obj, DatabaseModel):
                raise ValueError(f"'{model}' must be instance of '{DatabaseModel}'")

            result = model_obj.objects
    else:
        if not issubclass(model, DatabaseModel):
            raise ValueError(f"'{model}' must be instance of '{DatabaseModel}'")

        result = model.objects

    return result


class DatabaseWithValidationField(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    sqlalchemy_column: Column
    pydantic_field: FieldInfo

    # foreign key info
    to_model: Optional[str | Type] = None
    to_model_column: Optional[str] = None
    _to_manager: Optional[DatabaseManager] = None
    related_name: Optional[str] = None

    ready_for_sql: bool = True  # can be set to False on DatabaseModelMeta

    @property
    def to_manager(self) -> Optional[DatabaseManager]:
        if self.to_model is None:
            return None

        if self._to_manager is None:
            self._to_manager = get_manager_by_model(self.to_model)

        return self._to_manager

    def get_typing(self) -> Any:
        return self.to_model

    def get_queryset_for_instance(self, database_model_instance: "DatabaseModel") -> "QuerySet":
        if not self.sqlalchemy_column.foreign_keys:
            raise AttributeError(f"There is no foreign_keys in '{self.sqlalchemy_column.name}'")

        if self.to_manager is None:
            raise ValueError("Destination manager unknowm")

        foreign_key = list(self.sqlalchemy_column.foreign_keys)[0]
        foreign_key_value = getattr(database_model_instance, foreign_key.parent.name)
        qs_params = {f"{foreign_key.column.name}": foreign_key_value}

        return self.to_manager.get(**qs_params)


class RelatedFieldBase(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    related_name: str = None  # type: ignore  # will be known in the process
    current_manager: DatabaseManager = None  # type: ignore  # will be known in the process

    to_model: Union[str, Type]
    _to_manager: DatabaseManager = None  # type: ignore  # will be counted in the process

    @property
    def to_manager(self) -> DatabaseManager:
        if self._to_manager is None:
            self._to_manager = get_manager_by_model(self.to_model)

        return self._to_manager

    def get_typing(self) -> Any:
        raise NotImplementedError()

    def get_queryset_for_instance(self, database_model_instance: "DatabaseModel") -> "QuerySet":
        raise NotImplementedError()

    def get_default_value(self) -> Any:
        raise NotImplementedError()

    @property
    def serializer(self) -> Type[AutoPrefetchField]:
        raise NotImplementedError()

    def get_current_model_to_relation_columns(self) -> Tuple["Column", "Column"]:
        raise NotImplementedError()

    async def serialize(
        self,
        columns_to_select: Optional[Iterable["Column"]],
        objs: Tuple[Dict, ...],
        root_model: Type["DatabaseModel"],
        prefetch_field_root_model_name: str,
        objs_by_relation_attr: DefaultDict,
        additional_prefetch_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        return await self.serializer(self, columns_to_select=columns_to_select).serialize(
            objs,
            root_model,
            prefetch_field_root_model_name,
            objs_by_relation_attr,
            additional_prefetch_data=additional_prefetch_data,
        )


class OneToOneRelationField(RelatedFieldBase):
    def get_typing(self) -> Any:
        return self.to_model

    def get_default_value(self) -> Any:
        return None

    def get_current_model_to_relation_columns(self) -> Tuple["Column", "Column"]:
        """
        Returns tuple with dst model column to current model and current model column as source
        :return:
        """
        return find_foreign_key_relation(self.to_manager.table, self.current_manager.table)

    def get_queryset_for_instance(self, database_model_instance: "DatabaseModel") -> "QuerySet":
        destination_model_col_to_current, current_model_column = self.get_current_model_to_relation_columns()
        foreign_key_value = getattr(database_model_instance, current_model_column.name)
        qs_params = {f"{destination_model_col_to_current.name}": foreign_key_value}

        return self.to_manager.get(**qs_params)

    @property
    def serializer(self) -> Type[PrefetchFieldOneToOne]:
        return PrefetchFieldOneToOne


class OneToManyRelationField(OneToOneRelationField):
    def get_typing(self) -> Any:
        return Tuple[self.to_model, ...]

    def get_default_value(self) -> Any:
        return []

    def get_queryset_for_instance(self, database_model_instance: "DatabaseModel") -> "QuerySet":
        destination_model_col_to_current, current_model_column = find_foreign_key_relation(
            self.to_manager.table, self.current_manager.table
        )
        foreign_key_value = getattr(database_model_instance, current_model_column.name)
        qs_params = {f"{destination_model_col_to_current.name}": foreign_key_value}

        return self.to_manager.filter(**qs_params)

    @property
    def serializer(self) -> Type[PrefetchFieldOneToMany]:  # type: ignore
        return PrefetchFieldOneToMany


class ManyToManyRelationField(OneToManyRelationField):
    through_model: Union[str, Type]
    _through_manager: DatabaseManager = None  # type: ignore  # will be counted in the process

    @property
    def through_manager(self) -> DatabaseManager:
        """
        Can't use cached_property here due to attrs class
        """
        if self._through_manager is None:
            self._through_manager = get_manager_by_model(self.through_model)

        return self._through_manager

    def get_current_model_to_relation_columns(self) -> Tuple["Column", "Column"]:
        """
        Returns tuple with dst model column to current model and current model column as source
        :return:
        """
        return find_foreign_key_relation(self.through_manager.table, self.current_manager.table)

    def get_queryset_for_instance(self, database_model_instance: "DatabaseModel") -> "QuerySet":
        _, current_model_column = self.get_current_model_to_relation_columns()

        return self.current_manager.get_prefetch_related_queryset_m2m_single(
            self, getattr(database_model_instance, current_model_column.name)
        )

    @property
    def serializer(self) -> Type[PrefetchFieldManyToMany]:  # type: ignore
        return PrefetchFieldManyToMany
