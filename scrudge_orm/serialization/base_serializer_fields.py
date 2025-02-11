from functools import partial
from typing import TYPE_CHECKING, Any, DefaultDict, Dict, Iterable, Optional, Tuple, Type

from scrudge_orm.utils.sqalchemy import find_foreign_key_relation

if TYPE_CHECKING:
    from sqlalchemy.sql import ColumnElement

    from scrudge_orm.fields.fields import ManyToManyRelationField, OneToManyRelationField, OneToOneRelationField
    from scrudge_orm.models.base import DatabaseModel
    from scrudge_orm.serialization.base_serializers import BaseModelSerializer


class PrefetchFieldBase:
    def relation_attribute(self, root_model: Type["DatabaseModel"]) -> str:
        """
        Name of relation attribute of objects to serialize. By this field objs_by_relation_attr will construct
        :param root_model: Serializer root model class
        :return: name of attribute
        """
        raise NotImplementedError()

    @property
    def default_value(self) -> Any:
        return None

    async def serialize(
        self,
        objs: Tuple[Dict, ...],
        root_model: Type["DatabaseModel"],
        prefetch_field_root_model_name: str,
        objs_by_relation_attr: DefaultDict,
        additional_prefetch_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        raise NotImplementedError()


class AutoPrefetchField(PrefetchFieldBase):
    def __init__(
        self,
        source_field: Any,
        serializer_class: Optional[Type["BaseModelSerializer"]] = None,
        columns_to_select: Optional[Iterable["ColumnElement"]] = None,
    ) -> None:
        self.source_field = source_field
        self.serializer_class = serializer_class
        self.columns_to_select = columns_to_select

    @property
    def default_value(self) -> Any:
        return []

    def get_columns_to_select(self) -> Optional[Tuple["ColumnElement", ...]]:
        if self.columns_to_select is not None:
            columns_to_select = tuple(self.columns_to_select)

        elif self.serializer_class is not None:
            column_names_to_select, _, _, _, _ = self.serializer_class.parse_fields()
            columns_to_select = tuple(
                getattr(self.source_field.to_manager.table.c, col_name) for col_name in column_names_to_select
            )
        else:
            columns_to_select = None

        return columns_to_select


class PrefetchFieldManyToMany(AutoPrefetchField):
    _m2m_aggr_field = "main_aggregated"

    if TYPE_CHECKING:

        def __init__(
            self, source_field: "ManyToManyRelationField", serializer_class: Type["BaseModelSerializer"]
        ) -> None:
            super().__init__(source_field, serializer_class)

    def relation_attribute(self, root_model: Type["DatabaseModel"]) -> str:
        _, root_model_column = find_foreign_key_relation(
            self.source_field.through_manager.table, root_model.objects.table
        )
        return root_model_column.name

    async def serialize_prefetched_data(
        self,
        prefetched_results: Tuple[Dict, ...],
        prefetch_field_root_model_name: str,
        objs_by_relation_attr: DefaultDict,
    ) -> None:
        # there is aggregated_column_name data in prefetched_results
        serialized_data = (
            await self.serializer_class.serialize(prefetched_results, validate=False)
            if self.serializer_class is not None
            else prefetched_results
        )

        for item in serialized_data:
            current_to_through_model_keys = item.pop(self._m2m_aggr_field)

            for key in current_to_through_model_keys:
                for obj in objs_by_relation_attr[key]:
                    obj[prefetch_field_root_model_name].append(item)

    async def serialize(
        self,
        objs: Tuple[Dict, ...],
        root_model: Type["DatabaseModel"],
        prefetch_field_root_model_name: str,
        objs_by_relation_attr: DefaultDict,
        additional_prefetch_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        columns_to_select = self.get_columns_to_select()

        await root_model.objects.get_prefetch_related_queryset_m2m_many(
            self.source_field,
            objs_by_relation_attr,
            columns_to_select=columns_to_select,
            aggregated_column_name=self._m2m_aggr_field,
        ).add_callback(
            partial(
                self.serialize_prefetched_data,
                prefetch_field_root_model_name=prefetch_field_root_model_name,
                objs_by_relation_attr=objs_by_relation_attr,
            )
        )


class PrefetchFieldOneToMany(AutoPrefetchField):
    if TYPE_CHECKING:

        def __init__(
            self, source_field: "OneToManyRelationField", serializer_class: Type["BaseModelSerializer"]
        ) -> None:
            super().__init__(source_field, serializer_class)

    def relation_attribute(self, root_model: Type["DatabaseModel"]) -> str:
        _, root_model_column = find_foreign_key_relation(self.source_field.to_manager.table, root_model.objects.table)
        return root_model_column.name

    async def serialize_prefetched_data(
        self,
        prefetched_results: Tuple[Dict, ...],
        prefetch_field_root_model_name: str,
        objs_by_relation_attr: DefaultDict,
    ) -> None:
        # there is aggregated_column_name data in prefetched_results
        serialized_data = (
            await self.serializer_class.serialize(prefetched_results, validate=False)
            if self.serializer_class is not None
            else prefetched_results
        )

        destination_model_col_to_current, _ = find_foreign_key_relation(
            self.source_field.to_manager.table, self.source_field.current_manager.table
        )

        for row in serialized_data:
            key = row[destination_model_col_to_current.name]

            for obj in objs_by_relation_attr[key]:
                obj[prefetch_field_root_model_name].append(row)

    async def serialize(
        self,
        objs: Tuple[Dict, ...],
        root_model: Type["DatabaseModel"],
        prefetch_field_root_model_name: str,
        objs_by_relation_attr: DefaultDict,
        additional_prefetch_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        columns_to_select = self.get_columns_to_select() or ()

        destination_model_col_to_current, _ = find_foreign_key_relation(
            self.source_field.to_manager.table, self.source_field.current_manager.table
        )

        if destination_model_col_to_current not in columns_to_select:
            columns_to_select = columns_to_select + (destination_model_col_to_current,)

        await root_model.objects.get_prefetch_related_queryset_o2m(
            self.source_field,
            objs_by_relation_attr,
            columns_to_select=columns_to_select,
        ).add_callback(
            partial(
                self.serialize_prefetched_data,
                prefetch_field_root_model_name=prefetch_field_root_model_name,
                objs_by_relation_attr=objs_by_relation_attr,
            )
        )


class PrefetchFieldOneToOne(PrefetchFieldOneToMany):
    if TYPE_CHECKING:

        def __init__(
            self, source_field: "OneToOneRelationField", serializer_class: Type["BaseModelSerializer"]
        ) -> None:
            super().__init__(source_field, serializer_class)  # type: ignore

    @property
    def default_value(self) -> Any:
        return None

    async def serialize_prefetched_data(
        self,
        prefetched_results: Tuple[Dict, ...],
        prefetch_field_root_model_name: str,
        objs_by_relation_attr: DefaultDict,
    ) -> None:
        # there is aggregated_column_name data in prefetched_results
        serialized_data = (
            await self.serializer_class.serialize(prefetched_results, validate=False)
            if self.serializer_class is not None
            else prefetched_results
        )

        destination_model_col_to_current, _ = find_foreign_key_relation(
            self.source_field.to_manager.table, self.source_field.current_manager.table
        )

        for row in serialized_data:
            key = row[destination_model_col_to_current.name]

            for obj in objs_by_relation_attr[key]:
                obj[prefetch_field_root_model_name] = row


def PrefetchFieldSerializer(source_field: Any, serializer_class: Type["BaseModelSerializer"]) -> Any:
    from scrudge_orm.fields.fields import RelatedFieldBase

    if isinstance(source_field, RelatedFieldBase):
        field = source_field.serializer(source_field, serializer_class=serializer_class)
    else:
        raise ValueError(f"Unsupported source field '{source_field}'")

    return field
