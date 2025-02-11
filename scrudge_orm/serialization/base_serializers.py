from asyncio import gather, iscoroutinefunction
from collections import defaultdict
from copy import copy
from functools import lru_cache, partial
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Optional,
    Self,
    Set,
    Tuple,
    Type,
)

from pydantic import BaseModel, Field, model_validator
from pydantic._internal._model_construction import ModelMetaclass

from scrudge_orm.backends.patched_transaction import ON_TRANSACTION
from scrudge_orm.fields.fields import DatabaseWithValidationField
from scrudge_orm.models.base import DatabaseModel
from scrudge_orm.query.queryset import QuerySet
from scrudge_orm.serialization.base_serializer_fields import AutoPrefetchField, PrefetchFieldBase
from scrudge_orm.utils.sqalchemy import find_foreign_key_relation
from scrudge_orm.utils.typehint import get_cls_from_annotation


class BaseModelSerializerMeta(ModelMetaclass):
    def __new__(mcs: Type["BaseModelSerializerMeta"], name: str, bases: Tuple[Type, ...], attrs: Dict) -> Type:
        serializer_fields = {}

        new_namespace: Dict = {"__annotations__": attrs.get("__annotations__", {})}
        private_attributes_supported = {"__proxy__", "__m2m_aggr_field__", "__model__"}
        private_attributes = {}

        # support serializer_fields for dynamic created classes by only() and exclude()
        if (ser_fields := attrs.get("__serializer_fields__")) is not None:
            serializer_fields.update(ser_fields)

        if not (mcs.__module__ == attrs.get("__module__") and name == "BaseModelSerializer"):
            for base_cls in reversed(bases):
                if issubclass(base_cls, BaseModelSerializer):
                    if (base_cls_model := getattr(base_cls, "__model__", None)) is not None:
                        attrs["__model__"] = base_cls_model

                    serializer_fields.update(base_cls.__serializer_fields__)

        proxy = attrs.pop("__proxy__", False)

        if not proxy:
            model = attrs.get("__model__")
            assert model is not None, "'__model__' argument must be provided"
            assert issubclass(model, DatabaseModel), f"__model__ must be instance of '{DatabaseModel}'"

        for k, v in attrs.items():
            if isinstance(v, PrefetchFieldBase):
                serializer_fields[k] = v
                new_namespace[k] = Field(v.default_value)
            elif k in private_attributes_supported:
                private_attributes[k] = v

                # drop annotation for private attributes
                new_namespace["__annotations__"].pop(k, None)
            elif k == "__annotations__":
                continue
            else:
                new_namespace[k] = v

        klass = super().__new__(mcs, name, bases, new_namespace)

        type.__setattr__(klass, "__serializer_fields__", serializer_fields)

        for k, v in private_attributes.items():
            type.__setattr__(klass, k, v)

        return klass


class BaseModelSerializer(BaseModel, metaclass=BaseModelSerializerMeta):
    __m2m_aggr_field__: str = "main_aggregated"
    __proxy__: bool = True

    if TYPE_CHECKING:
        __serializer_fields__: Dict[str, PrefetchFieldBase]
        __model__: Type["DatabaseModel"]

        def __init__(self, **kwargs: Any) -> None:
            pass

    @model_validator(mode="before")
    def convert_scrudge_models(cls, values: Any) -> Any:
        if isinstance(values, dict):
            for k, v in values.items():
                if isinstance(v, DatabaseModel):
                    values[k] = v.model_dump()
                elif isinstance(v, (list, tuple)):
                    values[k] = tuple(item.model_dump() if isinstance(item, DatabaseModel) else item for item in v)

        return values

    @classmethod
    @lru_cache
    def parse_fields(cls) -> Tuple[Tuple[str, ...], Tuple[str, ...], Tuple[str, ...], bool, bool]:
        model_fields: Set[str] = set()
        prefetch_models: Set[str] = set()
        join_models: Set[str] = set()
        recursion_join_relations = False
        recursion_prefetch_relations = False

        for field_name, field in cls.model_fields.items():
            if (serializer_field := cls.__serializer_fields__.get(field_name)) is not None:
                if isinstance(serializer_field, AutoPrefetchField):
                    assert (
                        serializer_field.source_field.current_manager.model == cls.__model__
                    ), f"Invalid prefetch field '{field_name}'"

                    if serializer_field.serializer_class is not None:
                        (
                            _,
                            field_join_models,
                            field_prefetch_models,
                            _,
                            _,
                        ) = serializer_field.serializer_class.parse_fields()
                    else:
                        field_prefetch_models, field_join_models = (), ()

                    if field_prefetch_models or field_join_models:
                        recursion_prefetch_relations = True

                prefetch_models.add(field_name)

            elif (related_field := cls.__model__.related_fields.get(field_name)) is not None:
                assert (
                    related_field is not None
                    and isinstance(related_field, DatabaseWithValidationField)
                    and related_field.to_manager is not None
                ), f"There is no related field found with name '{field_name}' for model '{cls.__model__}'"

                serializer_cls: Type[BaseModelSerializer] = get_cls_from_annotation(  # type: ignore
                    field.annotation, BaseModelSerializer
                )

                assert serializer_cls is not None, (
                    f"serializer for field '{field_name}' must be instance of "
                    f"'{BaseModelSerializer.__module__}.{BaseModelSerializer.__name__}"
                )

                _, field_join_models, field_prefetch_models, _, _ = serializer_cls.parse_fields()
                join_models.add(field_name)

                if field_join_models or field_prefetch_models:
                    recursion_join_relations = True

                join_models.add(field_name)
            else:
                model_fields.add(field_name if field.alias is None else field.alias)

        return (
            tuple(model_fields),
            tuple(join_models),
            tuple(prefetch_models),
            recursion_join_relations,
            recursion_prefetch_relations,
        )

    @classmethod
    def _serialize_joined_fields_callback(
        cls, results: Iterable[Dict], model_name: str, foreign_model_to_current_keys: Dict[Any, DefaultDict[Any, Dict]]
    ) -> None:
        related_field = cls.__model__.related_fields[model_name]

        _, to_column = find_foreign_key_relation(
            cls.__model__.objects.table,
            related_field.to_manager.table,  # type: ignore
        )

        for serialized_obj in results:
            for obj in foreign_model_to_current_keys[serialized_obj[to_column.name]]:
                obj[model_name] = serialized_obj

    @classmethod
    async def _serialize_common(
        cls,
        objects: DatabaseModel | Dict | Iterable[DatabaseModel | Dict],
        validate: bool = True,
        joins_already_serialized: bool = False,
        prefetches_already_serialized: bool = False,
        additional_prefetch_data: Optional[Dict[str, Any]] = None,
    ) -> Any:
        if isinstance(objects, (DatabaseModel, dict)):
            many = False
            objects_as_iter = (objects.model_dump() if isinstance(objects, DatabaseModel) else objects,)
        else:
            many = True
            objects_as_iter = cls.__model__.serialize_bulk(objects)

        if not objects:
            return [] if many else {}

        _, join_models, prefetch_models, _, _ = cls.parse_fields()

        additional_prefetch_data = additional_prefetch_data or {}
        models_to_join = join_models if not joins_already_serialized else ()
        models_to_prefetch = prefetch_models if not prefetches_already_serialized else ()

        # save m2m aggregation field too. Needed for correct working of prefetch field
        current_model = cls.__model__

        async_tasks_to_run: List[Awaitable] = []

        if models_to_join or models_to_prefetch:
            foreign_models_to_current_model: DefaultDict[str, DefaultDict[Any, list]] = defaultdict(
                lambda: defaultdict(list)
            )
            prefetch_models_to_current_model: DefaultDict[str, DefaultDict[Any, list]] = defaultdict(
                lambda: defaultdict(list)
            )

            related_fields_data: Dict[str, Tuple] = {}

            for model_name in chain(models_to_join, models_to_prefetch):
                related_field: Any

                if (related_field := cls.__serializer_fields__.get(model_name)) is not None:
                    current_model_attr_name = related_field.relation_attribute(current_model)
                    collection = prefetch_models_to_current_model
                else:
                    related_field = current_model.related_fields[model_name]
                    current_model_column, _ = find_foreign_key_relation(
                        current_model.objects.table,
                        related_field.to_manager.table,
                    )
                    current_model_attr_name = current_model_column.name
                    collection = foreign_models_to_current_model

                # set null for foreign keys as default
                default_value = related_field.default_value if isinstance(related_field, PrefetchFieldBase) else None
                skip_prefetch = additional_prefetch_data.get(f"{model_name}__skip_prefetch", False)

                related_fields_data[model_name] = (current_model_attr_name, default_value, collection, skip_prefetch)

            for obj in objects_as_iter:
                for model_name in chain(models_to_join, models_to_prefetch):
                    # already serialized
                    if model_name in obj:
                        continue

                    current_model_attr_name, default_value, collection, skip_prefetch = related_fields_data[model_name]

                    if skip_prefetch:
                        # in this case we can use same value for all objects without copy
                        obj[model_name] = default_value
                        continue
                    else:
                        # set default value for object. Need to copy default value for field to each object
                        # to avoid bug with refs objects (dict or list for example)
                        obj[model_name] = copy(default_value)

                    if current_model_attr_name not in obj:
                        raise ValueError(
                            f"Can't join related model '{model_name}', "
                            f"due to '{current_model_attr_name}' not in serialized objects"
                        )
                    elif not obj[current_model_attr_name]:
                        # can't prefetch empty values
                        continue
                    else:
                        collection[model_name][obj[current_model_attr_name]].append(obj)

            for model_name, foreign_model_to_current_values in foreign_models_to_current_model.items():
                _, _, _, skip_prefetch = related_fields_data[model_name]

                if skip_prefetch or not foreign_model_to_current_values:
                    continue

                serializer: Type[BaseModelSerializer] = get_cls_from_annotation(  # type: ignore
                    cls.model_fields[model_name].annotation, BaseModelSerializer
                )
                related_field = current_model.related_fields[model_name]

                _, to_model_column = find_foreign_key_relation(
                    current_model.objects.table, related_field.to_manager.table
                )
                foreign_model_qs = serializer.__model__.objects.filter(
                    **{f"{to_model_column.name}__in": list(foreign_model_to_current_values.keys())}
                )

                async_tasks_to_run.append(
                    serializer.serialize(
                        foreign_model_qs,
                        callbacks=[
                            partial(
                                cls._serialize_joined_fields_callback,
                                model_name=model_name,
                                foreign_model_to_current_keys=foreign_model_to_current_values,  # type: ignore
                            )
                        ],
                        validate=False,
                        additional_prefetch_data=additional_prefetch_data,
                    )
                )

            for model_name, objs_by_relation_attr in prefetch_models_to_current_model.items():
                _, _, _, skip_prefetch = related_fields_data[model_name]

                if skip_prefetch or not objs_by_relation_attr:
                    continue

                prefetch_field = cls.__serializer_fields__[model_name]
                async_tasks_to_run.append(
                    prefetch_field.serialize(
                        objects_as_iter,
                        current_model,
                        model_name,
                        objs_by_relation_attr,
                        additional_prefetch_data=additional_prefetch_data,
                    )
                )

        is_any_backend_on_transaction = any(ON_TRANSACTION.get().values())

        # can't gather if any of backends pool in transaction state
        if is_any_backend_on_transaction:
            for coro in async_tasks_to_run:
                await coro
        elif async_tasks_to_run:
            await gather(*async_tasks_to_run)

        if validate:
            objects_as_iter = tuple(cls(**item) for item in objects_as_iter)  # type: ignore

        return objects_as_iter if many else (objects_as_iter[0] if objects_as_iter else {})

    @classmethod
    async def _serialize_queryset(
        cls, queryset: "QuerySet", validate: bool = True, additional_prefetch_data: Optional[Dict[str, Any]] = None
    ) -> Any:
        (
            fields_to_select_tuple,
            join_models,
            prefetch_models,
            recursion_join_relations,
            recursion_prefetch_relations,
        ) = cls.parse_fields()
        fields_to_select = set(fields_to_select_tuple)

        if join_models and not recursion_join_relations:
            # in this case we can select related model in the same query
            queryset = queryset.select_related(*join_models)

            for model_name in join_models:
                serializer: Type[BaseModelSerializer] = cls.model_fields[model_name].annotation  # type: ignore
                assert issubclass(serializer, BaseModelSerializer)
                column_names_to_select, _, _, _, _ = serializer.parse_fields()

                for field in column_names_to_select:
                    fields_to_select.add(f"{model_name}__{field}")
        elif join_models:
            # need to add foreign key column from current model to select
            for model_name in join_models:
                related_field = cls.__model__.related_fields[model_name]
                (
                    current_model_column,
                    _,
                ) = find_foreign_key_relation(
                    cls.__model__.objects.table,
                    related_field.to_manager.table,  # type: ignore
                )
                fields_to_select.add(current_model_column.name)

        manual_prefetches = False

        for model_name in prefetch_models:
            prefetch_field = cls.__serializer_fields__[model_name]

            if isinstance(prefetch_field, AutoPrefetchField):
                _, current_model_column = find_foreign_key_relation(
                    prefetch_field.source_field.through_manager.table, cls.__model__.objects.table
                )
                fields_to_select.add(current_model_column.name)

                if not recursion_prefetch_relations:
                    prefetch_col_names: Optional[Tuple[str, ...]] = None

                    if prefetch_field.serializer_class is not None:
                        prefetch_col_names, _, _, _, _ = prefetch_field.serializer_class.parse_fields()

                    queryset = queryset.prefetch_related(
                        prefetch_field.source_field.related_name, prefetch_col_names=prefetch_col_names
                    )
            elif isinstance(prefetch_field, PrefetchFieldBase):
                # it's means manual prefetch
                fields_to_select.add(prefetch_field.relation_attribute(cls.__model__))
                manual_prefetches = True

        queryset = queryset.values_list(*fields_to_select)
        results = await queryset

        many = not queryset.fetch_one

        if not results:
            return [] if many else {}

        prefetches_already_serialized = not recursion_prefetch_relations and not manual_prefetches

        if not recursion_join_relations and prefetches_already_serialized:
            if validate:
                results = tuple(cls(**item) for item in results)

            return results if many else (results[0] if results else {})
        else:
            return await cls._serialize_common(
                results,
                joins_already_serialized=not recursion_join_relations,
                prefetches_already_serialized=prefetches_already_serialized,
                validate=validate,
                additional_prefetch_data=additional_prefetch_data,
            )

    @classmethod
    async def serialize(
        cls,
        *qs_or_objects: QuerySet | DatabaseModel | Dict | Iterable[DatabaseModel | Dict],
        callbacks: Optional[Iterable[Callable]] = None,
        validate: bool = True,
        additional_prefetch_data: Optional[Dict[str, Any]] = None,
        **field_values: Any,
    ) -> Any:
        """
        Allows to serialize queryset
        :param qs_or_objects:
        :param callbacks: You can set callbacks after serialization
        :param validate: whether serialized data should be validated
        :param additional_prefetch_data: Allows you to provide any data to your custom serialization process.
        There are some special parameters for serialization.
        {prefetch_field_name}__skip_prefetch - allows to skip serialization for PrefetchField
        and use default value instead
        :param field_values: support of serialization of single dictionary
        :return: serialized data
        """
        if qs_or_objects:
            assert len(qs_or_objects) == 1, "You should specify only one positional argument to serialization"
            assert len(field_values) == 0, "There is no field_values expected"
            src_objects = qs_or_objects[0]
        else:
            src_objects = field_values

        results = (
            await cls._serialize_queryset(
                src_objects, validate=validate, additional_prefetch_data=additional_prefetch_data
            )
            if isinstance(src_objects, QuerySet)
            else await cls._serialize_common(
                src_objects, validate=validate, additional_prefetch_data=additional_prefetch_data
            )
        )

        if callbacks is not None:
            for callback_func in callbacks:
                if iscoroutinefunction(callback_func):
                    await callback_func(results)
                else:
                    callback_func(results)

        return results

    @classmethod
    def exclude(cls, *exclude_fields: str) -> Type[Self]:
        assert exclude_fields, "Nothing to exclude"
        all_fields_annotations = {field_name: field.annotation for field_name, field in cls.model_fields.items()}

        fields_to_save = set(all_fields_annotations) - set(exclude_fields)

        return cls.only(*fields_to_save)

    @classmethod
    def only(cls, *fields_to_save: str) -> Type[Self]:
        assert fields_to_save, "Nothing to save"

        attrs = {
            "__model__": cls.__model__,
            "__annotations__": {},
            "__serializer_fields__": {},
        }

        for field_name in fields_to_save:
            field = cls.model_fields[field_name]

            attrs["__annotations__"][field_name] = field.annotation  # type: ignore
            attrs[field_name] = field

            if field_name in cls.__serializer_fields__:
                attrs["__serializer_fields__"][field_name] = cls.__serializer_fields__[field_name]  # type: ignore

        return type(f"{cls.__name__}_{hash(fields_to_save)}", (BaseModelSerializer,), attrs)
