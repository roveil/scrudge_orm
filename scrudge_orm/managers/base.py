from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Literal, Optional, Sequence, Tuple, Type, Union

from pydantic import BaseModel
from sqlalchemy import (  # type: ignore
    Column,
    cast,
    insert,
    select,
    values,
)
from sqlalchemy import update as sa_update

from scrudge_orm.crypto.aes256.encrypted_field import AES256EncryptedField
from scrudge_orm.managers.set_functions import BaseSetFunction
from scrudge_orm.query.conditions import AndCondition, F, OrCondition
from scrudge_orm.query.functions import BaseFunction
from scrudge_orm.query.queryset import QuerySet, RawQuerySet
from scrudge_orm.utils.sqalchemy import find_foreign_key_relation

if TYPE_CHECKING:
    from sqlalchemy import Table
    from sqlalchemy.sql import ClauseElement, ColumnElement

    from scrudge_orm.backends.base import DatabaseBackend
    from scrudge_orm.fields.fields import ManyToManyRelationField, OneToManyRelationField
    from scrudge_orm.models.base import DatabaseModel, DatabaseModelTypeVar


class DatabaseManager:
    def __init__(self, table: "Table", model: Type["DatabaseModel"], pool: "DatabaseBackend") -> None:
        self.table = table
        self.model = model
        self.pool = pool

    @cached_property
    def get_table_columns_with_defaults(self) -> Tuple["Column", ...]:
        return tuple(
            col for col in self.table.c if col.onupdate is not None or col.server_default is not None or col.primary_key
        )

    @cached_property
    def get_table_columns_with_server_defaults(self) -> Tuple["Column", ...]:
        return tuple(col for col in self.table.c if col.server_default is not None or col.primary_key)

    @cached_property
    def get_table_onupdate_defaults(self) -> Dict[str, Any]:
        return {col.name: col.onupdate.arg for col in self.table.c if col.onupdate is not None}

    def convert_value_to_raw(self, value: Any) -> Any:
        result: Any = value

        if isinstance(value, AES256EncryptedField):
            result = value.encrypt()
        elif isinstance(value, Enum):
            result = value.value
        elif isinstance(value, BaseModel):
            result = value.model_dump()
        elif isinstance(value, F):
            result = value.get_expression(self.table)
        elif isinstance(value, BaseFunction):
            result = value.get_expression(self.table)

        return result

    def parse_returning_argument(
        self, returning: Optional[Union[Literal["*"] | Iterable[Union["str", "Column"]]]] = None
    ) -> Optional[Tuple["Column", ...]]:
        if returning is None:
            return None

        if returning == "*":
            return tuple(self.table.c)

        return tuple(item if isinstance(item, Column) else self.table.c[item] for item in returning)

    def to_database_data(self, data: Union[Dict, "DatabaseModelTypeVar"]) -> Dict:
        assert isinstance(data, (dict, self.model)), f"'data' should be instance of 'dict' or {self.model}"

        dicted_data: Dict = (
            data.model_dump(exclude=self.model.related_fields.keys())  # type: ignore
            if isinstance(data, self.model)
            else data
        )

        for k, v in dicted_data.items():
            try:
                assert k in self.table.c, f"There is no column '{k}' in table: '{self.table.name}'"
            except Exception as e:
                raise e

            dicted_data[k] = self.convert_value_to_raw(v)

        return dicted_data

    def get_prepared_to_insert_data(self, data: Union[Dict, "DatabaseModelTypeVar"]) -> Dict:
        dicted_data = self.to_database_data(data)

        for col in self.get_table_columns_with_server_defaults:
            if (col.primary_key and dicted_data.get(col.name) is None) or (
                col.server_default is not None and dicted_data.get(col.name) is None
            ):
                dicted_data.pop(col.name, None)

        return dicted_data

    async def fetch_one(
        self,
        query: Union["ClauseElement", str],
        values: Optional[Dict] = None,
    ) -> Optional["DatabaseModelTypeVar"]:
        query_result = await self.pool.fetch_one(query, values=values)

        return self.model(**query_result) if query_result is not None else None  # type: ignore

    async def fetch_all(
        self,
        query: Union["ClauseElement", str],
        values: Optional[Dict] = None,
    ) -> Tuple["DatabaseModelTypeVar", ...]:
        query_results = await self.pool.fetch_all(query, values=values)

        return tuple(self.model(**row) for row in query_results)  # type: ignore

    async def create(self, **kwargs: Any) -> "DatabaseModelTypeVar":
        query = (
            insert(self.table).values(**self.get_prepared_to_insert_data(self.model(**kwargs))).returning(self.table)
        )

        # will return always DatabaseModel instance or raise different exceptions
        return await self.fetch_one(query)  # type: ignore

    async def bulk_insert(
        self,
        data: Iterable["DatabaseModelTypeVar"],
        batch_size: int = 1000,
        refresh_auto_fields: bool = True,
    ) -> List["DatabaseModelTypeVar"]:
        """
        Bulk insert model objects to database
        :param data: objects to insert
        :param batch_size: an iteration batch size
        :param skip_column_defaults: should insert_objects be updated by column defaults onupdate
        and server_default expressions
        :param refresh_auto_fields: should insert_objects columns with auto defaults be refreshed from database
        :return: inserted objects
        """
        if not data:
            return []

        data_as_list = list(data)

        inserted_rows = await self.bulk_insert_raw(
            data_as_list,
            batch_size=batch_size,
            returning_columns=self.get_table_columns_with_defaults if refresh_auto_fields else None,
        )

        if refresh_auto_fields:
            for index, row in enumerate(inserted_rows):
                for k, v in dict(row).items():
                    setattr(data_as_list[index], k, v)

        return data_as_list

    async def bulk_insert_raw(
        self,
        insert_objects: Sequence[Union["DatabaseModelTypeVar", Dict]],
        batch_size: int = 1000,
        returning_columns: Optional[Union[Literal["*"] | Iterable[Union["str", "Column"]]]] = None,
    ) -> Any:
        """
        Bulk insert dictionaries with models data or model instances to database.
        This method will be helpful for partial returning inserted data from database.
        :param insert_objects: list with objects data to insert
        :param batch_size: an iteration batch size
        :param skip_column_defaults: should columns default values be updated
        by column defaults onupdate and server_default expressions
        :param returning_columns: list of columns to return
        :return: Any, depends on returning
        """
        returning_cols = self.parse_returning_argument(returning_columns)

        inserted_objects = []

        async with await self.pool.transaction():
            # can't gather inside transaction
            for i in range(0, len(insert_objects), batch_size):
                batch_objects = insert_objects[i : i + batch_size]
                insert_data = [self.get_prepared_to_insert_data(obj) for obj in batch_objects]
                query = insert(self.table).values(insert_data)

                if returning_cols:
                    query = query.returning(*returning_cols)

                inserted_objects.extend(await self.pool.fetch_all(query))

        return inserted_objects

    async def bulk_update(
        self,
        update_data: Sequence[Dict],
        batch_size: int = 1000,
        key_fields: str | Iterable[str] = "id",
        returning_columns: Optional[Literal["*"] | Iterable[Union["str", "Column"]]] = None,
        set_functions: Optional[Dict[str, str]] = None,
    ) -> List:
        if not update_data:
            return []

        key_fields = {key_fields} if isinstance(key_fields, str) else set(key_fields)
        update_col_names = set(update_data[0].keys())
        returning_cols = self.parse_returning_argument(returning_columns)
        set_functions = set_functions or {}

        updated_objects = []

        async with await self.pool.transaction():
            # can't gather inside transaction
            for i in range(0, len(update_data), batch_size):
                batched_data: Sequence[Dict] = update_data[i : i + batch_size]

                values_data = [
                    tuple(
                        cast(self.convert_value_to_raw(row_update[col_name]), self.table.c[col_name].type)
                        for col_name in update_col_names
                    )
                    if index == 0
                    else tuple(self.convert_value_to_raw(row_update[col_name]) for col_name in update_col_names)
                    for index, row_update in enumerate(batched_data)
                ]
                with_values = select(
                    values(
                        *(Column(col_name, self.table.c[col_name].type) for col_name in update_col_names),
                        name="values_data",
                    ).data(values_data)
                ).cte("values_data_table")

                query = (
                    sa_update(self.table)
                    .where(*(self.table.c[col_name] == with_values.c[col_name] for col_name in key_fields))
                    .values(
                        **{
                            col_name: BaseSetFunction.get_instance_by_name(set_func_name).get_expression(
                                self.table.c[col_name], with_values.c[col_name]
                            )
                            if (set_func_name := set_functions.get(col_name))
                            else with_values.c[col_name]
                            for col_name in update_col_names
                            if col_name not in key_fields
                        }
                    )
                )

                if returning_cols:
                    query = query.returning(*returning_cols)

                updated_objects.extend(await self.pool.fetch_all(query))

        return updated_objects

    async def bulk_update_or_create(
        self,
        data: Sequence["DatabaseModelTypeVar" | Dict],
        update: bool = False,
        batch_size: int = 1000,
        unique_constraint_fields: Optional[str | Tuple[str, ...]] = None,
        returning_columns: Optional[Union[Literal["*"] | Iterable[Union["str", "Column"]]]] = None,
    ) -> Any:
        raise NotImplementedError()

    async def get_or_create(
        self, unique_constraint_fields: Optional[Tuple[str, ...]] = None, **kwargs: Any
    ) -> Tuple["DatabaseModelTypeVar", bool]:
        raise NotImplementedError()

    async def create_or_nothing(
        self, unique_constraint_fields: Optional[Tuple[str, ...]] = None, **kwargs: Any
    ) -> bool:
        raise NotImplementedError()

    async def update_or_create(
        self,
        unique_constraint_fields: Optional[Tuple[str, ...]] = None,
        returning_columns: Optional[Union[Literal["*"] | Iterable[Union["str", "Column"]]]] = None,
        convert_to_model: bool = True,
        **field_values: Any,
    ) -> Any:
        raise NotImplementedError()

    def filter(self, *conditions: Union[AndCondition, OrCondition], **field_values: Any) -> QuerySet:
        return QuerySet(*conditions, manager=self, **field_values)

    def exclude(self, *conditions: Union[AndCondition, OrCondition], **field_values: Any) -> QuerySet:
        return QuerySet(*conditions, manager=self, negative=True, **field_values)

    def get(self, *conditions: Union[AndCondition, OrCondition], **field_values: Any) -> QuerySet:
        return QuerySet(*conditions, manager=self, **field_values).fetch_single()

    def get_prefetch_related_queryset_m2m_single(
        self,
        related_field: "ManyToManyRelationField",
        primary_key: Any,
    ) -> QuerySet:
        through_manager = related_field.through_manager

        through_model_col_to_current, current_model_column = find_foreign_key_relation(
            through_manager.table, self.table
        )
        through_model_col_to_destination, destination_model_column = find_foreign_key_relation(
            through_manager.table,
            related_field.to_manager.table,
        )

        return related_field.to_manager.filter(
            **{
                f"{destination_model_column.name}__in": through_manager.filter(
                    **{through_model_col_to_current.name: primary_key}
                ).values_list(through_model_col_to_destination.name)
            }
        )

    def get_prefetch_related_queryset_o2m(
        self,
        related_field: "OneToManyRelationField",
        current_model_keys: Iterable[Any],
        columns_to_select: Optional[Iterable["ColumnElement"]] = None,
    ) -> QuerySet:
        destination_model_col_to_current, current_model_column = find_foreign_key_relation(
            related_field.to_manager.table, self.table
        )
        qs_params = {f"{destination_model_col_to_current.name}__in": current_model_keys}
        queryset = related_field.to_manager.filter(**qs_params)

        if columns_to_select is not None:
            cols_to_select_str = (col.name for col in columns_to_select)  # type: ignore
            queryset = queryset.values_list(*cols_to_select_str)

        return queryset

    def get_prefetch_related_queryset_m2m_many(
        self,
        related_field: "ManyToManyRelationField",
        current_to_through_model_keys: Iterable[Any],
        columns_to_select: Optional[Iterable["ColumnElement"]] = None,
        aggregated_column_name: str = "main_aggregated",
    ) -> RawQuerySet:
        raise NotImplementedError()
