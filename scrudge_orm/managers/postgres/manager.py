from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Literal, Optional, Sequence, Tuple, Union

from sqlalchemy import Column, func, select, text
from sqlalchemy.dialects.postgresql import insert

from scrudge_orm.managers.base import DatabaseManager
from scrudge_orm.managers.set_functions import BaseSetFunction
from scrudge_orm.query.queryset import RawQuerySet
from scrudge_orm.utils.sqalchemy import find_foreign_key_relation, get_table_unique_constraints_fields

if TYPE_CHECKING:
    from sqlalchemy import Table
    from sqlalchemy.dialects.postgresql import Insert
    from sqlalchemy.sql import ColumnElement

    from scrudge_orm.fields.fields import ManyToManyRelationField
    from scrudge_orm.models.base import DatabaseModelTypeVar


class PostgresManager(DatabaseManager):
    def get_table_unique_constraint(self) -> Tuple[Tuple[str, ...], Optional[str]]:
        unique_index_where = None

        all_unique_constraints_fields_with_where_part = get_table_unique_constraints_fields(self.table)

        if not all_unique_constraints_fields_with_where_part:
            # If there is no unique constraints, we can use primary key column
            unique_constraint_fields = (self.model.get_pk_column_name(),)

        elif len(all_unique_constraints_fields_with_where_part) > 1:
            raise ValueError(
                f"Can't choose on conflict constraint, you should choose one from: "
                f"{','.join(map(str, all_unique_constraints_fields_with_where_part.keys()))}"
            )
        else:
            unique_constraints_as_list = list(all_unique_constraints_fields_with_where_part.items())
            unique_constraint_fields, unique_index_where = unique_constraints_as_list[0]  # type: ignore

        return unique_constraint_fields, unique_index_where

    async def __get_or_create(
        self,
        unique_constraint_fields: Optional[str | Tuple[str, ...]] = None,
        get_if_not_created: bool = True,
        **kwargs: Any,
    ) -> Tuple[Optional["DatabaseModelTypeVar"], bool]:
        insert_values = self.get_prepared_to_insert_data(self.model(**kwargs))
        unique_index_where = None

        query = insert(self.table).values(**insert_values).returning(self.table)

        if unique_constraint_fields is None:
            unique_constraint_fields, unique_index_where = self.get_table_unique_constraint()

        index_where = (
            text(unique_index_where)
            if unique_index_where is not None and isinstance(unique_index_where, str)
            else unique_index_where
        )

        query = query.on_conflict_do_nothing(
            index_elements=unique_constraint_fields,
            index_where=index_where,
        )

        result = await self.fetch_one(query)  # type: ignore
        created = result is not None

        # will return None if record already exists
        if result is None and get_if_not_created:
            result = await self.get(**{k: insert_values[k] for k in unique_constraint_fields})

        return result, created

    async def bulk_update_or_create(
        self,
        data: Sequence["DatabaseModelTypeVar" | Dict],
        update: bool = False,
        batch_size: int = 1000,
        unique_constraint_fields: Optional[str | Tuple[str, ...]] = None,
        returning_columns: Optional[Union[Literal["*"] | Iterable[Union["str", "Column"]]]] = None,
        set_functions: Optional[Dict[str, str]] = None,
    ) -> Any:
        returning_cols = self.parse_returning_argument(returning_columns)
        unique_index_where = None
        set_functions = set_functions or {}

        if unique_constraint_fields is None:
            unique_constraint_fields, unique_index_where = self.get_table_unique_constraint()

        index_where = (
            text(unique_index_where)
            if unique_index_where is not None and isinstance(unique_index_where, str)
            else unique_index_where
        )

        updated_or_created_objects = []

        async with await self.pool.transaction():
            # can't gather inside transaction
            for i in range(0, len(data), batch_size):
                batch_objects = data[i : i + batch_size]
                query = insert(self.table)

                if update:
                    update_or_create_data = [self.to_database_data(obj) for obj in batch_objects]

                    on_conflict_data = self.get_on_conflict_update_data(query, set_functions, unique_constraint_fields)
                    query = query.on_conflict_do_update(
                        index_elements=unique_constraint_fields,
                        index_where=index_where,
                        set_=on_conflict_data,
                    )
                else:
                    update_or_create_data = [self.get_prepared_to_insert_data(obj) for obj in batch_objects]

                    query = query.on_conflict_do_nothing(
                        index_elements=unique_constraint_fields,
                        index_where=index_where,
                    )

                query = query.values(update_or_create_data)

                if returning_cols:
                    query = query.returning(*returning_cols)

                updated_or_created_objects.extend(await self.pool.fetch_all(query))

        return updated_or_created_objects

    async def get_or_create(
        self, unique_constraint_fields: Optional[Tuple[str, ...]] = None, **kwargs: Any
    ) -> Tuple["DatabaseModelTypeVar", bool]:
        return await self.__get_or_create(unique_constraint_fields=unique_constraint_fields, **kwargs)  # type: ignore

    async def create_or_nothing(
        self, unique_constraint_fields: Optional[Tuple[str, ...]] = None, **kwargs: Any
    ) -> bool:
        _, created = await self.__get_or_create(
            unique_constraint_fields=unique_constraint_fields, get_if_not_created=False, **kwargs
        )

        return created

    def get_on_conflict_update_data(
        self, query: "Insert", set_functions: Optional[Dict[str, str]], unique_constraint_fields: str | Tuple[str, ...]
    ) -> Dict:
        set_functions = set_functions or {}
        unique_constraint_fields = (
            (unique_constraint_fields,) if isinstance(unique_constraint_fields, str) else unique_constraint_fields
        )

        on_conflict_data = {
            col.name: BaseSetFunction.get_instance_by_name(set_func_name).get_expression(
                col,
                query.excluded[col.name],  # type: ignore
            )
            if (set_func_name := set_functions.get(col.name))
            else query.excluded[col.name]  # type: ignore
            for col in self.table.c
            if col.name not in unique_constraint_fields and not col.primary_key
        }
        on_conflict_data.update(self.get_table_onupdate_defaults)

        return on_conflict_data

    async def update_or_create(
        self,
        unique_constraint_fields: Optional[Tuple[str, ...]] = None,
        returning_columns: Optional[Union[Literal["*"] | Iterable[Union["str", "Column"]]]] = None,
        convert_to_model: bool = True,
        set_functions: Optional[Dict[str, str]] = None,
        **field_values: Any,
    ) -> Any:
        """
        Update or create row in database
        :param unique_constraint_fields: Tuple of unique constraint field columns.
        If no provided will try to search it
        :param returning_columns: if "*" will return model instance
        :param field_values: field values to update or create
        :param convert_to_model: Need to convert result to model instance.
        If true, returning_columns parameter will be ignored
        :param set_functions: Set functions allow to define update expression if row already exists
        :return: Any. It can be model instance or None depends on the result
        """
        if convert_to_model:
            returning_columns = "*"

        returning_cols = self.parse_returning_argument(returning_columns)
        unique_index_where = None

        if unique_constraint_fields is None:
            unique_constraint_fields, unique_index_where = self.get_table_unique_constraint()

        index_where = (
            text(unique_index_where)
            if unique_index_where is not None and isinstance(unique_index_where, str)
            else unique_index_where
        )

        query = insert(self.table).values(self.to_database_data(field_values))
        on_conflict_data = self.get_on_conflict_update_data(query, set_functions, unique_constraint_fields)

        query = query.on_conflict_do_update(
            index_elements=unique_constraint_fields,
            index_where=index_where,
            set_=on_conflict_data,
        )

        if returning_cols:
            query = query.returning(*returning_cols)

        if convert_to_model:
            result: Any = await self.fetch_one(query)
        else:
            raw_result = await self.pool.fetch_one(query)
            result = dict(raw_result) if raw_result is not None else None

        return result

    def get_prefetch_related_queryset_m2m_many(
        self,
        related_field: "ManyToManyRelationField",
        current_to_through_model_keys: Iterable[Any],
        columns_to_select: Optional[Iterable["ColumnElement"]] = None,
        aggregated_column_name: str = "main_aggregated",
    ) -> RawQuerySet:
        through_manager = related_field.through_manager
        through_table = through_manager.table
        destination_table = related_field.to_manager.table

        through_model_col_to_current, current_model_column = find_foreign_key_relation(through_table, self.table)
        through_model_col_to_destination, destination_model_column = find_foreign_key_relation(
            through_table,
            destination_table,
        )

        with_m2m_table = (
            select(
                [
                    through_model_col_to_destination,
                    func.array_agg(through_model_col_to_current).label(aggregated_column_name),
                ]
            )
            .where(through_model_col_to_current.in_(list(current_to_through_model_keys)))
            .group_by(through_model_col_to_destination)
            .cte("m2m_tmp")
        )

        sub_m2m_qs = select(getattr(with_m2m_table.c, through_model_col_to_destination.name))

        columns_to_select_list: List[Union["ColumnElement", "Table"]] = (
            list(columns_to_select) if columns_to_select is not None else [destination_table]
        )
        columns_to_select_list.append(getattr(with_m2m_table.c, aggregated_column_name))

        main_query = (
            select(columns_to_select_list).join(with_m2m_table).where(destination_model_column.in_(sub_m2m_qs))  # type: ignore
        )

        return RawQuerySet(main_query, self.pool)
