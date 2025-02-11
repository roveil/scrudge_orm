import operator
from asyncio import gather, iscoroutinefunction
from collections import defaultdict
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Self,
    Set,
    Tuple,
    Type,
    Union,
)

from sqlalchemy import and_, delete, desc, func, not_, nullslast, or_, select, true, update  # type: ignore
from sqlalchemy.sql import Delete, Select, Update
from sqlalchemy.sql.elements import Label

from scrudge_orm.query.aggregations import Aggregation
from scrudge_orm.query.conditions import AndCondition, BaseCondition, F, OrCondition, SupportedOperator

if TYPE_CHECKING:
    from sqlalchemy import Table
    from sqlalchemy.sql.elements import BooleanClauseList, ColumnElement

    from scrudge_orm.backends.base import DatabaseBackend
    from scrudge_orm.managers.base import DatabaseManager
    from scrudge_orm.models.base import DatabaseModel


class CallBackMixin:
    def __init__(self) -> None:
        super().__init__()
        self.callbacks: List[Callable] = []

    @property
    def is_callbacks_set(self) -> bool:
        return bool(self.callbacks)

    async def execute_callbacks(self, result: Any) -> None:
        # don't gather because big amount of callbacks don't expected there
        for callback_func in self.callbacks:
            if iscoroutinefunction(callback_func):
                await callback_func(result)
            else:
                callback_func(result)

    def add_callback(self, *callbacks: Callable) -> Self:
        """
        Adds a callback after the result is known
        :param callbacks: any sync function that get result as first argument
        :return: Self
        """
        for callback_func in callbacks:
            self.callbacks.append(callback_func)

        return self


class QuerySet(CallBackMixin):
    operator_comparison = {
        SupportedOperator.eq: operator.eq,
        SupportedOperator.neq: operator.ne,
        SupportedOperator.gt: operator.gt,
        SupportedOperator.ge: operator.ge,
        SupportedOperator.lt: operator.lt,
        SupportedOperator.le: operator.le,
    }

    functions_comparison = {
        # SupportedOperator.between: 'between',
        SupportedOperator.in_: "in_",
        SupportedOperator.not_in: "not_in",
        SupportedOperator.is_: "is_",
        SupportedOperator.is_not: "is_not",
        SupportedOperator.like: "like",
        SupportedOperator.ilike: "ilike",
        SupportedOperator.not_like: "notlike",
        SupportedOperator.not_ilike: "notilike",
        SupportedOperator.startswith: "startswith",
        SupportedOperator.endswith: "endswith",
        SupportedOperator.contains: "contains",
        SupportedOperator.concat: "concat",
    }

    def __init__(
        self,
        *conditions: AndCondition | OrCondition,
        manager: "DatabaseManager",
        negative: bool = False,
        **field_values: Any,
    ):
        super().__init__()
        self.where_conditions: List["BooleanClauseList"] = []
        self.annotations: Dict[str, "ColumnElement"] = {}
        self.manager = manager
        self.return_raw = False
        self.flat = False
        self.fetch_one = False
        self.dict_key_fields: Optional[str | Tuple[str, ...]] = None
        self.query: Select | Update | Delete = select([self.manager.table])
        self.current_columns_to_select: List[Union["Table", "ColumnElement"]] = [*self.manager.table.columns]
        self.callbacks: List[Callable] = []
        self.prefetch_field_columns: DefaultDict[str, Tuple["ColumnElement", ...]] = defaultdict(tuple)
        self.joined_models: Set[str] = set()

        self._update_query_condition(self.prepare_queryset_parameters(*conditions, **field_values), negative=negative)

    def _update_query_condition(self, filter_condition: AndCondition | OrCondition, negative: bool = False) -> None:
        if negative:
            self.where_conditions.append(not_(self.parse_expression(filter_condition)))  # type: ignore
        else:
            self.where_conditions.append(self.parse_expression(filter_condition))

    @staticmethod
    def parse_field_name_and_operator(field_name_expression: str) -> Tuple[str, SupportedOperator]:
        splitted_field_expr = field_name_expression.split("__")
        field_name = splitted_field_expr[0]
        field_operator = (
            SupportedOperator.get_operator(splitted_field_expr[1])
            if len(splitted_field_expr) > 1
            else SupportedOperator.eq
        )
        return field_name, field_operator

    def parse_expression(self, condition: BaseCondition) -> "BooleanClauseList":
        if isinstance(condition, AndCondition):
            sqlalchemy_func = and_
        elif isinstance(condition, OrCondition):
            sqlalchemy_func = or_
        else:
            raise ValueError("Unsupported condition")

        args = []

        for filter_condition in condition.conditions:
            args.append(self.parse_expression(filter_condition))

        for field_expr, field_value in condition.fields_values.items():
            field_name, field_operator = self.parse_field_name_and_operator(field_expr)

            if isinstance(field_value, self.__class__):
                args.append(self.eval_operator(field_operator, field_name, field_value.compile().query))
            elif isinstance(field_value, F):
                column = self._get_column_by_name(field_value.column_name)
                column_expression = column if field_value.value_to_sum is None else column + field_value.value_to_sum
                args.append(self.eval_operator(field_operator, field_name, column_expression))
            else:
                args.append(self.eval_operator(field_operator, field_name, field_value))

        return sqlalchemy_func(*args)

    def _get_column_by_name(self, column_name: str) -> "ColumnElement":
        try:
            column = next(
                (
                    item
                    for item in (self.annotations.get(column_name), getattr(self.manager.table.c, column_name, None))
                    if item is not None
                ),
            )
        except StopIteration:
            column = None

        if column is None:
            raise ValueError(f"Column with name: '{column_name}' not defined")

        return column

    def eval_operator(self, field_operator: SupportedOperator, field_name: str, value: Any) -> Any:
        # can't use or there due to TypeError
        column = self._get_column_by_name(field_name)

        if field_operator in self.operator_comparison:
            return self.operator_comparison[field_operator](column, value)

        return getattr(column, self.functions_comparison[field_operator])(value)

    def compile(self) -> Self:
        if self.where_conditions:
            self.query = self.query.where(*self.where_conditions)

        return self

    async def _process_query_common(self) -> Any:
        if self.joined_models or self.prefetch_field_columns:
            # in this case need to fetch raw results, prefetch related tables and then construct the model
            results = await RawQuerySet(
                self.query,
                self.manager.pool,
                flat=self.flat,
                fetch_one=False,
            )
            await self._process_add_prefetch_tables_data(results)
            final_results = self.manager.model.to_models_bulk(results)

            return (final_results[0] if final_results else None) if self.fetch_one else final_results
        elif self.fetch_one:
            return await self.manager.fetch_one(self.query)
        else:
            return await self.manager.fetch_all(self.query)

    async def _process_add_prefetch_tables_data(self, current_results: Optional[Dict | List[Dict]]) -> None:
        """
        Adds to current results data from prefetched tables
        :param current_results: Data from root query by primary key
        :return None
        """
        if not current_results or (not self.prefetch_field_columns and not self.joined_models):
            return None

        results_as_iter = [current_results] if isinstance(current_results, dict) else current_results
        records_by_related_fields: DefaultDict[str, DefaultDict[Any, list]] = defaultdict(lambda: defaultdict(list))
        tasks_to_async_run = []

        from scrudge_orm.fields.fields import RelatedFieldBase

        current_to_destination_model_keys: DefaultDict[str, list] = defaultdict(list)  # o2m
        joined_model_columns: DefaultDict[str, List[Tuple[str, str]]] = defaultdict(list)
        root_model_col_names: Set[str] = set()

        for column in self.current_columns_to_select:
            if isinstance(column, Label):
                base_column = list(column.base_columns)[0]  # type: ignore
            else:
                base_column = column

            if (column_table := getattr(base_column, "table", None)) is None:
                continue

            if column_table == self.manager.table:
                root_model_col_names.add(column.name)  # type: ignore
            else:
                assert isinstance(column, Label)
                name_splitted = column.name.split(".")
                joined_model_name = ".".join(name_splitted[:-1])
                column_name = name_splitted[-1]

                joined_model_columns[joined_model_name].append((column_name, column.name))

        for item in results_as_iter:
            for prefetch_field_name in self.prefetch_field_columns:
                related_field: RelatedFieldBase = self.manager.model.related_fields[prefetch_field_name]  # type: ignore
                assert isinstance(
                    related_field, RelatedFieldBase
                ), f"'{related_field.related_name}' not supported to prefetch"

                _, current_model_column = related_field.get_current_model_to_relation_columns()

                item[prefetch_field_name] = related_field.get_default_value()

                if item[current_model_column.name] is not None:
                    current_to_destination_model_keys[prefetch_field_name].append(item[current_model_column.name])
                    records_by_related_fields[prefetch_field_name][item[current_model_column.name]].append(item)

            # infinite nested default dict
            related_tables_attributes: DefaultDict[str, DefaultDict] = defaultdict(
                lambda: defaultdict(related_tables_attributes.default_factory)  # noqa: B023
            )

            for joined_model_name in self.joined_models:
                for column_name, column_label in joined_model_columns[joined_model_name]:
                    nested_obj_to_set = related_tables_attributes

                    for nested_model_name in joined_model_name.split("."):
                        nested_obj_to_set = nested_obj_to_set[nested_model_name]

                    nested_obj_to_set[column_name] = item.pop(column_label)

            item.update(related_tables_attributes)

        for prefetch_field_name, columns_to_select in self.prefetch_field_columns.items():
            related_field = self.manager.model.related_fields[prefetch_field_name]  # type: ignore

            tasks_to_async_run.append(
                related_field.serialize(
                    columns_to_select,  # type: ignore
                    current_to_destination_model_keys[prefetch_field_name],  # type: ignore
                    self.manager.model,
                    prefetch_field_name,
                    records_by_related_fields[prefetch_field_name],
                )
            )

        # can't gather queries inside transaction. runtime will be broken
        if tasks_to_async_run:
            if self.manager.pool.is_on_transaction():
                for coro in tasks_to_async_run:
                    await coro
            else:
                await gather(*tasks_to_async_run)

    async def process_query(self) -> Any:
        self.compile()

        if self.return_raw:
            result = await RawQuerySet(
                self.query,
                self.manager.pool,
                flat=self.flat,
                fetch_one=self.fetch_one,
                dict_key_fields=self.dict_key_fields,
            )
            await self._process_add_prefetch_tables_data(result)
        else:
            result = await self._process_query_common()

        if self.is_callbacks_set:
            await self.execute_callbacks(result)

        return result

    def __await__(self) -> Any:
        return self.process_query().__await__()

    def values_list(self, *field_names: str, flat: bool = False) -> Self:
        """
        :param field_names: field names to return in list
        :param flat: if single column provided, unpacked list will be returned without info
        about column name instead of dict
        :return: None
        """
        columns = [self._get_column_by_name(field) for field in field_names]

        if flat:
            if self.dict_key_fields is None:
                assert len(columns) == 1, "flat=True allowed to single column select only"
            elif isinstance(self.dict_key_fields, str):  # one for key field and one for selected field
                assert len(columns) == 2, "flat=True allowed to single column select only"
            else:  # tuple
                assert len(columns) - len(self.dict_key_fields) == 1, "flat=True allowed to single column select only"

        self.return_raw = True
        self.flat = flat

        if isinstance(self.query, Select):
            self.query = self.query.with_only_columns(*columns)  # type: ignore
            self.current_columns_to_select = columns  # type: ignore

        return self

    def values_dict(self, *field_names: str, key_fields: str | Tuple[str, ...] = "id", flat: bool = False) -> Self:
        """
        :param field_names: field names to return in list
        :param key_fields: key columns to construct keys for result dictionary
        :param flat: if single column provided, unpacked list will be returned without info
        about column name instead of dict
        :return: None
        """
        key_fields_as_tuple = (key_fields,) if isinstance(key_fields, str) else key_fields
        unique_selected_columns = set(chain(key_fields_as_tuple, field_names))

        self.dict_key_fields = key_fields
        self.values_list(*unique_selected_columns, flat=flat)

        return self

    def order_by(self, *field_names: str, order_desc: bool = False, nulls_last: bool = False) -> Self:
        assert isinstance(self.query, Select), "You can use order by only for select statements"

        order_by_expressions = []

        for field in field_names:
            expression = self._get_column_by_name(field) if not order_desc else desc(self._get_column_by_name(field))

            if nulls_last:
                order_by_expressions.append(nullslast(expression))
            else:
                order_by_expressions.append(expression)

        self.query = self.query.order_by(*order_by_expressions)

        return self

    @staticmethod
    def prepare_queryset_parameters(
        *conditions: AndCondition | OrCondition, **field_values: Any
    ) -> AndCondition | OrCondition:
        if conditions:
            assert len(conditions) == 1, "You should specify only one query expression"
            assert len(field_values) == 0, "There is no field_values expected"
            query_condition = conditions[0]
        else:
            query_condition = AndCondition(**field_values)

        return query_condition

    def filter(self, *conditions: AndCondition | OrCondition, **field_values: Any) -> Self:
        self._update_query_condition(self.prepare_queryset_parameters(*conditions, **field_values))

        return self

    def exclude(self, *conditions: AndCondition | OrCondition, **field_values: Any) -> Self:
        self._update_query_condition(self.prepare_queryset_parameters(*conditions, **field_values), negative=True)

        return self

    def exists(self) -> Self:
        assert isinstance(self.query, Select), "Select query only allowed"

        self.query = self.query.with_only_columns(true)  # type: ignore
        self.current_columns_to_select = [true]  # type: ignore
        self.fetch_single()
        self.return_raw = True
        self.flat = True

        return self

    def count(self) -> Self:
        assert isinstance(self.query, Select), "Select query only allowed"
        cnt_column = func.count().label("cnt")

        self.query = self.query.with_only_columns(cnt_column)
        self.current_columns_to_select = [cnt_column]
        self.fetch_single()
        self.return_raw = True
        self.flat = True

        return self

    def fetch_single(self) -> Self:
        if isinstance(self.query, Select):
            self.query = self.query.limit(1)

        self.fetch_one = True

        return self

    def get(self, *conditions: AndCondition | OrCondition, **field_values: Any) -> Self:
        self._update_query_condition(self.prepare_queryset_parameters(*conditions, **field_values))
        self.fetch_single()

        return self

    def __update_qs_returning(self, returning: Optional[str | Iterable[str] | Literal["*"]] = None) -> None:
        assert not isinstance(self.query, Select), "returning is not supported by Select statement"

        if returning is not None:
            if returning == "*":
                self.query = self.query.returning(self.manager.table)
                self.return_raw = False
            else:
                column_names = [returning] if isinstance(returning, str) else returning
                self.query = self.query.returning(*[self.manager.table.c[col_name] for col_name in column_names])
                self.return_raw = True

    def update(self, returning: Optional[str | Iterable[str] | Literal["*"]] = None, **field_values: Any) -> Self:
        assert field_values, "Need to specify values to update"

        self.query = update(self.manager.table)
        self.query = self.query.values(**{k: self.manager.convert_value_to_raw(v) for k, v in field_values.items()})
        self.__update_qs_returning(returning=returning)

        return self

    def delete(self, returning: Optional[str | Iterable[str] | Literal["*"]] = None) -> Self:
        self.query = delete(self.manager.table)
        self.__update_qs_returning(returning=returning)

        return self

    def annotate(self, *aggregations: Aggregation) -> Self:
        for aggregation_obj in aggregations:
            aggregate_func = getattr(func, aggregation_obj.sql_func)(
                self._get_column_by_name(aggregation_obj.field)
            ).label(aggregation_obj.label)
            self.annotations[aggregation_obj.label] = aggregate_func

        return self

    def aggregate(
        self,
        group_by_fields: str | Iterable[str],
        select_fields: str | Aggregation | Iterable[str | Aggregation],
        flat: bool = False,
        **having: Any,
    ) -> Self:
        """
        Allow to construct aggregation group by queries
        :param group_by_fields: iterable of columns, that used in group by expression
        :param select_fields: fields to select
        :param flat: if single column selected, unpacked list will be returned without info about column name
        :param having: having expressions. Field names and operators expected
        :return: Queryset object
        """
        assert isinstance(self.query, Select), "Can't use aggregate on non selectable query"
        group_by_fields = [group_by_fields] if isinstance(group_by_fields, str) else group_by_fields
        select_fields = [select_fields] if isinstance(select_fields, (str, Aggregation)) else list(select_fields)

        self.return_raw = True

        if flat:
            assert len(select_fields) == 1, "There must be one selected field if flat=True"
            self.flat = True

        self.query = self.query.group_by(*(self.manager.table.c[field_name] for field_name in group_by_fields))
        fields_to_select = []

        for field in select_fields:
            if isinstance(field, Aggregation):
                aggregate_func = getattr(func, field.sql_func)(self._get_column_by_name(field.field)).label(field.label)
                self.annotations[field.label] = aggregate_func
                fields_to_select.append(aggregate_func)
            else:
                fields_to_select.append(self._get_column_by_name(field))

        self.query = self.query.with_only_columns(*fields_to_select)
        self.current_columns_to_select = fields_to_select

        having_expressions = []

        for field_expression, value in having.items():
            field_name, field_operator = self.parse_field_name_and_operator(field_expression)
            having_expressions.append(self.eval_operator(field_operator, field_name, value))

        if having_expressions:
            self.query = self.query.having(*having_expressions)

        return self

    def select_related(self, *related_models_to_join: str) -> Self:
        related_models_to_join_set = set(related_models_to_join) - self.joined_models

        if not related_models_to_join_set:
            return self

        assert isinstance(self.query, Select), "Can't select related tables on non selectable query"

        for rel_model_expression in related_models_to_join_set:
            # support recursive joins (<model>__<another_model_joined_with_model>)
            label_prefixes: List[str] = []
            current_model = self.manager.model

            for rel_model_name in rel_model_expression.split("__"):
                related_field = current_model.related_fields[rel_model_name]
                related_field_name = related_field.related_name

                if related_field_name is None or related_field.to_manager is None:
                    raise ValueError(f"Unsupported related model '{rel_model_expression}'")

                self.query = self.query.join(related_field.to_manager.table, isouter=True)  # type: ignore

                label_prefixes.append(related_field_name)
                prefix = ".".join(label_prefixes)

                for col in related_field.to_manager.table.c:
                    col_name = f"{prefix}.{col.name}"
                    labeled_column = col.label(col_name)
                    self.current_columns_to_select.append(labeled_column)
                    self.annotations[f"{prefix}__{col.name}"] = labeled_column

                current_model = related_field.to_model  # type: ignore
                self.joined_models.add(prefix)

        self.query = self.query.with_only_columns(*self.current_columns_to_select)  # type: ignore

        return self

    def join(
        self,
        model: Type["DatabaseModel"],
        on_clause: BaseCondition,
        join_model_name: Optional[str] = None,
        is_outer: bool = True,
    ) -> Self:
        """
        Construct sql join query with model
        :param model: Database model
        :param on_clause: join on expression.
        :param join_model_name: Name, that will be used to access related model columns.
        Lowered model class name by default
        :param is_outer: is_outer flag for join
        :return: Self
        """
        join_model_name = join_model_name or model.__name__.lower()

        if join_model_name in self.joined_models:
            return self

        assert isinstance(self.query, Select), "Can't select related tables on non selectable query"

        for col in model.objects.table.c:
            col_name = f"{join_model_name}.{col.name}"
            labeled_column = col.label(col_name)
            self.current_columns_to_select.append(labeled_column)
            self.annotations[f"{join_model_name}__{col.name}"] = labeled_column

        sql_alchemy_clause = self.parse_expression(on_clause)
        self.query = self.query.join(model.objects.table, isouter=is_outer, onclause=sql_alchemy_clause)  # type: ignore
        self.query = self.query.with_only_columns(*self.current_columns_to_select)  # type: ignore

        return self

    def prefetch_related(
        self, related_model_to_prefetch: str, prefetch_col_names: Optional[Iterable[str]] = None
    ) -> Self:
        assert isinstance(self.query, Select), "Can't select related tables on non selectable query"

        related_field = self.manager.model.related_fields[related_model_to_prefetch]
        related_field_name = related_field.related_name

        if related_field_name in self.prefetch_field_columns:
            return self

        from scrudge_orm.fields.fields import RelatedFieldBase

        if (
            related_field_name is None
            or related_field.to_manager is None
            or not isinstance(related_field, RelatedFieldBase)
        ):
            raise ValueError(f"Unsupported related model '{related_model_to_prefetch}'")

        self.prefetch_field_columns[related_field_name] = (
            tuple(related_field.to_manager.table.c[item] for item in prefetch_col_names)
            if prefetch_col_names is not None
            else tuple(related_field.to_manager.table.c)
        )

        if prefetch_col_names is not None:
            self.return_raw = True

        return self

    def add_callback(self, *callbacks: Callable) -> Self:
        """
        Adds a callback after the result is known
        :param callbacks: any sync function that get result as first argument
        :return: Self
        """
        for callback_func in callbacks:
            self.callbacks.append(callback_func)

        return self

    def limit(self, limit: int) -> Self:
        assert isinstance(self.query, Select), "Can't setup limit to non selectable query"
        self.query = self.query.limit(limit)

        return self

    def raw(self) -> Self:
        self.return_raw = True

        return self

    def flatten(self) -> Self:
        self.flat = True

        return self

    def for_update(self, no_key_update: bool = True) -> Self:
        assert isinstance(self.query, Select), "Can't select for update non selectable query"
        self.query = self.query.with_for_update(key_share=no_key_update)

        return self


class RawQuerySet(CallBackMixin):
    def __init__(
        self,
        query: Select | Update | Delete | Any,
        pool: "DatabaseBackend",
        flat: bool = False,
        fetch_one: bool = False,
        dict_key_fields: Optional[str | Tuple[str, ...]] = None,
    ):
        super().__init__()
        self.query = query
        self.pool = pool
        self.flat = flat
        self.fetch_one = fetch_one
        self.dict_key_fields = dict_key_fields

    async def _process_query_raw(self) -> Any:
        results = await self.pool.fetch_all(self.query)

        if self.dict_key_fields is None:
            parsed_results: Tuple | Dict = (
                tuple(v for row in results for v in row.values()) if self.flat else tuple(dict(row) for row in results)
            )
        else:
            parsed_results = {
                (
                    tuple(dicted_row.pop(field_name) for field_name in self.dict_key_fields)
                    if isinstance(self.dict_key_fields, tuple)
                    else dicted_row.pop(self.dict_key_fields)
                ): (tuple(dicted_row.values())[0] if self.flat else dicted_row)
                for row in results
                if (dicted_row := dict(row))
            }

        final_result = (parsed_results[0] if parsed_results else None) if self.fetch_one else parsed_results

        if self.is_callbacks_set:
            await self.execute_callbacks(final_result)

        return final_result

    def __await__(self) -> Any:
        return self._process_query_raw().__await__()
