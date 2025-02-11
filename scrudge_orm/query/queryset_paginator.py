from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, List, Tuple, Union

if TYPE_CHECKING:
    from scrudge_orm.query.queryset import QuerySet


class QuerySetPaginator:
    def __init__(
        self,
        queryset: "QuerySet",
        pagination_field: str,
        limit: int = 20,
        order_desc: bool = False,
        is_increase: bool = True,
        start_pagination_value: Any = None,
        nulls_last: bool = False,
    ):
        """
        Paginator class constructor
        :param pagination_field: column to paginate
        :param limit: paginated items limit
        :param order_desc: flag, that means order by expression should be reverse (DESC).
        If false order by expression is ASC
        :param is_increase: flag, that paginator is increases, else decreases
        """
        self.queryset = queryset
        self.pagination_field = pagination_field
        self.order_desc = order_desc
        self.limit = limit
        self.is_increase = is_increase
        self.start_pagination_value = start_pagination_value
        self.pagination_field_serialized = self.pagination_field.replace("__", ".")
        self.nulls_last = nulls_last

    async def paginate_query(self) -> Tuple[Union[List, Tuple], Any]:
        """
        Transform query to query with pagination conditions
        :param start_pagination_value: start pagination value
        :return: Select query with pagination conditions
        """
        self.queryset = self.queryset.order_by(
            self.pagination_field, order_desc=self.order_desc, nulls_last=self.nulls_last
        )
        self.queryset = self.queryset.limit(self.limit + 1)

        # >= condition, because some results can have similar value if column not unique
        # it's better to get results twice, than skip some results
        if self.start_pagination_value is not None:
            operator = "ge" if self.is_increase else "le"
            self.queryset = self.queryset.filter(
                **{f"{self.pagination_field}__{operator}": self.start_pagination_value}
            )

        results = await self.queryset
        return self.get_next_pagination_value_and_final_results(results)

    def __await__(self) -> Any:
        return self.paginate_query().__await__()

    def get_limit_exceeded_last_item_index(self) -> int:
        """
        return last item index of pagination results if query limit exceeded
        :return: last item index
        """
        if (self.order_desc and not self.is_increase) or (not self.order_desc and self.is_increase):
            index = -1
        else:
            index = 0

        return index

    def get_next_pagination_value_and_final_results(
        self, results: Union[List, Tuple]
    ) -> Tuple[Union[List, Tuple], Any]:
        """
        Get slice of pagination results if query limit exceeded and return next pagination value
        :param results: results of pagination query
        :return: Tuple with pagination results and next pagination value
        """
        if len(results) > self.limit:
            last_item_index = self.get_limit_exceeded_last_item_index()

            if isinstance(results[last_item_index], Mapping):
                next_pagination_value = results[last_item_index][self.pagination_field_serialized]
            else:
                next_pagination_value = getattr(results[last_item_index], self.pagination_field_serialized)

            final_results = results[1:] if last_item_index == 0 else results[0 : self.limit]
        else:
            next_pagination_value = None
            final_results = results

        return final_results, next_pagination_value
