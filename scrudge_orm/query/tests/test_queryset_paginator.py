from scrudge_orm.query.queryset_paginator import QuerySetPaginator


class TestGetResultsAndNextPaginationValue:
    limit = 4
    asc_results = [{"id": item} for item in range(1, 6)]  # 1,2,3,4,5
    desc_results = list(reversed(asc_results))  # 5,4,3,2,1
    column = "id"

    def test_asc_increasing_limit_exceeded(self) -> None:
        paginator = QuerySetPaginator(None, self.column, limit=self.limit)  # type: ignore
        results, next_pagination_value = paginator.get_next_pagination_value_and_final_results(self.asc_results)

        assert results == [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]
        assert next_pagination_value == 5

    def test_asc_decreasing_limit_exceeded(self) -> None:
        paginator = QuerySetPaginator(None, self.column, limit=self.limit, is_increase=False)  # type: ignore
        results, next_pagination_value = paginator.get_next_pagination_value_and_final_results(self.asc_results)

        assert results == [{"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}]
        assert next_pagination_value == 1

    def test_desc_increasing_limit_exceeded(self) -> None:
        paginator = QuerySetPaginator(None, self.column, limit=self.limit, order_desc=True)  # type: ignore
        results, next_pagination_value = paginator.get_next_pagination_value_and_final_results(self.desc_results)

        assert results == [{"id": 4}, {"id": 3}, {"id": 2}, {"id": 1}]
        assert next_pagination_value == 5

    def test_desc_decreasing_limit_exceeded(self) -> None:
        paginator = QuerySetPaginator(
            None,  # type: ignore
            self.column,
            limit=self.limit,
            order_desc=True,
            is_increase=False,
        )
        results, next_pagination_value = paginator.get_next_pagination_value_and_final_results(self.desc_results)

        assert results == [{"id": 5}, {"id": 4}, {"id": 3}, {"id": 2}]
        assert next_pagination_value == 1

    def test_limit_not_exceeded(self) -> None:
        paginator = QuerySetPaginator(None, self.column, limit=self.limit + 1)  # type: ignore
        results, next_pagination_value = paginator.get_next_pagination_value_and_final_results(self.desc_results)

        assert results == self.desc_results
        assert next_pagination_value is None
