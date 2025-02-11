from typing import List

import pytest

from tests.test_scrudge_orm.models import UnitTestOptionalPostgresModel


@pytest.mark.asyncio
async def test_query_comparison_filter(unittest_optional_pg_model_pg_data: List[UnitTestOptionalPostgresModel]) -> None:
    results = (
        await UnitTestOptionalPostgresModel.objects.filter(int_field=1)
        .update(int_field=100, returning="int_field")
        .values_list("int_field", flat=True)
        .fetch_single()
    )

    assert results == 100
