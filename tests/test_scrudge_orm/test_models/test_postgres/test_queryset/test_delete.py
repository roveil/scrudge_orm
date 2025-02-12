from typing import List

import pytest

from tests.test_scrudge_orm.models import UnitTestOptionalPostgresModel


@pytest.mark.asyncio
async def test_delete_operation(unittest_optional_pg_model_pg_data: List[UnitTestOptionalPostgresModel]) -> None:
    await UnitTestOptionalPostgresModel.objects.filter(int_field=1).delete()

    assert not await UnitTestOptionalPostgresModel.objects.filter(int_field=1).exists()
