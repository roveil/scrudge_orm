from asyncio import gather
from typing import TYPE_CHECKING

import pytest
from asyncpg.exceptions import UniqueViolationError

from tests.test_scrudge_orm.models import UnitTestOptionalPostgresModel

if TYPE_CHECKING:
    from databases.core import Transaction


@pytest.mark.asyncio
async def OptionalPostgresModel(postgres_transaction: "Transaction") -> None:
    await UnitTestOptionalPostgresModel.objects.create(int_field=6)

    with pytest.raises(UniqueViolationError):
        await UnitTestOptionalPostgresModel.objects.create(int_field=6)

    await gather(
        UnitTestOptionalPostgresModel.objects.create(int_field=3),
        UnitTestOptionalPostgresModel.objects.create(int_field=3),
    )
