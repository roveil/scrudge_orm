import pytest

from tests.test_scrudge_orm.models import UnitTestIDPkPostgresModel


@pytest.mark.asyncio
async def test_update() -> None:
    model_instance: UnitTestIDPkPostgresModel = await UnitTestIDPkPostgresModel.objects.create(int_field=100)

    assert model_instance.int_field == 100

    await model_instance.update(int_field=200)
    assert await UnitTestIDPkPostgresModel.objects.filter(int_field=200).exists() is True
    assert not await UnitTestIDPkPostgresModel.objects.filter(int_field=100).exists()
