import pytest

from tests.test_scrudge_orm.models import UnitTestIDPkPostgresModel


@pytest.mark.asyncio
async def test_eq_not_null_set_function() -> None:
    object_instance_id = await UnitTestIDPkPostgresModel.objects.update_or_create(
        int_field=100500,
        id=1,
    )

    assert object_instance_id.id == 1
    assert object_instance_id.int_field == 100500

    object_instance_id = await UnitTestIDPkPostgresModel.objects.update_or_create(
        int_field=None,
        id=1,
        set_functions={"int_field": "eq_not_null"},
    )

    assert object_instance_id.id == 1
    assert object_instance_id.int_field == 100500

    object_instance_id = await UnitTestIDPkPostgresModel.objects.update_or_create(
        int_field=100600,
        id=1,
        set_functions={"int_field": "eq_not_null"},
    )

    assert object_instance_id.id == 1
    assert object_instance_id.int_field == 100600
