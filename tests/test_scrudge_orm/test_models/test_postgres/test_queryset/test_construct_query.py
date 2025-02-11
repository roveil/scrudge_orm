import pytest

from tests.test_scrudge_orm.models import UnitTestOptionalPostgresModel


@pytest.mark.asyncio
async def test_query_comparison_filter() -> None:
    results = (
        await UnitTestOptionalPostgresModel.objects.filter(int_field__gt=1)
        .values_list("int_field", flat=True)
        .order_by("int_field")
    )

    assert results == (2, 3, 4, 5)

    results = (
        await UnitTestOptionalPostgresModel.objects.filter(int_field__ge=1)
        .values_list("int_field", flat=True)
        .order_by("int_field")
    )

    assert results == (1, 2, 3, 4, 5)

    results = (
        await UnitTestOptionalPostgresModel.objects.filter(int_field__lt=5)
        .values_list("int_field", flat=True)
        .order_by("int_field")
    )

    assert results == (1, 2, 3, 4)

    results = (
        await UnitTestOptionalPostgresModel.objects.filter(int_field__le=5)
        .values_list("int_field", flat=True)
        .order_by("int_field")
    )

    assert results == (1, 2, 3, 4, 5)


@pytest.mark.asyncio
async def test_query_comparison_exclude() -> None:
    results = (
        await UnitTestOptionalPostgresModel.objects.exclude(int_field__gt=1)
        .values_list("int_field", flat=True)
        .order_by("int_field")
    )

    assert results == (1,)

    results = (
        await UnitTestOptionalPostgresModel.objects.exclude(int_field__ge=1)
        .values_list("int_field", flat=True)
        .order_by("int_field")
    )

    assert results == ()

    results = (
        await UnitTestOptionalPostgresModel.objects.exclude(int_field__lt=5)
        .values_list("int_field", flat=True)
        .order_by("int_field")
    )

    assert results == (5,)

    results = (
        await UnitTestOptionalPostgresModel.objects.exclude(int_field__le=5)
        .values_list("int_field", flat=True)
        .order_by("int_field")
    )

    assert results == ()


@pytest.mark.asyncio
async def test_multiple_filter_expressions() -> None:
    results = (
        await UnitTestOptionalPostgresModel.objects.filter(int_field__gt=1)
        .filter(int_field__lt=4)
        .values_list("int_field", flat=True)
        .order_by("int_field")
    )

    assert results == (2, 3)

    results = (
        await UnitTestOptionalPostgresModel.objects.filter(int_field__gt=1)
        .exclude(int_field__ge=4)
        .values_list("int_field", flat=True)
        .order_by("int_field")
    )

    assert results == (2, 3)


@pytest.mark.asyncio
async def test_filter_str_expressions_contains() -> None:
    results = (
        await UnitTestOptionalPostgresModel.objects.filter(str_field__contains="5")
        .values_list("str_field", flat=True)
        .order_by("str_field")
    )

    assert results == ("12345",)

    results = (
        await UnitTestOptionalPostgresModel.objects.filter(str_field__contains="4")
        .values_list("str_field", flat=True)
        .order_by("str_field")
    )

    assert results == ("1234", "12345")


@pytest.mark.asyncio
async def test_filter_str_expressions_like() -> None:
    results = (
        await UnitTestOptionalPostgresModel.objects.filter(str_field__like="12%")
        .values_list("str_field", flat=True)
        .order_by("str_field")
    )

    assert results == ("12", "123", "1234", "12345")

    results = (
        await UnitTestOptionalPostgresModel.objects.exclude(str_field__like="12%")
        .values_list("str_field", flat=True)
        .order_by("str_field")
    )

    assert results == ("1",)


@pytest.mark.asyncio
async def test_exists() -> None:
    result = await UnitTestOptionalPostgresModel.objects.filter(str_field="123").exists()

    assert result is True

    result = await UnitTestOptionalPostgresModel.objects.filter(str_field="not_exist").exists()

    assert result is None
