from typing import Optional, Type

import pytest
from pydantic import ValidationError

from scrudge_orm.fields.postgres import PostgresField, PostgresFieldTypes
from scrudge_orm.models.base import MetaBase
from scrudge_orm.models.postgres import PostgresModel


class UnitTestModel(PostgresModel):
    int_field: int = PostgresField(PostgresFieldTypes.INTEGER, gt=1, lt=10, nullable=False)

    class Meta(MetaBase):
        is_proxy = True


class UnitTestModelWithEQ(UnitTestModel):
    int_field: int = PostgresField(PostgresFieldTypes.INTEGER, ge=1, le=10, nullable=False)

    class Meta(MetaBase):
        is_proxy = True


class UnitTestModelOptionalInt(UnitTestModel):
    int_field: Optional[int] = PostgresField(PostgresFieldTypes.INTEGER, gt=1, lt=10)  # type: ignore

    class Meta(MetaBase):
        is_proxy = True


class UnitTestModelOptionalIntWithEQ(UnitTestModel):
    int_field: Optional[int] = PostgresField(PostgresFieldTypes.INTEGER, ge=1, le=10)  # type: ignore

    class Meta(MetaBase):
        is_proxy = True


def _test_int_field_gt_lt(model_cls: Type[UnitTestModel], allow_none: bool = False) -> None:
    model_cls(int_field=9)

    with pytest.raises(ValidationError):
        model_cls(int_field=10)

    with pytest.raises(ValidationError):
        model_cls(int_field=1)

    if allow_none:
        model_cls(int_field=None)
    else:
        with pytest.raises(ValidationError):
            model_cls(int_field=None)


def _test_int_field_ge_le(model_cls: Type[UnitTestModel], allow_none: bool = False) -> None:
    model_cls(int_field=9)
    model_cls(int_field=1)
    model_cls(int_field=10)

    with pytest.raises(ValidationError):
        model_cls(int_field=11)

    with pytest.raises(ValidationError):
        model_cls(int_field=0)

    if allow_none:
        model_cls(int_field=None)
    else:
        with pytest.raises(ValidationError):
            model_cls(int_field=None)


def test_gt_lt_without_none() -> None:
    _test_int_field_gt_lt(UnitTestModel)


def test_ge_le_without_none() -> None:
    _test_int_field_ge_le(UnitTestModelWithEQ)


def test_gt_lt_with_none() -> None:
    _test_int_field_gt_lt(UnitTestModelOptionalInt, allow_none=True)


def test_ge_le_with_none() -> None:
    _test_int_field_ge_le(UnitTestModelOptionalIntWithEQ, allow_none=True)


def test_compare() -> None:
    assert UnitTestModel(int_field=5) == UnitTestModel(int_field=5)

    assert UnitTestModel(int_field=5) != UnitTestModel(int_field=6)


def test_convert_to_int() -> None:
    UnitTestModel(int_field="5")

    with pytest.raises(ValidationError):
        UnitTestModel(int_field="5.5")

    with pytest.raises(ValidationError):
        UnitTestModel(int_field="fadsfaf")
