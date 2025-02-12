from typing import Optional, Type

import pytest
from pydantic import ValidationError

from scrudge_orm.fields.postgres import PostgresField, PostgresFieldTypes
from scrudge_orm.models.base import MetaBase
from scrudge_orm.models.postgres import PostgresModel


class UnitTestModel(PostgresModel):
    float_field: float = PostgresField(PostgresFieldTypes.FLOAT, gt=1.5, lt=10.5)

    class Meta(MetaBase):
        is_proxy = True


class UnitTestModelWithEQ(UnitTestModel):
    float_field: float = PostgresField(PostgresFieldTypes.FLOAT, ge=1.5, le=10.5, nullable=False)

    class Meta(MetaBase):
        is_proxy = True


class UnitTestModelOptionalInt(UnitTestModel):
    float_field: Optional[float] = PostgresField(PostgresFieldTypes.FLOAT, gt=1.5, lt=10.5, nullable=True)  # type: ignore

    class Meta(MetaBase):
        is_proxy = True


class UnitTestModelOptionalIntWithEQ(UnitTestModel):
    float_field: Optional[float] = PostgresField(PostgresFieldTypes.FLOAT, ge=1.5, le=10.5, nullable=True)  # type: ignore

    class Meta(MetaBase):
        is_proxy = True


def _test_float_field_gt_lt(model_cls: Type[UnitTestModel], allow_none: bool = False) -> None:
    model_cls(float_field=9.5)

    with pytest.raises(ValidationError):
        model_cls(float_field=10.5)

    with pytest.raises(ValidationError):
        model_cls(float_field=1.49)

    if allow_none:
        model_cls(float_field=None)
    else:
        with pytest.raises(ValidationError):
            model_cls(float_field=None)


def _test_float_field_ge_le(model_cls: Type[UnitTestModel], allow_none: bool = False) -> None:
    model_cls(float_field=9.5)
    model_cls(float_field=1.5)
    model_cls(float_field=10.5)

    with pytest.raises(ValidationError):
        model_cls(float_field=10.6)

    with pytest.raises(ValidationError):
        model_cls(float_field=1.4)

    if allow_none:
        model_cls(float_field=None)
    else:
        with pytest.raises(ValidationError):
            model_cls(float_field=None)


def test_gt_lt_without_none() -> None:
    _test_float_field_gt_lt(UnitTestModel)


def test_ge_le_without_none() -> None:
    _test_float_field_ge_le(UnitTestModelWithEQ)


def test_gt_lt_with_none() -> None:
    _test_float_field_gt_lt(UnitTestModelOptionalInt, allow_none=True)


def test_ge_le_with_none() -> None:
    _test_float_field_ge_le(UnitTestModelOptionalIntWithEQ, allow_none=True)


def test_compare() -> None:
    assert UnitTestModel(float_field=5) == UnitTestModel(float_field=5)

    assert UnitTestModel(float_field=5) != UnitTestModel(float_field=6)


def test_convert_to_float() -> None:
    UnitTestModel(float_field="5")

    UnitTestModel(float_field="5.5")

    with pytest.raises(ValidationError):
        UnitTestModel(float_field="fadsfaf")
