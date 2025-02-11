import pytest
from pydantic import ValidationError

from scrudge_orm.fields.postgres import PostgresField, PostgresFieldTypes
from scrudge_orm.models.base import MetaBase
from scrudge_orm.models.postgres import PostgresModel


class UnitTestParent1(PostgresModel):
    int_field1: int = PostgresField(PostgresFieldTypes.INTEGER, gt=1, lt=10, nullable=False)

    class Meta(MetaBase):
        is_proxy = True


class UnitTestParent2(PostgresModel):
    int_field2: int = PostgresField(PostgresFieldTypes.INTEGER, gt=1, lt=10, nullable=False)

    class Meta(MetaBase):
        is_proxy = True


class UnitTestChild(UnitTestParent1, UnitTestParent2):
    class Meta(MetaBase):
        is_proxy = True


def test_validation_child() -> None:
    UnitTestChild(int_field1=2, int_field2=5)

    with pytest.raises(ValidationError):
        UnitTestChild(int_field1=0, int_field2=5)

    with pytest.raises(ValidationError):
        UnitTestChild(int_field1=5, int_field2=0)
