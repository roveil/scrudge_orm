from typing import Optional

from scrudge_orm.crypto.pydantic_custom_types import SHA512EncryptedStringAnnotation
from scrudge_orm.fields.postgres import PostgresField, PostgresFieldTypes
from scrudge_orm.models.postgres import PostgresMeta, PostgresModel, PostgresUniqueIndex
from tests.db_backends import postgres_backend


class UnitTestOptionalPostgresModel(PostgresModel):
    int_field: Optional[int] = PostgresField(PostgresFieldTypes.INTEGER, nullable=True)
    str_field: Optional[str] = PostgresField(PostgresFieldTypes.TEXT, nullable=True)

    class Meta(PostgresMeta):
        db_backend = postgres_backend
        table_name = "optional_model"
        unique_indexes = (
            PostgresUniqueIndex(name="int_gt_five_unique", fields=("int_field",), postgresql_where="int_field > 5"),
        )


class UnitTestSHA512CryptoModel(PostgresModel):
    field_str: SHA512EncryptedStringAnnotation = PostgresField(PostgresFieldTypes.TEXT)

    class Meta(PostgresMeta):
        db_backend = postgres_backend


class UnitTestIDPkPostgresModel(PostgresModel):
    id: int = PostgresField(
        PostgresFieldTypes.BIGINT, default=None, primary_key=True, autoincrement=True
    )
    int_field: Optional[int] = PostgresField(PostgresFieldTypes.INTEGER, nullable=True)

    class Meta(PostgresMeta):
        db_backend = postgres_backend
