from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.sql.sqltypes import BIGINT, BOOLEAN, FLOAT, INTEGER, SMALLINT, TEXT, TIMESTAMP, LargeBinary

from scrudge_orm.fields.consts import DatabaseFieldTypes


class PostgresFieldTypes(DatabaseFieldTypes):
    ARRAY_BIGINT = ARRAY(BIGINT)
    FLOAT = FLOAT
    TEXT = TEXT
    TIMESTAMP = TIMESTAMP
    INTEGER = INTEGER
    BIGINT = BIGINT
    SMALLINT = SMALLINT
    JSONB = JSONB
    UUID = UUID
    LargeBinary = LargeBinary
    BOOLEAN = BOOLEAN
