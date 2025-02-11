from typing import TYPE_CHECKING, Generator

from sqlalchemy import create_engine

from scrudge_orm.backends.postgres import SupportedPGDriver

if TYPE_CHECKING:
    from scrudge_orm.backends.postgres import PGDatabaseBackend, PGDatabaseSettings


def create_pg_db_models(database_backend: "PGDatabaseBackend", database_settings: "PGDatabaseSettings") -> Generator:
    if not database_settings.db.endswith("_tests"):
        raise EnvironmentError("Database name should end with _tests. You should use special database for tests")

    sync_connection = database_settings.get_connection_string(force_driver=SupportedPGDriver.PSYCOPG2)

    # Support for postgresql_concurrently index creation in tests. Need to use isolation level - Autocommit
    sync_engine = create_engine(sync_connection, echo=False, execution_options={"isolation_level": "AUTOCOMMIT"})

    sync_engine.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    sync_engine.execute("CREATE EXTENSION IF NOT EXISTS btree_gin;")
    database_backend.metadata.create_all(sync_engine)

    yield

    sync_engine.execute(
        """
    DROP SCHEMA public CASCADE;
    CREATE SCHEMA public;
    """
    )

    # database_backend.metadata.drop_all(sync_engine)
