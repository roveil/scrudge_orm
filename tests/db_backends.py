from os import environ, path

from dotenv import load_dotenv

from scrudge_orm.backends.postgres import PGDatabaseBackend, PGDatabaseSettings, SupportedPGDriver

ROOT_DIR = path.dirname(__file__).replace("tests", "")
load_dotenv(environ.get("ENV_FILE_NAME", ".env.tests"), override=True)

postgres_settings = PGDatabaseSettings(
    driver=SupportedPGDriver.ASYNC_PG,
    host=environ.get("PSQL_HOST", "localhost"),
    port=environ.get("PSQL_PORT", 5432),
    user=environ.get("PSQL_USER", "postgres"),
    password=environ.get("PSQL_PASSWORD", "postgres"),
    db=environ.get("PSQL_DB", "libs_tests"),
    min_pool_size=16,
    max_pool_size=512,
)
postgres_backend = PGDatabaseBackend(settings=postgres_settings, project_root_dir=ROOT_DIR)
