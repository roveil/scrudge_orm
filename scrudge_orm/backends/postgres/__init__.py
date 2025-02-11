from typing import Any

from scrudge_orm.backends.base import DatabaseBackend, DatabaseSettings
from scrudge_orm.backends.consts import SupportedPGDriver


class PGDatabaseSettings(DatabaseSettings):
    driver: SupportedPGDriver


class PGDatabaseBackend(DatabaseBackend):
    def __init__(
        self, settings: PGDatabaseSettings, project_root_dir: str, tag_sql_queries: bool = False, **kwargs: Any
    ) -> None:
        super().__init__(settings, project_root_dir, tag_sql_queries=tag_sql_queries, **kwargs)
