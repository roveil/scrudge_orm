from enum import Enum


class SupportedDriver(str, Enum):
    pass


class SupportedPGDriver(SupportedDriver):
    PSYCOPG2 = "postgresql+psycopg2"
    ASYNC_PG = "postgresql+asyncpg"
