from functools import wraps
from os import path
from typing import Any, Callable, Generator, List

import pytest

from scrudge_orm.testing.fixtures import async_mocker as async_mocker_lib
from scrudge_orm.testing.fixtures import event_loop as event_loop_lib
from scrudge_orm.testing.helpers import create_pg_db_models
from scrudge_orm.testing.json_to_model_creator import json_to_model_bulk_creator
from scrudge_orm.utils.imports import import_by_sources
from tests.db_backends import postgres_backend, postgres_settings
from tests.test_scrudge_orm.models import UnitTestOptionalPostgresModel

async_mocker = async_mocker_lib
event_loop = event_loop_lib


@pytest.fixture(scope="session", autouse=True)
def db_session() -> Generator:
    import_by_sources(path.join(path.dirname(__file__), "test_scrudge_orm"), "models")

    yield from create_pg_db_models(postgres_backend, postgres_settings)


@pytest.fixture(scope="session", autouse=True)
async def unittest_optional_pg_model_pg_data() -> List[UnitTestOptionalPostgresModel]:
    fixture_path = path.join(
        path.dirname(__file__), "test_scrudge_orm/test_models/models_data/unittestoptionalpostgresmodel.json"
    )
    return await json_to_model_bulk_creator(UnitTestOptionalPostgresModel, fixture_path)


def postgres_transaction(func: Callable) -> Callable:
    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        async with await postgres_backend.transaction(force_rollback=True):
            return await func(*args, **kwargs)

    return wrapper


def pytest_runtest_call(item: Any) -> None:
    c = pytest.Mark("asyncio", args=(), kwargs={})

    if c in getattr(item.obj, "pytestmark", []):
        item.obj = postgres_transaction(item.obj)
