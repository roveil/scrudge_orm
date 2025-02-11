from asyncio import get_event_loop_policy
from typing import TYPE_CHECKING, Any, Generator

import pytest

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from unittest.mock import AsyncMock

    from pytest_mock.plugin import MockerFixture

    class AsyncMocker(MockerFixture):
        @staticmethod
        def future_patch(unit: str, return_value: Any = None, *args: Any, **kwargs: Any) -> "AsyncMock": ...

        @staticmethod
        def coroutine_patch(unit: str, return_value: Any = None, *args: Any, **kwargs: Any) -> "AsyncMock": ...


@pytest.fixture(scope="session", autouse=True)
def event_loop() -> Generator["AbstractEventLoop", Any, None]:
    policy = get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def async_mocker(event_loop: "AbstractEventLoop", mocker: "AsyncMocker") -> "AsyncMocker":
    def future_patch(unit: str, return_value: Any = None, **kwargs: Any) -> "AsyncMock":
        return mocker.patch(unit, return_value=return_value, **kwargs)

    def coroutine_patch(unit: str, return_value: Any = None, **kwargs: Any) -> "AsyncMock":
        async def simple_coro(*_: Any, **__: Any) -> Any:
            return return_value

        coro = simple_coro()
        return mocker.patch(unit, return_value=coro, **kwargs)

    mocker.future_patch = future_patch  # type: ignore
    mocker.coroutine_patch = coroutine_patch  # type: ignore

    return mocker
