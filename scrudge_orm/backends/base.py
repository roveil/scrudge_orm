import traceback
from asyncio import Lock
from functools import partial
from typing import TYPE_CHECKING, Any, List, Mapping, Optional, Union

from databases import Database
from pydantic import BaseModel
from sqlalchemy import MetaData

from scrudge_orm.backends.consts import SupportedDriver
from scrudge_orm.backends.patched_transaction import ON_TRANSACTION, PatchedTransaction

if TYPE_CHECKING:
    from sqlalchemy.sql import ClauseElement


# This class is parent for childhoods, it is the reason why slots is False
class DatabaseSettings(BaseModel):
    driver: SupportedDriver

    user: str
    password: str
    db: str

    host: Optional[str] = None
    port: Optional[int] = None
    socket: Optional[str] = None

    max_pool_size: Optional[int] = None
    min_pool_size: Optional[int] = None

    def get_connection_string(self, force_driver: Optional[SupportedDriver] = None) -> str:
        driver = force_driver or self.driver

        return (
            f"{driver.value}://{self.user}:{self.password}@/{self.db}?host={self.socket}"
            if self.socket is not None
            else f"{driver.value}://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        )


class DatabaseBackend:
    __slots__ = ("metadata", "pool", "_connect_lock", "project_root_dir", "tag_sql_queries")

    def __init__(
        self,
        settings: DatabaseSettings,
        project_root_dir: str,
        tag_sql_queries: bool = False,
        **kwargs: Any,
    ) -> None:
        self.pool = Database(
            settings.get_connection_string(),
            min_size=settings.min_pool_size or 1,
            max_size=settings.max_pool_size or 1,
            **kwargs,
        )
        self.metadata = MetaData()
        self.project_root_dir = project_root_dir
        self.tag_sql_queries = (tag_sql_queries,)
        self._connect_lock: Optional[Lock] = None

    @property
    def lock(self) -> Lock:
        if self._connect_lock is None:
            self._connect_lock = Lock()
        return self._connect_lock

    async def connect(self) -> None:
        async with self.lock:
            if not self.pool.is_connected:
                await self.pool.connect()

    async def transaction(self, force_rollback: bool = False, **kwargs: Any) -> "PatchedTransaction":
        await self.connect()
        return PatchedTransaction(self, self.pool.connection, force_rollback=force_rollback, **kwargs)

    def is_on_transaction(self) -> bool:
        transaction_counters = ON_TRANSACTION.get()

        return bool(transaction_counters[id(self)])

    async def execute(self, query: Union[str, "ClauseElement"], values: Optional[dict] = None) -> Any:
        await self.connect()
        return await self.pool.execute(query, values)

    async def execute_many(self, query: Union[str, "ClauseElement"], values: List) -> Any:
        await self.connect()
        return await self.pool.execute_many(query, values)

    @staticmethod
    def compile_query_with_comments(
        query: Union[str, "ClauseElement"], project_root_dir: str, *args: Any, **kwargs: Any
    ) -> Any:
        compile_result = query.sqlalchemy_compile(*args, **kwargs)  # type: ignore

        try:
            trace_stack = traceback.extract_stack()
            outer_frame, inner_frame = None, None
            for frame in trace_stack:
                if frame.filename.startswith(project_root_dir) and "scrudge_orm" not in frame.filename:
                    inner_frame = frame
                    if outer_frame is None:
                        outer_frame = frame
        except StopIteration:
            pass
        else:
            if inner_frame is not None and outer_frame is not None:
                inner_file_name = inner_frame.filename[len(project_root_dir) + 1 :]  # +1 для `/`
                outer_file_name = outer_frame.filename[len(project_root_dir) + 1 :]  # +1 для `/`
                compile_result.string = (
                    f"/* inner_func:{inner_frame.name}, inner_line:{inner_file_name}:{inner_frame.lineno},"
                    f" outer_func:{outer_frame.name}, outer_line:{outer_file_name}:{outer_frame.lineno} */ "
                    f"{compile_result.string}"
                )

        return compile_result

    async def fetch_one(self, query: Union[str, "ClauseElement"], values: Optional[dict] = None) -> Optional[Mapping]:
        await self.connect()

        if self.tag_sql_queries:
            query.sqlalchemy_compile = query.compile  # type: ignore
            query.compile = partial(  # type: ignore
                self.compile_query_with_comments, query, self.project_root_dir
            )

        return await self.pool.fetch_one(query, values)  # type: ignore

    async def fetch_all(self, query: Union[str, "ClauseElement"], values: Optional[dict] = None) -> List[Mapping]:
        await self.connect()

        if self.tag_sql_queries:
            query.sqlalchemy_compile = query.compile  # type: ignore
            query.compile = partial(  # type: ignore
                self.compile_query_with_comments, query, self.project_root_dir
            )

        return await self.pool.fetch_all(query, values)  # type: ignore
