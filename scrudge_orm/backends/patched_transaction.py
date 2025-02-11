from collections import Counter
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from databases.core import Transaction

ON_TRANSACTION: ContextVar[Counter] = ContextVar("on-transaction-state", default=Counter())

if TYPE_CHECKING:
    from scrudge_orm.backends.base import DatabaseBackend


class PatchedTransaction(Transaction):
    def __init__(
        self,
        database: "DatabaseBackend",
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.__database_backend_id = id(database)
        super().__init__(*args, **kwargs)

    async def __aenter__(self) -> "Transaction":
        """
        Override, because need to know, is it current
        """
        transaction = await super().__aenter__()

        transaction_counter = ON_TRANSACTION.get()
        transaction_counter[self.__database_backend_id] += 1
        ON_TRANSACTION.set(transaction_counter)

        return transaction

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        """
        Called when exiting `async with database.transaction()`
        """
        await super().__aexit__(*args, **kwargs)

        transaction_counter = ON_TRANSACTION.get()
        transaction_counter[self.__database_backend_id] -= 1
        ON_TRANSACTION.set(transaction_counter)
