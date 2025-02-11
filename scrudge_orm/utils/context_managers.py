import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional, Type

if TYPE_CHECKING:
    from logging import Logger
    from types import TracebackType

lib_logger = logging.getLogger(__name__)


class log_execution_time:
    def __init__(self, log_message: str, logger: Optional["Logger"] = None) -> None:
        self.log_message = log_message
        self.logger = logger or lib_logger

    def __enter__(self) -> None:
        self.start_time = datetime.utcnow()

    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_val: BaseException | None, exc_tb: Optional["TracebackType"]
    ) -> None:
        execution_time = (datetime.utcnow() - self.start_time).total_seconds()
        logged_time = "{:.3f}".format(execution_time)
        result = "succeeded" if exc_type is None else "failed"

        self.logger.info(f"{self.log_message} {result} in {logged_time}s")
