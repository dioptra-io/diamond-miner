import time
from logging import Logger
from types import TracebackType
from typing import Optional, Type


class Timer:
    """A very simple timer for profiling code blocks."""

    start_time = None
    total_time = 0

    def start(self) -> None:
        self.start_time = time.time_ns()

    def stop(self) -> None:
        if self.start_time:
            self.total_time += time.time_ns() - self.start_time
            self.start_time = None

    def clear(self) -> None:
        self.start_time = None
        self.total_time = 0

    @property
    def total_ms(self) -> float:
        return self.total_time / 10 ** 6

    def __enter__(self) -> None:
        self.start()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.stop()


class LoggingTimer:
    """A very simple timer for logging the execution time of code blocks."""

    def __init__(self, logger: Logger, prefix: str = ""):
        self.logger = logger
        self.prefix = prefix
        self.timer = Timer()

    def __enter__(self) -> None:
        self.logger.info(self.prefix)
        self.timer.clear()
        self.timer.start()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.timer.stop()
        self.logger.info("%s time_ms=%s", self.prefix, self.timer.total_ms)
