import time
from dataclasses import fields
from logging import Logger
from types import TracebackType
from typing import Any, Dict, Optional, Type


def common_parameters(from_dataclass: Any, to_dataclass: Any) -> Dict[str, Any]:
    to_params = {field.name for field in fields(to_dataclass)}
    return {
        field.name: getattr(from_dataclass, field.name)
        for field in fields(from_dataclass)
        if field.name in to_params
    }


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
