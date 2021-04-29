import time


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

    @property
    def total_ms(self) -> float:
        return self.total_time / 10 ** 6

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
