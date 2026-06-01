"""Concurrency primitives: rate limiter, interruptible pools, and worker bootstrap.

Author: Emre Tezel
"""

from __future__ import annotations

import concurrent.futures.thread as _thread_futures
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
)
from threading import Lock, Thread
import time
from typing import (
    Callable,
    Optional,
    Sequence,
    cast,
)
import weakref

from pyvalue.logging_utils import (
    current_logging_config,
    setup_logging,
)

from ._common import (
    LOGGER,
    MARKET_DATA_RATE_LIMIT_BURST,
)


class _RateLimiter:
    """Token-bucket limiter shared across concurrent EODHD worker threads."""

    def __init__(
        self, rate_per_minute: float, burst: int = MARKET_DATA_RATE_LIMIT_BURST
    ):
        self.rate_per_second = max(rate_per_minute, 0.0) / 60.0
        self.capacity = float(max(burst, 1))
        self.tokens = self.capacity
        self.updated_at = time.monotonic()
        self._lock = Lock()

    def acquire(self) -> None:
        if self.rate_per_second <= 0:
            return
        while True:
            wait_time = 0.0
            with self._lock:
                now = time.monotonic()
                elapsed = now - self.updated_at
                self.tokens = min(
                    self.capacity, self.tokens + (elapsed * self.rate_per_second)
                )
                self.updated_at = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait_time = (1.0 - self.tokens) / self.rate_per_second
            if wait_time > 0:
                time.sleep(wait_time)


class _InterruptibleThreadPoolExecutor(ThreadPoolExecutor):
    """Thread pool whose workers do not block interpreter shutdown on Ctrl+C."""

    def _adjust_thread_count(self) -> None:
        if self._idle_semaphore.acquire(timeout=0):
            return

        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            thread_name = "%s_%d" % (self._thread_name_prefix or self, num_threads)
            thread = Thread(
                name=thread_name,
                target=_thread_futures._worker,
                args=(
                    weakref.ref(self, weakref_cb),
                    self._work_queue,
                    self._initializer,
                    self._initargs,
                ),
                daemon=True,
            )
            thread.start()
            cast(set[Thread], self._threads).add(thread)


def _create_interruptible_thread_executor(
    max_workers: int,
) -> _InterruptibleThreadPoolExecutor:
    """Return a thread pool that can be abandoned promptly on Ctrl+C."""

    return _InterruptibleThreadPoolExecutor(max_workers=max_workers)


def _terminate_process_pool_workers(executor: ProcessPoolExecutor) -> None:
    """Best-effort terminate running process workers without waiting for them."""

    processes = getattr(executor, "_processes", None)
    if not isinstance(processes, dict):
        return
    for process in list(processes.values()):
        if process is None:
            continue
        try:
            if process.is_alive():
                process.terminate()
        except Exception:  # pragma: no cover - defensive cleanup path
            continue


def _shutdown_executor_now(executor: object) -> None:
    """Stop an executor without waiting for outstanding work to finish."""

    if isinstance(executor, ProcessPoolExecutor):
        _terminate_process_pool_workers(executor)
    shutdown = getattr(executor, "shutdown", None)
    if shutdown is None:
        return
    try:
        shutdown(wait=False, cancel_futures=True)
    except TypeError:
        shutdown(wait=False)


def _cancel_cli_command(
    message: str,
    *,
    executors: Sequence[object] = (),
    flushers: Sequence[Callable[[], None]] = (),
) -> int:
    """Flush parent state, stop workers, and exit a command cleanly."""

    for executor in executors:
        if executor is not None:
            _shutdown_executor_now(executor)
    for flusher in flushers:
        try:
            flusher()
        except Exception as exc:  # pragma: no cover - defensive cleanup path
            LOGGER.error("Failed to flush pending state during cancellation: %s", exc)
    print(message, flush=True)
    return 1


def _create_process_pool_executor(max_workers: int) -> ProcessPoolExecutor:
    """Create a process pool using the interpreter's platform default start method."""

    log_dir, console_level, file_level = current_logging_config()
    if log_dir is None:
        return ProcessPoolExecutor(max_workers=max_workers)
    return ProcessPoolExecutor(
        max_workers=max_workers,
        initializer=_initialize_worker_logging,
        initargs=(
            str(log_dir) if log_dir is not None else None,
            console_level,
            file_level,
        ),
    )


def _initialize_worker_logging(
    log_dir: Optional[str],
    console_level: int,
    file_level: int,
) -> None:
    """Mirror the parent logging configuration inside spawned worker processes."""

    setup_logging(
        log_dir=log_dir or "data/logs",
        console_level=console_level,
        file_level=file_level,
    )
