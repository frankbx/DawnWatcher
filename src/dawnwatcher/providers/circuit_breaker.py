"""Small in-memory circuit breaker for free quote endpoints."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum


class CircuitState(StrEnum):
    """Circuit state exposed to collection health reports."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(slots=True)
class CircuitBreaker:
    """Open after consecutive failures and permit one probe after cooldown."""

    failure_threshold: int = 3
    cooldown_seconds: float = 60.0
    clock: Callable[[], float] = time.monotonic
    state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    opened_at: float | None = None

    def allow_request(self) -> bool:
        """Return whether a normal request or recovery probe may proceed."""
        if self.state is CircuitState.CLOSED:
            return True
        if self.state is CircuitState.HALF_OPEN:
            return False
        if self.opened_at is not None and self.clock() - self.opened_at >= self.cooldown_seconds:
            self.state = CircuitState.HALF_OPEN
            return True
        return False

    def record_success(self) -> None:
        """Close the circuit after any successful batch or probe."""
        self.state = CircuitState.CLOSED
        self.consecutive_failures = 0
        self.opened_at = None

    def record_failure(self) -> None:
        """Count a failure and open the circuit at the configured threshold."""
        self.consecutive_failures += 1
        if (
            self.state is CircuitState.HALF_OPEN
            or self.consecutive_failures >= self.failure_threshold
        ):
            self.state = CircuitState.OPEN
            self.opened_at = self.clock()
