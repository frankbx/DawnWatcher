"""Circuit breaker failure and recovery-probe tests."""

from __future__ import annotations

from regimebeacon.providers.circuit_breaker import CircuitBreaker, CircuitState


def test_circuit_opens_and_allows_one_probe_after_cooldown() -> None:
    current_time = [100.0]
    breaker = CircuitBreaker(
        failure_threshold=2,
        cooldown_seconds=10,
        clock=lambda: current_time[0],
    )

    assert breaker.allow_request() is True
    breaker.record_failure()
    assert breaker.state is CircuitState.CLOSED
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN
    assert breaker.allow_request() is False

    current_time[0] += 10
    assert breaker.allow_request() is True
    assert breaker.state is CircuitState.HALF_OPEN
    assert breaker.allow_request() is False

    breaker.record_success()
    assert breaker.state is CircuitState.CLOSED
    assert breaker.allow_request() is True
