"""Opt-in diagnostics that are kept separate from the production collector."""

from regimebeacon.diagnostics.comparison import (
    DiagnosticComparisonRunner,
    DiagnosticComparisonSummary,
)

__all__ = ["DiagnosticComparisonRunner", "DiagnosticComparisonSummary"]
