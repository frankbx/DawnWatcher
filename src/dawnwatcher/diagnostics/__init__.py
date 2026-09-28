"""Opt-in diagnostics that are kept separate from the production collector."""

from dawnwatcher.diagnostics.comparison import (
    DiagnosticComparisonRunner,
    DiagnosticComparisonSummary,
)

__all__ = ["DiagnosticComparisonRunner", "DiagnosticComparisonSummary"]
