"""Pipeline monitoring and metrics collection for patchwork ETL.

Provides lightweight hooks for tracking pipeline execution stats,
step-level timing, and error counts without external dependencies.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional


@dataclass
class StepMetrics:
    """Metrics collected for a single pipeline step."""

    step_name: str
    start_time: float = field(default_factory=time.monotonic)
    end_time: Optional[float] = None
    records_in: int = 0
    records_out: int = 0
    error_count: int = 0
    retry_count: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> Optional[float]:
        """Wall-clock duration of the step in seconds."""
        if self.end_time is None:
            return None
        return self.end_time - self.start_time

    @property
    def success(self) -> bool:
        """True if the step completed without errors."""
        return self.end_time is not None and self.error_count == 0

    def finish(self) -> None:
        """Mark the step as finished."""
        self.end_time = time.monotonic()

    def __repr__(self) -> str:
        dur = f"{self.duration_seconds:.3f}s" if self.duration_seconds is not None else "running"
        return (
            f"StepMetrics(step={self.step_name!r}, duration={dur}, "
            f"in={self.records_in}, out={self.records_out}, errors={self.error_count})"
        )


class PipelineMonitor:
    """Collects and exposes runtime metrics for a pipeline run.

    Usage::

        monitor = PipelineMonitor(pipeline_name="my_pipeline")
        monitor.on_step_start("extract")
        # ... run step ...
        monitor.on_step_end("extract", records_in=0, records_out=150)
        summary = monitor.summary()
    """

    def __init__(
        self,
        pipeline_name: str,
        on_event: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> None:
        """
        Args:
            pipeline_name: Human-readable name for the pipeline being monitored.
            on_event: Optional callback invoked with (event_name, payload) for
                      every monitoring event (useful for shipping to external systems).
        """
        self.pipeline_name = pipeline_name
        self._on_event = on_event
        self._started_at: Optional[datetime] = None
        self._finished_at: Optional[datetime] = None
        self._steps: Dict[str, StepMetrics] = {}
        self._step_order: List[str] = []
        self._counters: Dict[str, int] = defaultdict(int)

    # ------------------------------------------------------------------
    # Lifecycle hooks
    # ------------------------------------------------------------------

    def on_pipeline_start(self) -> None:
        """Call once before the pipeline begins execution."""
        self._started_at = datetime.utcnow()
        self._emit("pipeline_start", {"pipeline": self.pipeline_name})

    def on_pipeline_end(self, success: bool = True) -> None:
        """Call once after the pipeline finishes (or fails)."""
        self._finished_at = datetime.utcnow()
        self._emit(
            "pipeline_end",
            {"pipeline": self.pipeline_name, "success": success, "summary": self.summary()},
        )

    def on_step_start(self, step_name: str) -> None:
        """Call before a step begins processing."""
        metrics = StepMetrics(step_name=step_name)
        self._steps[step_name] = metrics
        self._step_order.append(step_name)
        self._emit("step_start", {"step": step_name})

    def on_step_end(
        self,
        step_name: str,
        records_in: int = 0,
        records_out: int = 0,
        error_count: int = 0,
        retry_count: int = 0,
        **extra: Any,
    ) -> None:
        """Call after a step completes."""
        metrics = self._steps.get(step_name)
        if metrics is None:
            # Defensive: create a stub if on_step_start was skipped.
            metrics = StepMetrics(step_name=step_name)
            self._steps[step_name] = metrics
        metrics.finish()
        metrics.records_in = records_in
        metrics.records_out = records_out
        metrics.error_count = error_count
        metrics.retry_count = retry_count
        metrics.extra.update(extra)
        self._counters["total_records_out"] += records_out
        self._counters["total_errors"] += error_count
        self._counters["total_retries"] += retry_count
        self._emit("step_end", {"step": step_name, "metrics": repr(metrics)})

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def summary(self) -> Dict[str, Any]:
        """Return a JSON-serialisable summary of the run so far."""
        total_duration: Optional[float] = None
        if self._started_at and self._finished_at:
            total_duration = (
                self._finished_at - self._started_at
            ).total_seconds()

        return {
            "pipeline": self.pipeline_name,
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "finished_at": self._finished_at.isoformat() if self._finished_at else None,
            "duration_seconds": total_duration,
            "steps": [
                {
                    "name": name,
                    "duration_seconds": self._steps[name].duration_seconds,
                    "records_in": self._steps[name].records_in,
                    "records_out": self._steps[name].records_out,
                    "error_count": self._steps[name].error_count,
                    "retry_count": self._steps[name].retry_count,
                    "success": self._steps[name].success,
                }
                for name in self._step_order
            ],
            "totals": dict(self._counters),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _emit(self, event: str, payload: Dict[str, Any]) -> None:
        """Fire the optional user-supplied event callback."""
        if self._on_event is not None:
            try:
                self._on_event(event, payload)
            except Exception:  # noqa: BLE001 — never let monitoring break the pipeline
                pass
