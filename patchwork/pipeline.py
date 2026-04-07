"""Core pipeline orchestration module for patchwork-etl.

Provides the Pipeline class which manages the execution of ETL steps
in sequence, with built-in retry logic and error handling.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, List, Optional

from patchwork.exceptions import PipelineError, StepExecutionError

logger = logging.getLogger(__name__)


class Step:
    """Represents a single unit of work within a pipeline.

    Args:
        name: Human-readable identifier for this step.
        fn: Callable that performs the step's work. Receives the current
            context dict and returns updated context.
        retries: Number of times to retry on failure (default: 0).
        retry_delay: Seconds to wait between retry attempts (default: 1.0).
    """

    def __init__(
        self,
        name: str,
        fn: Callable[[dict], dict],
        retries: int = 0,
        retry_delay: float = 1.0,
    ) -> None:
        self.name = name
        self.fn = fn
        self.retries = retries
        self.retry_delay = retry_delay

    def execute(self, context: dict) -> dict:
        """Run this step, retrying on failure if configured.

        Args:
            context: Shared pipeline context passed between steps.

        Returns:
            Updated context after step execution.

        Raises:
            StepExecutionError: If the step fails after all retry attempts.
        """
        attempts = self.retries + 1
        last_exc: Optional[Exception] = None

        for attempt in range(1, attempts + 1):
            try:
                logger.debug("Step '%s' — attempt %d/%d", self.name, attempt, attempts)
                result = self.fn(context)
                if result is not None:
                    context = result
                logger.info("Step '%s' completed successfully.", self.name)
                return context
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                logger.warning(
                    "Step '%s' failed on attempt %d/%d: %s",
                    self.name,
                    attempt,
                    attempts,
                    exc,
                )
                if attempt < attempts:
                    time.sleep(self.retry_delay)

        raise StepExecutionError(
            f"Step '{self.name}' failed after {attempts} attempt(s)."
        ) from last_exc


class Pipeline:
    """Orchestrates a sequence of ETL steps.

    Example::

        pipeline = Pipeline(name="my_pipeline")
        pipeline.add_step("extract", extract_fn, retries=2)
        pipeline.add_step("transform", transform_fn)
        pipeline.add_step("load", load_fn)
        pipeline.run()

    Args:
        name: Identifier for this pipeline, used in logging.
        initial_context: Optional starting context dict shared across steps.
    """

    def __init__(
        self,
        name: str,
        initial_context: Optional[dict] = None,
    ) -> None:
        self.name = name
        self._steps: List[Step] = []
        self._context: dict = initial_context or {}

    def add_step(
        self,
        name: str,
        fn: Callable[[dict], dict],
        retries: int = 0,
        retry_delay: float = 1.0,
    ) -> "Pipeline":
        """Append a step to the pipeline.

        Returns self to allow method chaining.
        """
        self._steps.append(Step(name=name, fn=fn, retries=retries, retry_delay=retry_delay))
        return self

    def run(self, context: Optional[dict] = None) -> dict:
        """Execute all steps in order.

        Args:
            context: Optional context to merge with the pipeline's initial
                context before execution begins.

        Returns:
            Final context dict after all steps have run.

        Raises:
            PipelineError: If any step fails and cannot be recovered.
        """
        ctx = {**self._context, **(context or {})}
        logger.info("Pipeline '%s' starting with %d step(s).", self.name, len(self._steps))

        for step in self._steps:
            try:
                ctx = step.execute(ctx)
            except StepExecutionError as exc:
                raise PipelineError(
                    f"Pipeline '{self.name}' aborted at step '{step.name}'."
                ) from exc

        logger.info("Pipeline '%s' finished successfully.", self.name)
        return ctx

    def __repr__(self) -> str:  # pragma: no cover
        return f"Pipeline(name={self.name!r}, steps={[s.name for s in self._steps]})"
