"""Pipeline runners for executing ETL pipelines with various execution strategies."""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from .pipeline import Pipeline
from .retry import RetryConfig

logger = logging.getLogger(__name__)


@dataclass
class RunResult:
    """Holds the result and metadata of a pipeline run."""

    pipeline_name: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    success: bool = False
    records_processed: int = 0
    errors: List[str] = field(default_factory=list)
    step_timings: Dict[str, float] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> Optional[float]:
        """Total wall-clock duration of the run in seconds."""
        if self.finished_at is None:
            return None
        return (self.finished_at - self.started_at).total_seconds()

    def __str__(self) -> str:
        status = "SUCCESS" if self.success else "FAILED"
        duration = f"{self.duration_seconds:.2f}s" if self.duration_seconds is not None else "N/A"
        return (
            f"RunResult({self.pipeline_name!r}, status={status}, "
            f"records={self.records_processed}, duration={duration})"
        )


class PipelineRunner:
    """Executes a single pipeline and captures run metadata.

    Example::

        runner = PipelineRunner(pipeline)
        result = runner.run(input_data)
        print(result)
    """

    def __init__(self, pipeline: Pipeline, retry_config: Optional[RetryConfig] = None) -> None:
        self.pipeline = pipeline
        self.retry_config = retry_config or RetryConfig()
        self._result: Optional[RunResult] = None

    @property
    def last_result(self) -> Optional[RunResult]:
        """The result of the most recent run, or None if never executed."""
        return self._result

    def run(self, data: Any = None) -> RunResult:
        """Execute the pipeline and return a RunResult.

        Args:
            data: Optional initial data to pass into the first pipeline step.

        Returns:
            A populated RunResult instance.
        """
        result = RunResult(
            pipeline_name=self.pipeline.name,
            started_at=datetime.utcnow(),
        )

        logger.info("Starting pipeline %r", self.pipeline.name)

        try:
            step_start = time.perf_counter()
            output = self.pipeline.run(data)
            step_elapsed = time.perf_counter() - step_start

            result.step_timings["total"] = round(step_elapsed, 4)
            result.records_processed = len(output) if hasattr(output, "__len__") else 1
            result.success = True
            logger.info(
                "Pipeline %r finished in %.2fs, %d record(s) processed.",
                self.pipeline.name,
                step_elapsed,
                result.records_processed,
            )
        except Exception as exc:  # noqa: BLE001
            result.errors.append(str(exc))
            logger.error("Pipeline %r failed: %s", self.pipeline.name, exc)
        finally:
            result.finished_at = datetime.utcnow()

        self._result = result
        return result


class BatchRunner:
    """Runs multiple pipelines sequentially and collects all results.

    Example::

        batch = BatchRunner([pipeline_a, pipeline_b])
        results = batch.run_all()
        for r in results:
            print(r)
    """

    def __init__(self, pipelines: List[Pipeline], stop_on_failure: bool = False) -> None:
        self.pipelines = pipelines
        self.stop_on_failure = stop_on_failure

    def run_all(self, data: Any = None) -> List[RunResult]:
        """Execute all registered pipelines.

        Args:
            data: Shared initial data forwarded to every pipeline.

        Returns:
            List of RunResult objects in execution order.
        """
        results: List[RunResult] = []

        for pipeline in self.pipelines:
            runner = PipelineRunner(pipeline)
            result = runner.run(data)
            results.append(result)

            if not result.success and self.stop_on_failure:
                logger.warning(
                    "Stopping batch early — pipeline %r failed.",
                    pipeline.name,
                )
                break

        passed = sum(1 for r in results if r.success)
        logger.info("Batch complete: %d/%d pipelines succeeded.", passed, len(results))
        return results
