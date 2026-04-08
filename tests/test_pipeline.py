"""Tests for the core Pipeline and Step classes."""

import pytest
from unittest.mock import MagicMock, patch

from patchwork.pipeline import Step, Pipeline
from patchwork.context import PipelineContext
from patchwork.exceptions import PipelineConfigError, PipelineAbortedError


class TestStep:
    """Tests for the Step class."""

    def test_step_creation_with_callable(self):
        """A Step can be created with a name and callable."""
        fn = lambda ctx: ctx
        step = Step(name="my_step", fn=fn)
        assert step.name == "my_step"
        assert step.fn is fn

    def test_step_execute_calls_fn_with_context(self):
        """Step.execute passes the context to the wrapped function."""
        ctx = PipelineContext()
        ctx.set("key", "value")

        result_holder = {}

        def fn(context):
            result_holder["seen"] = context.get("key")
            return context

        step = Step(name="check_step", fn=fn)
        step.execute(ctx)

        assert result_holder["seen"] == "value"

    def test_step_execute_returns_context(self):
        """Step.execute returns the context after running."""
        ctx = PipelineContext()
        step = Step(name="passthrough", fn=lambda c: c)
        returned = step.execute(ctx)
        assert returned is ctx

    def test_step_repr_includes_name(self):
        """Step __repr__ includes the step name."""
        step = Step(name="transform_data", fn=lambda c: c)
        assert "transform_data" in repr(step)


class TestPipeline:
    """Tests for the Pipeline class."""

    def _make_pipeline(self, name="test_pipeline"):
        return Pipeline(name=name)

    def test_pipeline_creation(self):
        """A Pipeline can be created with a name."""
        p = self._make_pipeline()
        assert p.name == "test_pipeline"

    def test_pipeline_add_step(self):
        """Steps can be added to a pipeline."""
        p = self._make_pipeline()
        step = Step(name="step_one", fn=lambda c: c)
        p.add_step(step)
        assert len(p.steps) == 1
        assert p.steps[0] is step

    def test_pipeline_add_multiple_steps_preserves_order(self):
        """Steps are executed in the order they are added."""
        p = self._make_pipeline()
        order = []

        for i in range(3):
            idx = i  # capture loop variable
            p.add_step(Step(name=f"step_{idx}", fn=lambda c, i=idx: order.append(i) or c))

        ctx = PipelineContext()
        p.run(ctx)

        assert order == [0, 1, 2]

    def test_pipeline_run_returns_context(self):
        """Pipeline.run returns the final context."""
        p = self._make_pipeline()
        p.add_step(Step(name="noop", fn=lambda c: c))
        ctx = PipelineContext()
        result = p.run(ctx)
        assert result is ctx

    def test_pipeline_run_with_no_steps(self):
        """Pipeline.run with no steps returns the context unchanged."""
        p = self._make_pipeline()
        ctx = PipelineContext()
        ctx.set("untouched", True)
        result = p.run(ctx)
        assert result.get("untouched") is True

    def test_pipeline_step_can_mutate_context(self):
        """A step can write data to the context for downstream steps."""
        p = self._make_pipeline()

        def writer(ctx):
            ctx.set("written", 42)
            return ctx

        def reader(ctx):
            ctx.set("read", ctx.get("written") * 2)
            return ctx

        p.add_step(Step(name="writer", fn=writer))
        p.add_step(Step(name="reader", fn=reader))

        ctx = PipelineContext()
        result = p.run(ctx)

        assert result.get("read") == 84

    def test_pipeline_aborts_on_step_exception(self):
        """Pipeline.run raises PipelineAbortedError when a step raises."""
        p = self._make_pipeline()

        def bad_step(ctx):
            raise ValueError("something went wrong")

        p.add_step(Step(name="bad", fn=bad_step))

        ctx = PipelineContext()
        with pytest.raises((PipelineAbortedError, ValueError)):
            p.run(ctx)

    def test_pipeline_repr_includes_name(self):
        """Pipeline __repr__ includes the pipeline name."""
        p = self._make_pipeline(name="my_etl")
        assert "my_etl" in repr(p)
