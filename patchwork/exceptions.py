"""
Central exceptions module for patchwork-etl.

Provides a unified exception hierarchy so that callers can catch
patchwork-specific errors at whatever granularity they need:

    except PatchworkError:          # catch anything from the framework
    except ExtractionError:         # only extraction failures
    except TransformationError:     # only transformation failures
    ...
"""


class PatchworkError(Exception):
    """Base class for all patchwork exceptions."""

    def __init__(self, message: str, *, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.message = message
        # Preserve the original exception for debugging / logging
        self.__cause__ = cause

    def __str__(self) -> str:  # noqa: D105
        if self.__cause__:
            return f"{self.message} (caused by {type(self.__cause__).__name__}: {self.__cause__})"
        return self.message


# ---------------------------------------------------------------------------
# Pipeline lifecycle
# ---------------------------------------------------------------------------


class PipelineConfigError(PatchworkError):
    """Raised when a pipeline or step is misconfigured before execution."""


class PipelineAbortedError(PatchworkError):
    """Raised when a pipeline is explicitly aborted mid-run."""


# ---------------------------------------------------------------------------
# Step-level errors
# ---------------------------------------------------------------------------


class StepError(PatchworkError):
    """Base class for errors that occur within a pipeline step."""

    def __init__(
        self,
        message: str,
        *,
        step_name: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message, cause=cause)
        self.step_name = step_name

    def __str__(self) -> str:  # noqa: D105
        prefix = f"[step={self.step_name}] " if self.step_name else ""
        base = super().__str__()
        return f"{prefix}{base}"


class ExtractionError(StepError):
    """Raised when data cannot be read from a source."""


class TransformationError(StepError):
    """Raised when a transformation cannot be applied to the data."""


class LoadError(StepError):
    """Raised when data cannot be written to a destination."""


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class SchemaValidationError(PatchworkError):
    """Raised when a record fails schema validation.

    Attributes
    ----------
    field:
        The name of the field that failed validation, if applicable.
    value:
        The offending value, if available.
    """

    def __init__(
        self,
        message: str,
        *,
        field: str | None = None,
        value: object = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message, cause=cause)
        self.field = field
        self.value = value

    def __str__(self) -> str:  # noqa: D105
        parts = [self.message]
        if self.field is not None:
            parts.append(f"field={self.field!r}")
        if self.value is not None:
            parts.append(f"value={self.value!r}")
        detail = ", ".join(parts[1:])
        base = parts[0]
        return f"{base} ({detail})" if detail else base


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------


class RetryExhaustedError(PatchworkError):
    """Raised when all retry attempts for an operation have been exhausted.

    Attributes
    ----------
    attempts:
        Total number of attempts that were made.
    """

    def __init__(
        self,
        message: str,
        *,
        attempts: int = 0,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message, cause=cause)
        self.attempts = attempts

    def __str__(self) -> str:  # noqa: D105
        base = super().__str__()
        return f"{base} (attempts={self.attempts})"
