"""Patchwork ETL - Lightweight Python framework for building modular ETL pipelines.

Provides core abstractions for Extract, Transform, and Load operations
with built-in retry logic and schema validation.

Example usage::

    from patchwork import Pipeline, ExtractStep, TransformStep, LoadStep

    pipeline = Pipeline([
        ExtractStep(source),
        TransformStep(transform_fn),
        LoadStep(destination),
    ])
    pipeline.run()
"""

from patchwork.pipeline import Pipeline
from patchwork.steps import ExtractStep, TransformStep, LoadStep
from patchwork.exceptions import (
    PatchworkError,
    ExtractionError,
    TransformationError,
    LoadError,
    SchemaValidationError,
    RetryExhaustedError,
)

__version__ = "0.1.0"
__author__ = "Patchwork ETL Contributors"
__all__ = [
    "Pipeline",
    "ExtractStep",
    "TransformStep",
    "LoadStep",
    "PatchworkError",
    "ExtractionError",
    "TransformationError",
    "LoadError",
    "SchemaValidationError",
    "RetryExhaustedError",
    "get_version",
]


def get_version() -> str:
    """Return the current version of the patchwork-etl package.

    Returns:
        str: The version string in ``MAJOR.MINOR.PATCH`` format.

    Example::

        >>> import patchwork
        >>> patchwork.get_version()
        '0.1.0'
    """
    return __version__
