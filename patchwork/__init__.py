"""Patchwork ETL - Lightweight Python framework for building modular ETL pipelines.

Provides core abstractions for Extract, Transform, and Load operations
with built-in retry logic and schema validation.
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
]
