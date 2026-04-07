"""Schema validation utilities for patchwork ETL pipelines.

Provides classes and functions for validating data records against
defined schemas before and after pipeline step execution.
"""

from typing import Any, Dict, List, Optional, Type, Union
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when data fails schema validation."""

    def __init__(self, message: str, errors: Optional[List[str]] = None):
        super().__init__(message)
        self.errors = errors or []

    def __str__(self):
        if self.errors:
            error_list = "\n  - ".join(self.errors)
            return f"{super().__str__()}\n  - {error_list}"
        return super().__str__()


@dataclass
class FieldSchema:
    """Defines validation rules for a single field."""

    name: str
    dtype: Type
    required: bool = True
    nullable: bool = False
    choices: Optional[List[Any]] = None
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None

    def validate(self, value: Any) -> List[str]:
        """Validate a single value against this field's rules.

        Returns a list of error messages (empty if valid).
        """
        errors = []

        if value is None:
            if self.required and not self.nullable:
                errors.append(f"Field '{self.name}' is required and cannot be None")
            return errors

        if not isinstance(value, self.dtype):
            errors.append(
                f"Field '{self.name}' expected type {self.dtype.__name__}, "
                f"got {type(value).__name__}"
            )
            return errors  # Skip further checks if type is wrong

        if self.choices is not None and value not in self.choices:
            errors.append(
                f"Field '{self.name}' value {value!r} not in allowed choices: {self.choices}"
            )

        if self.min_value is not None and value < self.min_value:
            errors.append(
                f"Field '{self.name}' value {value} is below minimum {self.min_value}"
            )

        if self.max_value is not None and value > self.max_value:
            errors.append(
                f"Field '{self.name}' value {value} exceeds maximum {self.max_value}"
            )

        return errors


@dataclass
class Schema:
    """Defines the expected structure and types for a data record."""

    name: str
    fields: List[FieldSchema] = field(default_factory=list)
    allow_extra_fields: bool = False

    def validate_record(self, record: Dict[str, Any]) -> List[str]:
        """Validate a single record against this schema.

        Returns a list of validation error messages.
        """
        errors = []
        defined_field_names = {f.name for f in self.fields}

        # Check for missing required fields
        for schema_field in self.fields:
            if schema_field.required and schema_field.name not in record:
                errors.append(f"Missing required field: '{schema_field.name}'")
            elif schema_field.name in record:
                errors.extend(schema_field.validate(record[schema_field.name]))

        # Check for unexpected extra fields
        if not self.allow_extra_fields:
            extra = set(record.keys()) - defined_field_names
            if extra:
                errors.append(f"Unexpected fields not in schema: {sorted(extra)}")

        return errors

    def validate(self, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> None:
        """Validate one or more records, raising ValidationError on failure.

        Args:
            data: A single record dict or a list of record dicts.

        Raises:
            ValidationError: If any record fails validation.
        """
        records = data if isinstance(data, list) else [data]
        all_errors = []

        for idx, record in enumerate(records):
            record_errors = self.validate_record(record)
            if record_errors:
                for err in record_errors:
                    all_errors.append(f"[record {idx}] {err}")

        if all_errors:
            raise ValidationError(
                f"Schema '{self.name}' validation failed with {len(all_errors)} error(s)",
                errors=all_errors,
            )

        logger.debug(
            "Schema '%s' validation passed for %d record(s)", self.name, len(records)
        )
