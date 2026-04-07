"""Built-in transformer classes for common ETL transformation tasks."""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

from .validators import SchemaValidator, ValidationError


class TransformError(Exception):
    """Raised when a transformation fails."""

    def __init__(self, message: str, record: Optional[Dict] = None):
        self.record = record
        super().__init__(message)


class BaseTransformer(ABC):
    """Abstract base class for all transformers."""

    def __init__(self, name: Optional[str] = None):
        self.name = name or self.__class__.__name__

    @abstractmethod
    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform a list of records and return the transformed list."""
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"


class FieldMapper(BaseTransformer):
    """Renames fields according to a mapping dictionary.

    Example::

        mapper = FieldMapper({"first_name": "fname", "last_name": "lname"})
        records = mapper.transform([{"first_name": "Ada", "last_name": "Lovelace"}])
        # [{"fname": "Ada", "lname": "Lovelace"}]
    """

    def __init__(self, mapping: Dict[str, str], name: Optional[str] = None):
        super().__init__(name)
        self.mapping = mapping

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        result = []
        for record in records:
            new_record = {}
            for key, value in record.items():
                new_key = self.mapping.get(key, key)
                new_record[new_key] = value
            result.append(new_record)
        return result


class FieldFilter(BaseTransformer):
    """Keeps only the specified fields in each record.

    Example::

        filt = FieldFilter(keep=["id", "name"])
        records = filt.transform([{"id": 1, "name": "Alice", "age": 30}])
        # [{"id": 1, "name": "Alice"}]
    """

    def __init__(self, keep: List[str], name: Optional[str] = None):
        super().__init__(name)
        self.keep = set(keep)

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [{k: v for k, v in record.items() if k in self.keep} for record in records]


class ValueTransformer(BaseTransformer):
    """Applies a callable to a specific field's value in every record.

    Example::

        upper = ValueTransformer("name", str.upper)
        records = upper.transform([{"name": "alice"}])
        # [{"name": "ALICE"}]
    """

    def __init__(
        self,
        field: str,
        func: Callable[[Any], Any],
        skip_missing: bool = True,
        name: Optional[str] = None,
    ):
        super().__init__(name)
        self.field = field
        self.func = func
        self.skip_missing = skip_missing

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        result = []
        for record in records:
            if self.field not in record:
                if self.skip_missing:
                    result.append(record)
                    continue
                raise TransformError(
                    f"Field '{self.field}' not found in record", record=record
                )
            new_record = {**record, self.field: self.func(record[self.field])}
            result.append(new_record)
        return result


class RecordFilter(BaseTransformer):
    """Drops records that do not satisfy a predicate function.

    Example::

        positive = RecordFilter(lambda r: r["amount"] > 0)
        records = positive.transform([{"amount": 5}, {"amount": -1}])
        # [{"amount": 5}]
    """

    def __init__(self, predicate: Callable[[Dict[str, Any]], bool], name: Optional[str] = None):
        super().__init__(name)
        self.predicate = predicate

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [record for record in records if self.predicate(record)]


class SchemaTransformer(BaseTransformer):
    """Validates records against a SchemaValidator and drops (or raises on) invalid ones."""

    def __init__(
        self,
        validator: "SchemaValidator",
        on_error: str = "drop",
        name: Optional[str] = None,
    ):
        """
        Args:
            validator: A SchemaValidator instance to validate each record.
            on_error: Either ``'drop'`` to silently discard invalid records or
                ``'raise'`` to propagate the ValidationError.
        """
        super().__init__(name)
        if on_error not in ("drop", "raise"):
            raise ValueError("on_error must be 'drop' or 'raise'")
        self.validator = validator
        self.on_error = on_error

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        result = []
        for record in records:
            try:
                self.validator.validate(record)
                result.append(record)
            except ValidationError:
                if self.on_error == "raise":
                    raise
                # on_error == 'drop': silently skip the invalid record
        return result
