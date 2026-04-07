"""Loaders for writing transformed data to various destinations.

Provides base and concrete loader implementations for the patchwork ETL
framework. Loaders are the final step in a pipeline, responsible for
persisting or forwarding processed records.
"""

import csv
import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class LoadError(Exception):
    """Raised when a loader fails to write data to the destination."""

    def __init__(self, message: str, destination: str, cause: Optional[Exception] = None):
        self.destination = destination
        self.cause = cause
        super().__init__(message)

    def __str__(self):
        base = super().__str__()
        if self.cause:
            return f"{base} (destination={self.destination!r}, caused by: {self.cause})"
        return f"{base} (destination={self.destination!r})"


class BaseLoader(ABC):
    """Abstract base class for all loaders."""

    def __init__(self, destination: str):
        """
        Args:
            destination: A string identifier for the load target (e.g. file path, table name, URL).
        """
        self.destination = destination
        self._records_written = 0

    @abstractmethod
    def load(self, records: List[Dict[str, Any]]) -> int:
        """Write records to the destination.

        Args:
            records: List of record dicts to persist.

        Returns:
            Number of records successfully written.

        Raises:
            LoadError: If writing fails.
        """

    @property
    def records_written(self) -> int:
        """Total records written across all load() calls."""
        return self._records_written

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(destination={self.destination!r})"


class CSVLoader(BaseLoader):
    """Writes records to a CSV file."""

    def __init__(
        self,
        destination: str,
        fieldnames: Optional[List[str]] = None,
        delimiter: str = ",",
        append: bool = False,
        encoding: str = "utf-8",
    ):
        """
        Args:
            destination: Path to the output CSV file.
            fieldnames: Column names. If None, inferred from the first record.
            delimiter: Field delimiter character.
            append: If True, append to an existing file instead of overwriting.
            encoding: File encoding.
        """
        super().__init__(destination)
        self.fieldnames = fieldnames
        self.delimiter = delimiter
        self.append = append
        self.encoding = encoding

    def load(self, records: List[Dict[str, Any]]) -> int:
        if not records:
            logger.warning("CSVLoader received empty record list; nothing written.")
            return 0

        fieldnames = self.fieldnames or list(records[0].keys())
        mode = "a" if self.append else "w"
        write_header = not (self.append and os.path.exists(self.destination))

        try:
            with open(self.destination, mode, newline="", encoding=self.encoding) as fh:
                writer = csv.DictWriter(
                    fh,
                    fieldnames=fieldnames,
                    delimiter=self.delimiter,
                    extrasaction="ignore",
                )
                if write_header:
                    writer.writeheader()
                writer.writerows(records)

            self._records_written += len(records)
            logger.info("CSVLoader wrote %d records to %r", len(records), self.destination)
            return len(records)
        except OSError as exc:
            raise LoadError(f"Failed to write CSV file: {exc}", self.destination, cause=exc) from exc


class JSONLLoader(BaseLoader):
    """Writes records to a JSON Lines (.jsonl) file, one JSON object per line."""

    def __init__(self, destination: str, append: bool = False, encoding: str = "utf-8"):
        """
        Args:
            destination: Path to the output JSONL file.
            append: If True, append to an existing file.
            encoding: File encoding.
        """
        super().__init__(destination)
        self.append = append
        self.encoding = encoding

    def load(self, records: List[Dict[str, Any]]) -> int:
        if not records:
            logger.warning("JSONLLoader received empty record list; nothing written.")
            return 0

        mode = "a" if self.append else "w"
        try:
            with open(self.destination, mode, encoding=self.encoding) as fh:
                for record in records:
                    fh.write(json.dumps(record, default=str) + "\n")

            self._records_written += len(records)
            logger.info("JSONLLoader wrote %d records to %r", len(records), self.destination)
            return len(records)
        except OSError as exc:
            raise LoadError(f"Failed to write JSONL file: {exc}", self.destination, cause=exc) from exc


class InMemoryLoader(BaseLoader):
    """Accumulates records in memory. Useful for testing and pipeline inspection."""

    def __init__(self):
        super().__init__(destination="memory")
        self.data: List[Dict[str, Any]] = []

    def load(self, records: List[Dict[str, Any]]) -> int:
        self.data.extend(records)
        self._records_written += len(records)
        logger.debug("InMemoryLoader stored %d records (total=%d)", len(records), self._records_written)
        return len(records)

    def clear(self):
        """Reset stored records and write counter."""
        self.data.clear()
        self._records_written = 0
