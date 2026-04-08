"""Built-in extractor classes for common data source patterns.

Extractors are the first stage of an ETL pipeline responsible for
reading raw data from various sources and yielding records downstream.
"""

import csv
import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, Iterator, List, Optional, Union

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Abstract base class for all extractors.

    Subclasses must implement the ``extract`` method which should
    return an iterable of records (dicts by convention).
    """

    def __init__(self, name: Optional[str] = None) -> None:
        self.name = name or self.__class__.__name__

    @abstractmethod
    def extract(self) -> Iterable[Dict[str, Any]]:
        """Yield records from the data source."""
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"


class CSVExtractor(BaseExtractor):
    """Extract records from a CSV file.

    Args:
        filepath: Path to the CSV file.
        delimiter: Field delimiter character (default: ',').
        encoding: File encoding (default: 'utf-8').
        skip_blank_lines: Whether to ignore blank rows (default: True).
        name: Optional human-readable name for this extractor.
    """

    def __init__(
        self,
        filepath: Union[str, Path],
        delimiter: str = ",",
        encoding: str = "utf-8",
        skip_blank_lines: bool = True,
        name: Optional[str] = None,
    ) -> None:
        super().__init__(name=name)
        self.filepath = Path(filepath)
        self.delimiter = delimiter
        self.encoding = encoding
        self.skip_blank_lines = skip_blank_lines

    def extract(self) -> Generator[Dict[str, Any], None, None]:
        """Yield rows from the CSV file as dictionaries."""
        if not self.filepath.exists():
            raise FileNotFoundError(f"CSV file not found: {self.filepath}")

        logger.debug("Extracting from CSV: %s", self.filepath)
        with self.filepath.open(encoding=self.encoding, newline="") as fh:
            reader = csv.DictReader(fh, delimiter=self.delimiter)
            for row in reader:
                if self.skip_blank_lines and not any(row.values()):
                    continue
                yield dict(row)


class JSONLExtractor(BaseExtractor):
    """Extract records from a newline-delimited JSON (JSONL) file.

    Each line in the file must be a valid JSON object.

    Args:
        filepath: Path to the JSONL file.
        encoding: File encoding (default: 'utf-8').
        name: Optional human-readable name for this extractor.
    """

    def __init__(
        self,
        filepath: Union[str, Path],
        encoding: str = "utf-8",
        name: Optional[str] = None,
    ) -> None:
        super().__init__(name=name)
        self.filepath = Path(filepath)
        self.encoding = encoding

    def extract(self) -> Generator[Dict[str, Any], None, None]:
        """Yield records from the JSONL file as dictionaries.

        Raises:
            FileNotFoundError: If the file does not exist.
            json.JSONDecodeError: If a line contains invalid JSON, the error
                is logged with the offending line number and the line is skipped.
        """
        if not self.filepath.exists():
            raise FileNotFoundError(f"JSONL file not found: {self.filepath}")

        logger.debug("Extracting from JSONL: %s", self.filepath)
        with self.filepath.open(encoding=self.encoding) as fh:
            for lineno, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    logger.warning(
                        "Skipping invalid JSON on line %d of %s: %s",
                        lineno,
                        self.filepath,
                        exc,
                    )
