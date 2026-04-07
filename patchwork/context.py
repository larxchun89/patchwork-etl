"""Execution context for sharing state across pipeline steps.

Provides a thread-safe context object that allows steps within a pipeline
to share metadata, configuration, and intermediate results without tight
coupling between components.
"""

import threading
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional


class PipelineContext:
    """Thread-safe context object passed through pipeline execution.

    Stores arbitrary key-value metadata that extractors, transformers,
    and loaders can read and write during a pipeline run. Also tracks
    basic execution metadata like start time and run ID.

    Example::

        ctx = PipelineContext(run_id="run-001", env="production")
        ctx.set("source_row_count", 1500)
        count = ctx.get("source_row_count")  # 1500
    """

    def __init__(self, run_id: Optional[str] = None, **initial_values: Any) -> None:
        """Initialise context with an optional run ID and seed values.

        Args:
            run_id: Unique identifier for this pipeline run. Auto-generated
                    from the current UTC timestamp when not provided.
            **initial_values: Arbitrary key-value pairs to pre-populate
                              the context store.
        """
        self._lock = threading.Lock()
        self._store: Dict[str, Any] = {}
        self.run_id: str = run_id or self._generate_run_id()
        self.created_at: datetime = datetime.now(timezone.utc)

        for key, value in initial_values.items():
            self._store[key] = value

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set(self, key: str, value: Any) -> None:
        """Store a value under *key*, overwriting any existing entry."""
        with self._lock:
            self._store[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve the value for *key*, returning *default* if absent."""
        with self._lock:
            return self._store.get(key, default)

    def require(self, key: str) -> Any:
        """Return the value for *key* or raise ``KeyError`` if missing.

        Use this when a step cannot proceed without a particular context
        value being set by an earlier stage.

        Raises:
            KeyError: If *key* is not present in the context store.
        """
        with self._lock:
            if key not in self._store:
                raise KeyError(
                    f"Required context key '{key}' is not set. "
                    "Ensure an earlier pipeline step populates this value."
                )
            return self._store[key]

    def update(self, mapping: Dict[str, Any]) -> None:
        """Merge *mapping* into the context store in a single lock acquisition."""
        with self._lock:
            self._store.update(mapping)

    def delete(self, key: str) -> None:
        """Remove *key* from the store; silently ignored if absent."""
        with self._lock:
            self._store.pop(key, None)

    def keys(self) -> Iterator[str]:
        """Return a snapshot of the current context keys."""
        with self._lock:
            return iter(list(self._store.keys()))

    def as_dict(self) -> Dict[str, Any]:
        """Return a shallow copy of the current context store."""
        with self._lock:
            return dict(self._store)

    def clear(self) -> None:
        """Remove all user-set values from the store.

        The ``run_id`` and ``created_at`` attributes are preserved.
        """
        with self._lock:
            self._store.clear()

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __contains__(self, key: str) -> bool:
        with self._lock:
            return key in self._store

    def __repr__(self) -> str:
        return (
            f"PipelineContext(run_id={self.run_id!r}, "
            f"keys={list(self._store.keys())!r})"
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _generate_run_id() -> str:
        """Generate a simple timestamp-based run identifier."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        return f"run-{ts}"
