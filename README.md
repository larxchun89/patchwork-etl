# patchwork-etl

> Lightweight Python framework for building modular ETL pipelines with built-in retry logic and schema validation.

---

## Installation

```bash
pip install patchwork-etl
```

Or with optional dependencies:

```bash
pip install patchwork-etl[all]
```

---

## Usage

```python
from patchwork import Pipeline, Extract, Transform, Load
from patchwork.validators import Schema

# Define your schema
schema = Schema({
    "id": int,
    "name": str,
    "value": float
})

# Build a pipeline
pipeline = (
    Pipeline(name="sales-etl")
    .extract(Extract.from_csv("data/input.csv"))
    .validate(schema)
    .transform(lambda row: {**row, "value": round(row["value"], 2)})
    .load(Load.to_postgres(dsn="postgresql://user:pass@localhost/db"))
)

# Run with built-in retry logic
pipeline.run(retries=3, backoff=2.0)
```

Pipelines are composable and each stage can be tested independently:

```python
# Test a single stage
result = pipeline.dry_run(limit=10)
print(result.summary())
```

---

## Features

- 🧩 **Modular stages** — mix and match extract, transform, and load components
- 🔁 **Retry logic** — configurable retries with exponential backoff
- ✅ **Schema validation** — catch bad data early before it hits your destination
- 🪶 **Lightweight** — minimal dependencies, easy to integrate

---

## License

MIT © [patchwork-etl contributors](LICENSE)