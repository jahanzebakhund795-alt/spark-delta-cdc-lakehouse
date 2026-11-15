# Spark Delta CDC Lakehouse

A newly built portfolio implementation for Jahanzeb Akhund, aligned with the data architecture and engineering work described in his resume. All example data is synthetic. This repository is independent sample work; it does not contain employer code, claim a historical deployment, or reproduce resume performance metrics.

## Implemented system

A real PySpark/Delta job writes bronze and quarantine tables, detects conflicting versions, uses a window to select source updates, performs a conditional Delta MERGE into silver, and publishes regional gold aggregates. Durable tombstones preserve source versions and prevent stale CDC updates from resurrecting deleted orders. A separate reference implementation defines the merge contract and enables quick deterministic regression tests.

[Architecture diagram and design review](docs/ARCHITECTURE.md)

## Run

Use Python 3.11 for the documented development environment.

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[test]"
pytest -q
python -m cdc_lakehouse.demo
# Full Spark/Delta integration requires Java 17:
pip install -r requirements-spark.txt
sh scripts/integration.sh
```

## Design and operating limits

Spark dependencies are separate because they require Java and download Delta JARs. Local reference tests do not prove Spark runtime execution. Bronze and quarantine are append-only delivery logs and can contain replay duplicates; silver is idempotent by source key/version. Tables commit independently, so recovery replays a retained batch to finish gold publication. This bounded job is designed for batch CDC, not an implemented streaming connector.

See [the operating guide](docs/OPERATIONS.md) for recovery, deployment, and validation boundaries. An optional GitHub Actions definition is in `docs/ci-template.yml`; enabling it requires installing it under `.github/workflows/` with workflow permission.

## References

- [Delta Python API](https://docs.delta.io/api/latest/python/spark/)
- [Spark window functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)

## Executed validation

3 pytest tests passed. Reference CDC tombstones, replay, version conflicts, and malformed-row quarantine; the reference batch demo was executed.

[Full validation evidence and limits](VALIDATION.md)
