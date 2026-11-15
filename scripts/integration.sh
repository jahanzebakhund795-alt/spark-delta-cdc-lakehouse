#!/bin/sh
set -eu
pip install -r requirements-spark.txt
python -m cdc_lakehouse.job --input fixtures/batch1.jsonl --batch-id batch1
python -m cdc_lakehouse.job --input fixtures/batch2.jsonl --batch-id batch2
python -m cdc_lakehouse.job --input fixtures/batch2.jsonl --batch-id replay
# Silver contains A as a tombstone and active B at version 2; replay must preserve both.
