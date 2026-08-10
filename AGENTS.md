# Contributor Guide

## Stack

- Python 3.9 or newer
- PySpark 4.0 with Scala 2.13
- Apache Iceberg 1.11
- Java 17 or 21
- Cloudflare R2 Data Catalog

## Validation

Run syntax and whitespace checks before submitting changes:

```bash
python3 -m compileall -q .
git diff --check
```

Spark 4 can be validated locally with the Iceberg runtime package configured in
`r2dc_spark_config.py.example`. Remote catalog checks require valid R2 Data
Catalog credentials in the ignored `r2dc_spark_config.py` file.

## Constraints

- Iceberg v2-to-v3 upgrades are metadata-only, irreversible operations.
- Row lineage metadata is available only on Iceberg v3 tables.
- Existing v2 data may initially have null row IDs after a v3 upgrade.
- Use `pyspark.sql.functions.partitioning` for Spark 4 WriterV2 transforms.
- PySpark 4 WriterV2 does not expose Iceberg's `truncate` partition transform.
- Keep direct S3 credentials out of tracked files.
