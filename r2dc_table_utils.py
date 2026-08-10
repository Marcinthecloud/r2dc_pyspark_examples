"""Shared helpers for inspecting Iceberg table properties."""


def get_table_format_version(spark, table_name):
    """Return an Iceberg table's format version as an integer."""
    properties = {
        row.key: row.value
        for row in spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
    }
    try:
        return int(properties["format-version"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(
            f"Could not determine Iceberg format version for table '{table_name}'"
        ) from exc
