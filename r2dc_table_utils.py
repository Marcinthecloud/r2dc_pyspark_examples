"""Shared helpers for inspecting Iceberg table properties."""

import re

from pyspark.sql import SparkSession


_TABLE_NAME_PATTERN = re.compile(
    r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$"
)


def validate_table_name(table_name: str) -> str:
    """Validate and return an unquoted, optionally qualified table name."""
    if (
        not isinstance(table_name, str)
        or not _TABLE_NAME_PATTERN.fullmatch(table_name)
    ):
        raise ValueError(
            "Table name must contain only letters, numbers, underscores, and dots, "
            "with each segment starting with a letter or underscore"
        )
    return table_name


def get_table_format_version(spark: SparkSession, table_name: str) -> int:
    """Return an Iceberg table's format version as an integer."""
    table_name = validate_table_name(table_name)
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
