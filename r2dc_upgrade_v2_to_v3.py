"""Upgrade an Iceberg table from format version 2 to version 3."""

import argparse

from pyspark.sql import SparkSession

from r2dc_spark_config import get_spark_session
from r2dc_table_utils import get_table_format_version, validate_table_name


def upgrade_v2_to_v3(
    spark: SparkSession, table_name: str, dry_run: bool = False
) -> bool:
    """Upgrade a v2 table to v3 and return whether it was changed."""
    table_name = validate_table_name(table_name)
    current_version = get_table_format_version(spark, table_name)

    if current_version == 3:
        print(f"Table '{table_name}' is already format version 3; no change needed.")
        return False
    if current_version != 2:
        raise ValueError(
            f"Table '{table_name}' is format version {current_version}; "
            "only version 2 tables can be upgraded with this utility"
        )

    if dry_run:
        print(f"Table '{table_name}' can be upgraded from format version 2 to 3.")
        return False

    spark.sql(
        f"ALTER TABLE {table_name} SET TBLPROPERTIES ('format-version' = '3')"
    ).collect()

    upgraded_version = get_table_format_version(spark, table_name)
    if upgraded_version != 3:
        raise RuntimeError(
            f"Upgrade completed without an error, but table '{table_name}' reports "
            f"format version {upgraded_version}"
        )

    print(f"Upgraded table '{table_name}' from format version 2 to 3.")
    print(
        "Existing v2 data files do not gain row IDs from this metadata-only upgrade; "
        "the lineage inspector may report null row IDs until a later snapshot assigns them."
    )
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upgrade an Iceberg table from format version 2 to version 3"
    )
    parser.add_argument("table", help="Fully qualified table name (namespace.table)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check that the table is v2 without changing it",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm the irreversible format-version upgrade",
    )
    args = parser.parse_args()

    if not args.dry_run and not args.yes:
        parser.error("the upgrade is irreversible; pass --yes to continue")

    spark = get_spark_session("R2DataCatalog-UpgradeV2ToV3")
    try:
        upgrade_v2_to_v3(spark, args.table, dry_run=args.dry_run)
    finally:
        spark.stop()
