"""Inspect Iceberg v3 row lineage metadata."""

import argparse
import json

from r2dc_spark_config import get_spark_session
from r2dc_table_utils import get_table_format_version

MAX_LINEAGE_ROWS = 10000


def inspect_row_lineage(spark, table_name, where_clause=None, limit=20):
    """Return row lineage summary information and a sample DataFrame."""
    if not isinstance(limit, int) or not 1 <= limit <= MAX_LINEAGE_ROWS:
        raise ValueError(
            f"Lineage row limit must be between 1 and {MAX_LINEAGE_ROWS}"
        )

    format_version = get_table_format_version(spark, table_name)
    if format_version != 3:
        raise ValueError(
            f"Row lineage requires Iceberg format version 3; table '{table_name}' "
            f"is format version {format_version}"
        )

    where_sql = f" WHERE {where_clause}" if where_clause else ""
    summary = spark.sql(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(_row_id) AS rows_with_lineage,
            COUNT(DISTINCT _row_id) AS distinct_row_ids,
            COUNT(*) - COUNT(_row_id) AS null_row_ids
        FROM {table_name}{where_sql}
        """
    ).first().asDict()
    summary["duplicate_row_ids"] = (
        summary["rows_with_lineage"] - summary["distinct_row_ids"]
    )

    rows = spark.sql(
        f"""
        SELECT *, _row_id, _last_updated_sequence_number
        FROM {table_name}{where_sql}
        ORDER BY _row_id
        LIMIT {limit}
        """
    )
    return summary, rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect Iceberg v3 row lineage metadata")
    parser.add_argument("table", help="Fully qualified table name (namespace.table)")
    parser.add_argument("--where", help="Optional SQL WHERE expression")
    parser.add_argument("--limit", type=int, default=20, help="Rows to display (default: 20)")
    parser.add_argument("--json", metavar="PATH", help="Write the summary and sampled rows to JSON")
    args = parser.parse_args()

    if not 1 <= args.limit <= MAX_LINEAGE_ROWS:
        parser.error(f"--limit must be between 1 and {MAX_LINEAGE_ROWS}")

    spark = get_spark_session("R2DataCatalog-InspectRowLineage")
    try:
        summary, rows = inspect_row_lineage(
            spark, args.table, where_clause=args.where, limit=args.limit
        )
        print(f"\nRow lineage summary for '{args.table}':")
        for key, value in summary.items():
            print(f"  {key}: {value}")

        print(f"\nRow lineage sample (limit {args.limit}):")
        rows.show(args.limit, truncate=False)

        if args.json:
            output = {
                "table": args.table,
                "summary": summary,
                "rows": [row.asDict(recursive=True) for row in rows.collect()],
            }
            with open(args.json, "w", encoding="utf-8") as output_file:
                json.dump(output, output_file, indent=2, default=str)
            print(f"\nJSON results saved to: {args.json}")
    finally:
        spark.stop()
