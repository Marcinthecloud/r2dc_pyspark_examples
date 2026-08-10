"""
R2 Data Catalog - JSON to Iceberg Table
Reads JSON files from an R2 bucket and converts them into an Apache Iceberg table.
Schema is inferred from the JSON data. By default the table is partitioned by
days(__ingest_ts) for R2 SQL compatibility, but users can specify custom partition
expressions via --partition-by.
"""
from r2dc_spark_config import get_spark_session, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_ENDPOINT
import argparse
import re
import sys
from datetime import datetime
from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql.functions import partitioning as P


def build_s3a_path(bucket, prefix=None):
    """
    Builds the s3a:// path for reading JSON files from R2.

    Args:
        bucket (str): R2 bucket name
        prefix (str): Optional key prefix (folder path)

    Returns:
        str: s3a:// URI pointing to JSON files
    """
    path = f"s3a://{bucket}"
    if prefix:
        prefix = prefix.strip("/")
        path = f"{path}/{prefix}"
    # Spark reads all JSON files under the path
    return path


def read_json_from_r2(spark, s3a_path, multiline=False, sample_ratio=None):
    """
    Reads JSON files from R2 via S3A and infers the schema.

    Args:
        spark: SparkSession instance
        s3a_path (str): s3a:// URI to JSON files
        multiline (bool): Whether JSON files contain multi-line JSON objects
        sample_ratio (float): Optional sampling ratio for schema inference (0.0-1.0)

    Returns:
        DataFrame: DataFrame with inferred schema from JSON files
    """
    reader = spark.read.option("multiLine", str(multiline).lower())

    if sample_ratio is not None:
        reader = reader.option("samplingRatio", str(sample_ratio))

    print(f"Reading JSON from: {s3a_path}")
    df = reader.json(s3a_path)

    row_count = df.count()
    print(f"Read {row_count} rows")
    print(f"\nInferred schema:")
    df.printSchema()

    return df


def prepare_dataframe(df, timestamp_col=None):
    """
    Prepares the DataFrame for Iceberg by adding __ingest_ts if not present.

    If the source data has a timestamp column the user wants to use as the
    partition key, they can specify it. Otherwise, the current timestamp is used.

    Args:
        df: Source DataFrame
        timestamp_col (str): Optional existing column to use as __ingest_ts

    Returns:
        DataFrame: DataFrame with __ingest_ts column
    """
    if "__ingest_ts" in df.columns:
        print("Source data already has __ingest_ts column, using as-is")
        # Ensure it's a timestamp type
        df = df.withColumn("__ingest_ts", F.to_timestamp("__ingest_ts"))
        return df

    if timestamp_col and timestamp_col in df.columns:
        print(f"Using '{timestamp_col}' as __ingest_ts")
        df = df.withColumn("__ingest_ts", F.to_timestamp(F.col(timestamp_col)))
    else:
        if timestamp_col:
            print(f"WARNING: Column '{timestamp_col}' not found in data. Using current_timestamp().")
        print("Adding __ingest_ts = current_timestamp()")
        df = df.withColumn("__ingest_ts", F.current_timestamp())

    return df


def _is_column_name(s):
    """Check if a string is a valid column name, including dot-notation for nested fields."""
    return bool(re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$", s))


def _is_nested(col_name):
    """Check if a column name references a nested field (contains dots)."""
    return "." in col_name


def _validate_format_version(format_version: Optional[int], mode: str) -> None:
    """Validate a requested Iceberg format version and write mode."""
    if format_version is not None and format_version not in (1, 2, 3):
        raise ValueError("Format version must be 1, 2, or 3")
    if format_version is not None and mode != "create":
        raise ValueError("Format version can only be set when mode is 'create'")


def parse_partition_expr(expr_str):
    """
    Parses a partition expression string into a PySpark column transform.

    Supported formats:
        days(col)           → P.days("col")
        hours(col)          → P.hours("col")
        months(col)         → P.months("col")
        years(col)          → P.years("col")
        bucket(n, col)      → P.bucket(n, "col")
        col                 → F.col("col")   (identity partition)

    Nested fields are supported using dot-notation (e.g. metadata.region,
    days(event.timestamp)). Nested fields are automatically extracted to
    top-level columns before table creation (metadata.region → metadata_region).

    Args:
        expr_str (str): Partition expression string

    Returns:
        tuple: (partition_transform, nested_col_or_None)
            - partition_transform: PySpark partition transform expression
            - nested_col_or_None: Original nested column path if extraction is needed, else None
    """
    expr_str = expr_str.strip()

    # Transform functions: days(col), hours(col), months(col), years(col)
    match = re.match(r"^(days|hours|months|years)\((.+)\)$", expr_str)
    if match:
        func_name, col_name = match.group(1), match.group(2).strip()
        if not _is_column_name(col_name):
            raise ValueError(f"Invalid column name in partition expression: '{col_name}'")
        transform_fn = getattr(P, func_name)
        if _is_nested(col_name):
            flat_name = col_name.replace(".", "_")
            return transform_fn(flat_name), col_name
        return transform_fn(col_name), None

    # bucket(n, col)
    match = re.match(r"^bucket\((\d+)\s*,\s*(.+)\)$", expr_str)
    if match:
        n = int(match.group(1))
        col_name = match.group(2).strip()
        if not _is_column_name(col_name):
            raise ValueError(f"Invalid column name in partition expression: '{col_name}'")
        nested_col = col_name if _is_nested(col_name) else None
        partition_col = col_name.replace(".", "_") if nested_col else col_name
        return P.bucket(n, partition_col), nested_col

    if re.match(r"^truncate\s*\(", expr_str):
        raise ValueError(
            "truncate() partitioning is not available through PySpark 4 WriterV2; "
            "use a supported transform such as bucket()"
        )

    # Identity partition: column name (supports dot-notation for nested fields)
    if _is_column_name(expr_str):
        if _is_nested(expr_str):
            flat_name = expr_str.replace(".", "_")
            return F.col(flat_name), expr_str
        return F.col(expr_str), None

    raise ValueError(
        f"Unsupported partition expression: '{expr_str}'. "
        f"Supported: days(col), hours(col), months(col), years(col), "
        f"bucket(n, col), or col (identity). "
        f"Nested fields use dot-notation: metadata.region, days(event.timestamp)"
    )


def parse_partition_by(partition_by_list):
    """
    Parses a list of partition expression strings into PySpark transforms.

    Args:
        partition_by_list (list[str]): List of partition expression strings

    Returns:
        tuple: (list[Column], list[str])
            - List of PySpark partition transform expressions
            - List of nested column paths that need extraction
    """
    exprs = []
    nested_cols = []
    for expr_str in partition_by_list:
        parsed, nested_col = parse_partition_expr(expr_str)
        print(f"  Partition expression: {expr_str}")
        if nested_col:
            flat_name = nested_col.replace(".", "_")
            print(f"    → nested field '{nested_col}' will be extracted to '{flat_name}'")
            nested_cols.append(nested_col)
        exprs.append(parsed)
    return exprs, nested_cols


def extract_nested_fields(df, nested_cols):
    """
    Extracts nested struct fields to top-level columns for partitioning.

    Iceberg cannot partition directly on nested struct fields, so this function
    creates top-level columns from nested paths. For example, 'metadata.region'
    becomes a new column 'metadata_region' with the value of df["metadata"]["region"].

    Args:
        df: Source DataFrame
        nested_cols (list[str]): List of dot-notation column paths (e.g. ['metadata.region'])

    Returns:
        DataFrame: DataFrame with extracted top-level columns added
    """
    for nested_col in nested_cols:
        flat_name = nested_col.replace(".", "_")
        print(f"Extracting nested field '{nested_col}' → '{flat_name}'")
        df = df.withColumn(flat_name, F.col(nested_col))
    return df


def create_iceberg_table(spark, df, namespace, table_name, mode="create", partition_exprs=None,
                         format_version=None):
    """
    Creates a partitioned Iceberg table and writes the data.

    Args:
        spark: SparkSession instance
        df: DataFrame to write
        namespace (str): Target namespace
        table_name (str): Target table name
        mode (str): 'create' (fail if exists), 'append' (add to existing), 'overwrite' (replace data)
        partition_exprs (list[Column]): Partition transform expressions. Defaults to [P.days("__ingest_ts")].
        format_version (int): Iceberg format version for a newly created table.

    Returns:
        str: Fully qualified table name
    """
    if partition_exprs is None:
        partition_exprs = [P.days("__ingest_ts")]

    _validate_format_version(format_version, mode)

    fq_table = f"{namespace}.{table_name}"

    # Ensure namespace exists
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    print(f"Namespace '{namespace}' ready")

    if mode == "create":
        print(f"Creating table '{fq_table}'")
        writer = df.writeTo(fq_table).using("iceberg")
        writer = writer.partitionedBy(*partition_exprs)
        if format_version is not None:
            writer = writer.tableProperty("format-version", str(format_version))
        writer.create()
    elif mode == "append":
        print(f"Appending to table '{fq_table}'")
        df.writeTo(fq_table).using("iceberg").append()
    elif mode == "overwrite":
        print(f"Overwriting table '{fq_table}'")
        df.writeTo(fq_table).using("iceberg").overwritePartitions()
    else:
        raise ValueError(f"Unknown mode: {mode}. Use 'create', 'append', or 'overwrite'.")

    return fq_table


def verify_table(spark, fq_table, limit=5):
    """
    Verifies the table was created correctly by showing sample data and metadata.

    Args:
        spark: SparkSession instance
        fq_table (str): Fully qualified table name
        limit (int): Number of sample rows to show
    """
    print(f"\n{'='*60}")
    print(f"Table '{fq_table}' created successfully")
    print(f"{'='*60}")

    print(f"\nTable schema:")
    spark.sql(f"DESCRIBE {fq_table}").show(truncate=False)

    row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {fq_table}").collect()[0]["cnt"]
    print(f"Total rows: {row_count}")

    print(f"\nSample data ({limit} rows):")
    spark.sql(f"SELECT * FROM {fq_table} LIMIT {limit}").show(truncate=False)

    print(f"\nSnapshot history:")
    spark.sql(f"SELECT * FROM {fq_table}.snapshots").show(truncate=False)


def json_to_iceberg(bucket, namespace, table_name, prefix=None, timestamp_col=None,
                    mode="create", multiline=False, sample_ratio=None, verify=True,
                    partition_by=None, format_version=None):
    """
    End-to-end: reads JSON from R2 and writes it as a partitioned Iceberg table.

    Args:
        bucket (str): R2 bucket name
        namespace (str): Target Iceberg namespace
        table_name (str): Target Iceberg table name
        prefix (str): Optional key prefix in the bucket
        timestamp_col (str): Optional column to use as __ingest_ts
        mode (str): Write mode - 'create', 'append', 'overwrite'
        multiline (bool): Whether JSON files are multi-line
        sample_ratio (float): Optional sampling ratio for schema inference
        verify (bool): Whether to verify the table after creation
        partition_by (list[str]): Optional partition expressions. Defaults to ['days(__ingest_ts)'].
        format_version (int): Iceberg format version for a newly created table.

    Returns:
        str: Fully qualified table name
    """
    _validate_format_version(format_version, mode)

    if not S3_ACCESS_KEY_ID or not S3_SECRET_ACCESS_KEY:
        print("ERROR: S3 credentials are required in r2dc_spark_config.py to read from R2.")
        print("Set S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY.")
        sys.exit(1)

    spark = get_spark_session("R2DataCatalog-JSONToIceberg")

    try:
        s3a_path = build_s3a_path(bucket, prefix)

        # Read and infer schema
        df = read_json_from_r2(spark, s3a_path, multiline=multiline, sample_ratio=sample_ratio)

        if df.rdd.isEmpty():
            print("ERROR: No data found at the specified path.")
            sys.exit(1)

        # Add __ingest_ts
        df = prepare_dataframe(df, timestamp_col=timestamp_col)

        # Parse partition expressions
        partition_exprs = None
        nested_cols = []
        if partition_by:
            print(f"\nCustom partitioning:")
            partition_exprs, nested_cols = parse_partition_by(partition_by)
        else:
            print(f"\nUsing default partitioning: days(__ingest_ts)")

        # Extract nested fields to top-level columns if needed for partitioning
        if nested_cols:
            df = extract_nested_fields(df, nested_cols)

        # Write to Iceberg
        fq_table = create_iceberg_table(spark, df, namespace, table_name,
                                        mode=mode, partition_exprs=partition_exprs,
                                        format_version=format_version)

        # Verify
        if verify:
            verify_table(spark, fq_table)

        print(f"\nDone. Table is queryable at: {fq_table}")
        return fq_table

    except Exception as e:
        print(f"ERROR: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert JSON files in R2 to an Apache Iceberg table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage — read all JSON from a bucket (partitions by days(__ingest_ts))
  python3 r2dc_json_to_iceberg.py --bucket my-data --namespace analytics --table events

  # Read from a specific prefix (folder)
  python3 r2dc_json_to_iceberg.py --bucket my-data --prefix logs/2026/03 --namespace analytics --table march_logs

  # Use an existing timestamp column for partitioning
  python3 r2dc_json_to_iceberg.py --bucket my-data --namespace analytics --table events --timestamp-col event_time

  # Custom partition key — partition by category (identity)
  python3 r2dc_json_to_iceberg.py --bucket my-data --namespace analytics --table events --partition-by category

  # Custom partition key — partition by month instead of day
  python3 r2dc_json_to_iceberg.py --bucket my-data --namespace analytics --table events --partition-by "months(__ingest_ts)"

  # Multiple partition keys
  python3 r2dc_json_to_iceberg.py --bucket my-data --namespace analytics --table events --partition-by "days(__ingest_ts)" --partition-by category

  # Bucket partition (hash-based, good for high-cardinality columns)
  python3 r2dc_json_to_iceberg.py --bucket my-data --namespace analytics --table events --partition-by "bucket(16, user_id)"

  # Nested JSON field as partition key (metadata.region → metadata_region column)
  python3 r2dc_json_to_iceberg.py --bucket my-data --namespace analytics --table events --partition-by metadata.region

  # Nested field with time transform
  python3 r2dc_json_to_iceberg.py --bucket my-data --namespace analytics --table events --partition-by "days(event.timestamp)"

  # Append to an existing table
  python3 r2dc_json_to_iceberg.py --bucket my-data --namespace analytics --table events --mode append

  # Multi-line JSON files
  python3 r2dc_json_to_iceberg.py --bucket my-data --namespace analytics --table events --multiline

Supported partition expressions:
  days(col)           Time-based partition by day (R2 SQL compatible)
  hours(col)          Time-based partition by hour
  months(col)         Time-based partition by month
  years(col)          Time-based partition by year
  bucket(n, col)      Hash partition into n buckets
  col                 Identity partition (exact column value)

Nested fields (dot-notation):
  Nested JSON struct fields are supported using dot-notation.
  They are automatically extracted to top-level columns for partitioning.
    metadata.region           → extracted as metadata_region
    days(event.timestamp)     → extracted as event_timestamp, partitioned by day
        """,
    )

    parser.add_argument("--bucket", required=True, help="R2 bucket name containing JSON files")
    parser.add_argument("--prefix", default=None, help="Optional key prefix (folder path) within the bucket")
    parser.add_argument("--namespace", required=True, help="Target Iceberg namespace")
    parser.add_argument("--table", required=True, help="Target Iceberg table name")
    parser.add_argument("--timestamp-col", default=None,
                        help="Existing column to use as __ingest_ts (otherwise current_timestamp is used)")
    parser.add_argument("--mode", default="create", choices=["create", "append", "overwrite"],
                        help="Write mode: create (default), append, or overwrite")
    parser.add_argument("-v", "--version", type=int, choices=[1, 2, 3],
                        help="Iceberg format version for a newly created table")
    parser.add_argument("--multiline", action="store_true",
                        help="Enable multi-line JSON parsing (for pretty-printed JSON files)")
    parser.add_argument("--sample-ratio", type=float, default=None,
                        help="Sampling ratio for schema inference (0.0-1.0, default: read all)")
    parser.add_argument("--partition-by", action="append", default=None,
                        help="Partition expression (repeatable). Default: days(__ingest_ts). "
                             "Supports: days(col), hours(col), months(col), years(col), "
                             "bucket(n, col), or col (identity). "
                             "Nested fields use dot-notation: metadata.region, days(event.timestamp)")
    parser.add_argument("--no-verify", action="store_true",
                        help="Skip table verification after creation")

    args = parser.parse_args()

    if args.version and args.mode != "create":
        parser.error("--version can only be used with --mode create")

    json_to_iceberg(
        bucket=args.bucket,
        namespace=args.namespace,
        table_name=args.table,
        prefix=args.prefix,
        timestamp_col=args.timestamp_col,
        mode=args.mode,
        multiline=args.multiline,
        sample_ratio=args.sample_ratio,
        verify=not args.no_verify,
        partition_by=args.partition_by,
        format_version=args.version,
    )
