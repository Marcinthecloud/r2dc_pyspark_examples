"""
R2 Data Catalog - JSON to Iceberg Table
Reads JSON files from an R2 bucket and converts them into an Apache Iceberg table.
Schema is inferred from the JSON data. The resulting table is partitioned by
days(__ingest_ts) for R2 SQL compatibility.
"""
from r2dc_spark_config import get_spark_session, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_ENDPOINT
import argparse
import sys
from datetime import datetime
from pyspark.sql import functions as F


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


def create_iceberg_table(spark, df, namespace, table_name, mode="create"):
    """
    Creates a partitioned Iceberg table and writes the data.

    Args:
        spark: SparkSession instance
        df: DataFrame to write
        namespace (str): Target namespace
        table_name (str): Target table name
        mode (str): 'create' (fail if exists), 'append' (add to existing), 'overwrite' (replace data)

    Returns:
        str: Fully qualified table name
    """
    fq_table = f"{namespace}.{table_name}"

    # Ensure namespace exists
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    print(f"Namespace '{namespace}' ready")

    if mode == "create":
        print(f"Creating table '{fq_table}' with partition by days(__ingest_ts)")
        df.writeTo(fq_table).using("iceberg").partitionedBy(
            F.days("__ingest_ts")
        ).create()
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
                    mode="create", multiline=False, sample_ratio=None, verify=True):
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

    Returns:
        str: Fully qualified table name
    """
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

        # Write to Iceberg
        fq_table = create_iceberg_table(spark, df, namespace, table_name, mode=mode)

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
  # Basic usage — read all JSON from a bucket
  python3 r2dc_json_to_iceberg.py --bucket my-data --namespace analytics --table events

  # Read from a specific prefix (folder)
  python3 r2dc_json_to_iceberg.py --bucket my-data --prefix logs/2026/03 --namespace analytics --table march_logs

  # Use an existing timestamp column for partitioning
  python3 r2dc_json_to_iceberg.py --bucket my-data --namespace analytics --table events --timestamp-col event_time

  # Append to an existing table
  python3 r2dc_json_to_iceberg.py --bucket my-data --namespace analytics --table events --mode append

  # Multi-line JSON files
  python3 r2dc_json_to_iceberg.py --bucket my-data --namespace analytics --table events --multiline
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
    parser.add_argument("--multiline", action="store_true",
                        help="Enable multi-line JSON parsing (for pretty-printed JSON files)")
    parser.add_argument("--sample-ratio", type=float, default=None,
                        help="Sampling ratio for schema inference (0.0-1.0, default: read all)")
    parser.add_argument("--no-verify", action="store_true",
                        help="Skip table verification after creation")

    args = parser.parse_args()

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
    )
