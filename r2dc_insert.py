"""
R2 Data Catalog - Insert Operations
Handles INSERT operations for Apache Iceberg tables.
"""
from r2dc_spark_config import get_spark_session
import argparse
import os

def insert_from_sql(spark, sql_file_path):
    """
    Executes INSERT statement from a SQL file.

    Args:
        spark: SparkSession instance
        sql_file_path (str): Path to the SQL file containing INSERT statement

    Returns:
        DataFrame: Result of the insert operation
    """
    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

    with open(sql_file_path, 'r') as f:
        sql_content = f.read().strip()

    if not sql_content:
        raise ValueError("SQL file is empty")

    print(f"Executing SQL from {sql_file_path}:")
    print(sql_content)

    result = spark.sql(sql_content)
    return result

def insert_values(spark, table_name, values):
    """
    Inserts values directly into a table.

    Args:
        spark: SparkSession instance
        table_name (str): Fully qualified table name
        values (str): Values to insert (e.g., "(1, 'Alice'), (2, 'Bob')")

    Returns:
        DataFrame: Result of the insert operation
    """
    insert_query = f"INSERT INTO {table_name} VALUES {values}"
    print(f"Executing: {insert_query}")
    result = spark.sql(insert_query)
    return result

def insert_from_dataframe(spark, df, table_name, mode="append"):
    """
    Inserts data from a DataFrame into an Iceberg table.

    Args:
        spark: SparkSession instance
        df: DataFrame to insert
        table_name (str): Fully qualified table name
        mode (str): Write mode - 'append', 'overwrite', 'error', 'ignore'

    Returns:
        None
    """
    print(f"Inserting {df.count()} rows into {table_name} with mode '{mode}'")
    df.writeTo(table_name).using("iceberg").mode(mode).save()
    print("Insert completed successfully")

def insert_from_csv(spark, table_name, csv_path, header=True, mode="append"):
    """
    Reads CSV file and inserts data into an Iceberg table.

    Args:
        spark: SparkSession instance
        table_name (str): Fully qualified table name
        csv_path (str): Path to CSV file
        header (bool): Whether CSV has header row
        mode (str): Write mode - 'append', 'overwrite', 'error', 'ignore'

    Returns:
        None
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    print(f"Reading CSV file: {csv_path}")
    df = spark.read.option("header", str(header).lower()).csv(csv_path)

    print(f"CSV Schema:")
    df.printSchema()

    insert_from_dataframe(spark, df, table_name, mode)

def insert_from_parquet(spark, table_name, parquet_path, mode="append"):
    """
    Reads Parquet file and inserts data into an Iceberg table.

    Args:
        spark: SparkSession instance
        table_name (str): Fully qualified table name
        parquet_path (str): Path to Parquet file
        mode (str): Write mode - 'append', 'overwrite', 'error', 'ignore'

    Returns:
        None
    """
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    print(f"Reading Parquet file: {parquet_path}")
    df = spark.read.parquet(parquet_path)

    print(f"Parquet Schema:")
    df.printSchema()

    insert_from_dataframe(spark, df, table_name, mode)

def insert_select(spark, target_table, source_query):
    """
    Inserts data using INSERT INTO ... SELECT.

    Args:
        spark: SparkSession instance
        target_table (str): Target table name
        source_query (str): SELECT query to get source data

    Returns:
        DataFrame: Result of the insert operation
    """
    insert_query = f"INSERT INTO {target_table} {source_query}"
    print(f"Executing: {insert_query}")
    result = spark.sql(insert_query)
    return result

def show_table_data(spark, table_name, limit=10):
    """
    Shows sample data from a table.

    Args:
        spark: SparkSession instance
        table_name (str): Fully qualified table name
        limit (int): Number of rows to display

    Returns:
        DataFrame: Sample data
    """
    return spark.sql(f"SELECT * FROM {table_name} LIMIT {limit}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Insert data into Iceberg tables')
    parser.add_argument('--sql-file', help='Path to SQL file containing INSERT statement')
    parser.add_argument('--csv-file', help='Path to CSV file to insert')
    parser.add_argument('--parquet-file', help='Path to Parquet file to insert')
    parser.add_argument('--table', help='Target table name (required with --csv-file or --parquet-file)')
    parser.add_argument('--mode', default='append', choices=['append', 'overwrite', 'error', 'ignore'],
                        help='Write mode (default: append)')
    parser.add_argument('--show', help='Show sample data from table')
    parser.add_argument('--limit', type=int, default=10, help='Number of rows to show (default: 10)')

    args = parser.parse_args()

    spark = get_spark_session("R2DataCatalog-Insert")

    try:
        if args.sql_file:
            result = insert_from_sql(spark, args.sql_file)
            print("Insert operation completed successfully")

        if args.csv_file:
            if not args.table:
                raise ValueError("--table is required when using --csv-file")
            insert_from_csv(spark, args.table, args.csv_file, mode=args.mode)

        if args.parquet_file:
            if not args.table:
                raise ValueError("--table is required when using --parquet-file")
            insert_from_parquet(spark, args.table, args.parquet_file, mode=args.mode)

        if args.show:
            print(f"\nSample data from '{args.show}' (limit {args.limit}):")
            show_table_data(spark, args.show, args.limit).show(truncate=False)

        if not any([args.sql_file, args.csv_file, args.parquet_file, args.show]):
            parser.print_help()

    except Exception as e:
        print(f"Error during insert operation: {e}")
        raise
    finally:
        spark.stop()
