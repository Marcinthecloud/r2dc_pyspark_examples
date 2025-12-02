"""
R2 Data Catalog - Delete Operations
Handles DELETE operations on Iceberg tables with optional cleanup of unreferenced files.
"""
from r2dc_spark_config import get_spark_session
import argparse
from datetime import datetime, timedelta

def delete_data(spark, table_name, where_clause=None):
    """
    Deletes data from an Iceberg table.

    Args:
        spark: SparkSession instance
        table_name (str): Fully qualified table name (e.g., 'namespace.table')
        where_clause (str): Optional WHERE clause for conditional delete

    Returns:
        DataFrame: Result of the delete operation
    """
    if where_clause:
        delete_query = f"DELETE FROM {table_name} WHERE {where_clause}"
    else:
        delete_query = f"DELETE FROM {table_name}"

    print(f"Executing: {delete_query}")
    result = spark.sql(delete_query)
    return result

def expire_snapshots(spark, table_name, older_than_days=7):
    """
    Expires old snapshots to mark files for deletion.

    Args:
        spark: SparkSession instance
        table_name (str): Fully qualified table name
        older_than_days (int): Expire snapshots older than this many days

    Returns:
        DataFrame: Result of expire_snapshots procedure
    """
    timestamp = (datetime.now() - timedelta(days=older_than_days)).strftime('%Y-%m-%d %H:%M:%S.%f')
    expire_query = f"""
    CALL r2dc.system.expire_snapshots(
        table => '{table_name}',
        older_than => TIMESTAMP '{timestamp}'
    )
    """
    print(f"Expiring snapshots older than {older_than_days} days (before {timestamp})...")
    result = spark.sql(expire_query)
    return result

def remove_orphan_files(spark, table_name, older_than_days=3):
    """
    Removes orphan files that are no longer referenced by any snapshot.

    Args:
        spark: SparkSession instance
        table_name (str): Fully qualified table name
        older_than_days (int): Remove orphan files older than this many days

    Returns:
        DataFrame: Result of remove_orphan_files procedure
    """
    timestamp = (datetime.now() - timedelta(days=older_than_days)).strftime('%Y-%m-%d %H:%M:%S.%f')
    orphan_query = f"""
    CALL r2dc.system.remove_orphan_files(
        table => '{table_name}',
        older_than => TIMESTAMP '{timestamp}'
    )
    """
    print(f"Removing orphan files older than {older_than_days} days (before {timestamp})...")
    result = spark.sql(orphan_query)
    return result

def rewrite_manifests(spark, table_name):
    """
    Rewrites manifest files to consolidate small manifests and clean up metadata.

    Args:
        spark: SparkSession instance
        table_name (str): Fully qualified table name

    Returns:
        DataFrame: Result of rewrite_manifests procedure
    """
    rewrite_query = f"""
    CALL r2dc.system.rewrite_manifests(
        table => '{table_name}'
    )
    """
    print("Rewriting manifest files...")
    result = spark.sql(rewrite_query)
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Delete data from Iceberg table with optional cleanup')
    parser.add_argument('--table', required=True, help='Table name (e.g., namespace.table)')
    parser.add_argument('--where', help='WHERE clause for conditional delete')
    parser.add_argument('--cleanup', action='store_true', help='Perform cleanup operations after delete')
    parser.add_argument('--expire-days', type=int, default=7, help='Days for snapshot expiration (default: 7)')
    parser.add_argument('--orphan-days', type=int, default=3, help='Days for orphan file removal (default: 3)')

    args = parser.parse_args()

    spark = get_spark_session("R2DataCatalog-Delete")

    try:
        result = delete_data(spark, args.table, args.where)
        print("Delete operation completed:")
        result.show()

        if args.cleanup:
            print("\n=== Starting Cleanup Operations ===")

            expire_result = expire_snapshots(spark, args.table, args.expire_days)
            print("Snapshot expiration completed:")
            expire_result.show()

            try:
                rewrite_result = rewrite_manifests(spark, args.table)
                print("Manifest rewrite completed:")
                rewrite_result.show()
            except Exception as e:
                print(f"\nNote: Manifest rewrite failed: {e}")
                print("This operation may not be supported or may require additional permissions.")
            try:
                orphan_result = remove_orphan_files(spark, args.table, args.orphan_days)
                print("Orphan file removal completed:")
                orphan_result.show()
            except Exception as e:
                if "NoAuthWithAWSException" in str(e) or "AccessDeniedException" in str(e):
                    print("\nNote: Orphan file removal skipped - requires direct S3 access.")
                    print("This is a known limitation when using vended credentials with R2 Data Catalog.")
                    print("Orphan files will be cleaned up by R2's automatic maintenance processes.")
                else:
                    raise

            print("\n=== Cleanup Operations Completed ===")

    except Exception as e:
        print(f"Error during delete operation: {e}")
        raise
    finally:
        spark.stop()
