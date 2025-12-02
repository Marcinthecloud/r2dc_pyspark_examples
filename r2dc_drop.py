"""
R2 Data Catalog - Drop Operations
Handles DROP operations for tables and namespaces, with optional purge of all files.
"""
from r2dc_spark_config import get_spark_session
import argparse

def drop_table(spark, table_name, purge=False, if_exists=True):
    """
    Drops a table from the catalog.

    Args:
        spark: SparkSession instance
        table_name (str): Fully qualified table name (e.g., 'namespace.table')
        purge (bool): If True, also deletes all data and metadata files from storage
        if_exists (bool): If True, won't error if table doesn't exist

    Returns:
        None
    """
    if_exists_clause = "IF EXISTS" if if_exists else ""
    purge_clause = "PURGE" if purge else ""

    drop_query = f"DROP TABLE {if_exists_clause} {table_name} {purge_clause}".strip()

    print(f"Executing: {drop_query}")

    if purge:
        print("WARNING: This will delete the table AND all data/metadata files from storage!")
        print("This operation cannot be undone.")

    spark.sql(drop_query)

    if purge:
        print(f"✓ Table '{table_name}' dropped and all files removed from storage")
    else:
        print(f"✓ Table '{table_name}' dropped from catalog (files remain in storage)")

def drop_namespace(spark, namespace_name, cascade=False, if_exists=True):
    """
    Drops a namespace from the catalog.

    Args:
        spark: SparkSession instance
        namespace_name (str): Namespace name
        cascade (bool): If True, also drops all tables in the namespace
        if_exists (bool): If True, won't error if namespace doesn't exist

    Returns:
        None
    """
    if_exists_clause = "IF EXISTS" if if_exists else ""
    cascade_clause = "CASCADE" if cascade else "RESTRICT"

    drop_query = f"DROP NAMESPACE {if_exists_clause} {namespace_name} {cascade_clause}"

    print(f"Executing: {drop_query}")

    if cascade:
        print("WARNING: This will drop the namespace AND all tables within it!")
        print("Use --purge-tables to also delete all files from storage.")

    spark.sql(drop_query)

    if cascade:
        print(f"✓ Namespace '{namespace_name}' and all tables dropped")
    else:
        print(f"✓ Namespace '{namespace_name}' dropped")

def list_tables_in_namespace(spark, namespace_name):
    """
    Lists all tables in a namespace before dropping.

    Args:
        spark: SparkSession instance
        namespace_name (str): Namespace name

    Returns:
        list: List of table names
    """
    result = spark.sql(f"SHOW TABLES IN {namespace_name}")
    tables = [row.tableName for row in result.collect()]
    return tables

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Drop tables or namespaces from R2 Data Catalog',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Drop table (keeps files in storage)
  python3 r2dc_drop.py --table namespace.table

  # Drop table and DELETE ALL FILES from storage
  python3 r2dc_drop.py --table namespace.table --purge

  # Drop namespace (must be empty)
  python3 r2dc_drop.py --namespace my_namespace

  # Drop namespace and all tables in it
  python3 r2dc_drop.py --namespace my_namespace --cascade

  # Drop namespace, all tables, and DELETE ALL FILES
  python3 r2dc_drop.py --namespace my_namespace --cascade --purge-tables

WARNING: --purge operations DELETE ALL FILES from storage and CANNOT BE UNDONE!
        """
    )

    parser.add_argument('--table', help='Table name to drop (e.g., namespace.table)')
    parser.add_argument('--purge', action='store_true',
                        help='DELETE all data and metadata files from storage (CANNOT BE UNDONE!)')

    parser.add_argument('--namespace', help='Namespace to drop')
    parser.add_argument('--cascade', action='store_true',
                        help='Drop namespace and all tables within it')
    parser.add_argument('--purge-tables', action='store_true',
                        help='When using --cascade, also purge all table files from storage')

    parser.add_argument('--force', action='store_true',
                        help='Skip confirmation prompts (use with caution!)')

    args = parser.parse_args()

    if not args.table and not args.namespace:
        parser.print_help()
        print("\nError: Must specify either --table or --namespace")
        exit(1)

    if args.table and args.namespace:
        print("Error: Cannot specify both --table and --namespace")
        exit(1)

    if args.purge_tables and not args.cascade:
        print("Error: --purge-tables requires --cascade")
        exit(1)

    spark = get_spark_session("R2DataCatalog-Drop")

    try:
        if args.table:
            if args.purge and not args.force:
                response = input(f"\nAre you sure you want to DROP '{args.table}' and DELETE ALL FILES? (yes/no): ")
                if response.lower() != 'yes':
                    print("Operation cancelled.")
                    exit(0)

            drop_table(spark, args.table, purge=args.purge)

        elif args.namespace:
            if args.cascade:
                tables = list_tables_in_namespace(spark, args.namespace)
                if tables:
                    print(f"\nTables in namespace '{args.namespace}':")
                    for table in tables:
                        print(f"  - {table}")
                    print()

                if not args.force:
                    action = "DROP and DELETE ALL FILES for" if args.purge_tables else "DROP"
                    response = input(f"Are you sure you want to {action} namespace '{args.namespace}' and {len(tables)} table(s)? (yes/no): ")
                    if response.lower() != 'yes':
                        print("Operation cancelled.")
                        exit(0)

                if args.purge_tables:
                    print("\nDropping and purging tables...")
                    for table in tables:
                        drop_table(spark, f"{args.namespace}.{table}", purge=True, if_exists=True)

            drop_namespace(spark, args.namespace, cascade=args.cascade)

    except Exception as e:
        print(f"\nError during drop operation: {e}")
        raise
    finally:
        spark.stop()
