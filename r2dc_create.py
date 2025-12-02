"""
R2 Data Catalog - Create Operations
Handles CREATE operations for namespaces and tables in Apache Iceberg.
"""
from r2dc_spark_config import get_spark_session
import argparse
import os

def create_namespace(spark, namespace_name, properties=None):
    """
    Creates a new namespace in the catalog.

    Args:
        spark: SparkSession instance
        namespace_name (str): Name of the namespace to create
        properties (dict): Optional properties for the namespace

    Returns:
        DataFrame: Result of the create namespace operation
    """
    if properties:
        props_str = ", ".join([f"'{k}' = '{v}'" for k, v in properties.items()])
        create_query = f"CREATE NAMESPACE IF NOT EXISTS {namespace_name} WITH PROPERTIES ({props_str})"
    else:
        create_query = f"CREATE NAMESPACE IF NOT EXISTS {namespace_name}"

    print(f"Executing: {create_query}")
    result = spark.sql(create_query)
    return result

def create_table_from_sql(spark, sql_file_path):
    """
    Creates a table by reading a CREATE TABLE statement from a SQL file.

    Args:
        spark: SparkSession instance
        sql_file_path (str): Path to the SQL file containing CREATE TABLE statement

    Returns:
        DataFrame: Result of the create table operation
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

def create_table_direct(spark, table_name, schema_definition, partition_by=None, properties=None):
    """
    Creates a table directly using provided schema definition.

    Args:
        spark: SparkSession instance
        table_name (str): Fully qualified table name (e.g., 'namespace.table')
        schema_definition (str): Column definitions (e.g., 'id INT, name STRING, created_at TIMESTAMP')
        partition_by (list): Optional list of columns to partition by
        properties (dict): Optional table properties

    Returns:
        DataFrame: Result of the create table operation
    """
    create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_definition})"

    if partition_by:
        partition_clause = ", ".join(partition_by)
        create_query += f" PARTITIONED BY ({partition_clause})"

    if properties:
        props_str = ", ".join([f"'{k}' = '{v}'" for k, v in properties.items()])
        create_query += f" TBLPROPERTIES ({props_str})"

    print(f"Executing: {create_query}")
    result = spark.sql(create_query)
    return result

def show_namespaces(spark):
    """
    Lists all namespaces in the catalog.

    Args:
        spark: SparkSession instance

    Returns:
        DataFrame: List of namespaces
    """
    return spark.sql("SHOW NAMESPACES")

def show_tables(spark, namespace=None):
    """
    Lists all tables in a namespace.

    Args:
        spark: SparkSession instance
        namespace (str): Optional namespace name

    Returns:
        DataFrame: List of tables
    """
    if namespace:
        return spark.sql(f"SHOW TABLES IN {namespace}")
    else:
        return spark.sql("SHOW TABLES")

def describe_table(spark, table_name):
    """
    Describes a table's schema.

    Args:
        spark: SparkSession instance
        table_name (str): Fully qualified table name

    Returns:
        DataFrame: Table schema description
    """
    return spark.sql(f"DESCRIBE EXTENDED {table_name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create namespaces and tables in Iceberg catalog')
    parser.add_argument('--namespace', help='Create a namespace with this name')
    parser.add_argument('--sql-file', help='Path to SQL file containing CREATE TABLE statement')
    parser.add_argument('--list-namespaces', action='store_true', help='List all namespaces')
    parser.add_argument('--list-tables', help='List tables in specified namespace')
    parser.add_argument('--describe', help='Describe a table (provide table name)')

    args = parser.parse_args()

    spark = get_spark_session("R2DataCatalog-Create")

    try:
        if args.namespace:
            result = create_namespace(spark, args.namespace)
            print(f"Namespace '{args.namespace}' created successfully")

        if args.sql_file:
            result = create_table_from_sql(spark, args.sql_file)
            print("Table created successfully from SQL file")

        if args.list_namespaces:
            print("\nAvailable namespaces:")
            show_namespaces(spark).show(truncate=False)

        if args.list_tables:
            print(f"\nTables in namespace '{args.list_tables}':")
            show_tables(spark, args.list_tables).show(truncate=False)

        if args.describe:
            print(f"\nTable description for '{args.describe}':")
            describe_table(spark, args.describe).show(truncate=False)

        if not any([args.namespace, args.sql_file, args.list_namespaces, args.list_tables, args.describe]):
            parser.print_help()

    except Exception as e:
        print(f"Error during create operation: {e}")
        raise
    finally:
        spark.stop()
