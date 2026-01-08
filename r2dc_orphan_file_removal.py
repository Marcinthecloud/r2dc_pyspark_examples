#!/usr/bin/env python3
"""
Simple Orphan File Cleanup for a Single Table
Usage: python3 orphan_cleanup_single_table.py <namespace.table> [expire_older_than_days] [orphan_days]

Examples:
  python3 orphan_cleanup_single_table.py namespace.table_name 7 3
"""
from r2dc_spark_config import get_spark_session
from datetime import datetime, timedelta
import sys

def expire_snapshots(spark, table_name, older_than_days=7):
    """Expire old snapshots to mark files for deletion."""
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
    """Remove orphan files that are no longer referenced."""
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

def main():
    if len(sys.argv) < 2:
        print("ERROR: Table name required!")
        print()
        print("Usage: python3 orphan_cleanup_single_table.py <namespace.table> [expire_older_than_days] [orphan_days]")
        print()
        print("Examples:")
        print("  python3 orphan_cleanup_single_table.py namespace.table_name")
        print("  python3 orphan_cleanup_single_table.py namespace.table_name 7 3")
        print()
        print("Parameters:")
        print("  table_name    : Required. Fully qualified table name (namespace.table)")
        print("  expire_days   : Optional. Default 7. Expire snapshots older than N days")
        print("  orphan_days   : Optional. Default 3. Remove orphan files older than N days")
        sys.exit(1)
    
    table_name = sys.argv[1]
    expire_days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
    orphan_days = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    
    print("=" * 80)
    print(f"ORPHAN FILE CLEANUP: {table_name}")
    print("=" * 80)
    print()
    
    spark = get_spark_session("OrphanCleanup")
    
    try:
        print("Step 1: Snapshot Expiration")
        print("-" * 80)
        expire_result = expire_snapshots(spark, table_name, expire_days)
        print("Result:")
        expire_result.show(truncate=False)
        print()
        
        print("Step 2: Orphan File Removal")
        print("-" * 80)
        orphan_result = remove_orphan_files(spark, table_name, orphan_days)
        print("Result:")
        orphan_result.show(truncate=False)
        print()
        
        print("=" * 80)
        print("✓ CLEANUP COMPLETED SUCCESSFULLY")
        print("=" * 80)
        
    except Exception as e:
        print()
        print("=" * 80)
        print("✗ ERROR DURING CLEANUP")
        print("=" * 80)
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
