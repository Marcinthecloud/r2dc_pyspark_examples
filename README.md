# PySpark Examples and Utilities for R2 Data Catalog

A collection of Python scripts (examples) for managing Apache Iceberg tables on Cloudflare R2 Data Catalog using PySpark.

## Features

- **Create**: Create namespaces and tables from SQL files
- **Insert**: Insert data from SQL files, CSV, or Parquet
- **Delete**: Delete data with optional cleanup of unreferenced files
- **Drop**: Drop tables or namespaces with optional purge of all files from storage
- **Shared Configuration**: Centralized Spark session management

## Prerequisites

- Python 3.8+
- Java 8, 11, or 17 (required by PySpark)
- Cloudflare R2 Data Catalog account

## Installation

### 1. Install Java (if not already installed)

```bash
# macOS with Homebrew
brew install openjdk@17

# Add to PATH (add to ~/.zshrc or ~/.bash_profile)
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

## Configuration

rename `r2dc_spark_config.py.example` to  `r2dc_spark_config.py` and update the credentials:

```python
WAREHOUSE = "your-warehouse-path"
TOKEN = "your-token"
ENDPOINT = "your-catalog-uri"
```
**NOTE**: if you want to cleanup/remove files, you'll need to configure S3 style access in the config:
```python
S3_ACCESS_KEY_ID = "key"  
S3_SECRET_ACCESS_KEY = "secret"  
S3_ENDPOINT = "https://<account_id>.r2.cloudflarestorage.com/" 

## Usage

### Create Operations

**Create a namespace:**
```bash
python r2dc_create.py --namespace my_namespace
```

**Create a table from SQL file:**
```bash
python r2dc_create.py --sql-file create_table.sql
```

Example `create_table.sql`:
```sql
CREATE TABLE IF NOT EXISTS my_namespace.users (
    id INT,
    name STRING,
    email STRING,
    created_at TIMESTAMP
) PARTITIONED BY (days(created_at))
```

**List all namespaces:**
```bash
python r2dc_create.py --list-namespaces
```

**List tables in a namespace:**
```bash
python r2dc_create.py --list-tables my_namespace
```

**Describe a table:**
```bash
python r2dc_create.py --describe my_namespace.users
```

### Insert Operations

**Insert from SQL file:**
```bash
python r2dc_insert.py --sql-file insert_data.sql
```

Example `insert_data.sql`:
```sql
INSERT INTO my_namespace.users VALUES
(1, 'Alice', 'alice@example.com', TIMESTAMP '2024-01-01 10:00:00'),
(2, 'Bob', 'bob@example.com', TIMESTAMP '2024-01-02 11:00:00')
```

**Insert from CSV:**
```bash
python r2dc_insert.py --csv-file data.csv --table my_namespace.users
```

**Insert from Parquet:**
```bash
python r2dc_insert.py --parquet-file data.parquet --table my_namespace.users
```

**Insert with overwrite mode:**
```bash
python r2dc_insert.py --csv-file data.csv --table my_namespace.users --mode overwrite
```

**Show table data:**
```bash
python r2dc_insert.py --show my_namespace.users --limit 20
```

### Delete Operations

**Delete all records:**
```bash
python r2dc_delete.py --table my_namespace.users
```

**Delete with WHERE clause:**
```bash
python r2dc_delete.py --table my_namespace.users --where "id > 100"
```

**Delete with cleanup (recommended):**
```bash
python r2dc_delete.py --table my_namespace.users --cleanup
```

This will:
1. Delete the data
2. Expire old snapshots (default: 7 days)
3. Remove orphan files (default: 3 days)

**Delete with custom cleanup settings:**
```bash
python r2dc_delete.py --table my_namespace.users --cleanup --expire-days 5 --orphan-days 2
```

### Drop Operations

**Drop table (removes from catalog, keeps files in storage):**
```bash
python r2dc_drop.py --table my_namespace.users
```

**Drop table and DELETE ALL FILES from storage:**
```bash
python r2dc_drop.py --table my_namespace.users --purge
```

**Drop namespace (must be empty):**
```bash
python r2dc_drop.py --namespace my_namespace
```

**Drop namespace and all tables in it:**
```bash
python r2dc_drop.py --namespace my_namespace --cascade
```

**Drop namespace, all tables, and DELETE ALL FILES:**
```bash
python r2dc_drop.py --namespace my_namespace --cascade --purge-tables
```

**Skip confirmation prompts (use with caution):**
```bash
python r2dc_drop.py --table my_namespace.users --purge --force
```

**IMPORTANT NOTES:**
- `--purge` operations DELETE ALL FILES from storage and CANNOT BE UNDONE
- `--purge` requires S3 credentials to be configured in `r2dc_spark_config.py`
- Without `--purge`, the table is removed from the catalog but files remain in storage
- `--purge-tables` only works with `--cascade` for namespace operations
- By default, you will be prompted to confirm destructive operations

## File Structure

```
pyspark/
├── r2dc_spark_config.py    # Shared Spark configuration
├── r2dc_create.py           # Create namespaces and tables
├── r2dc_insert.py           # Insert operations
├── r2dc_delete.py           # Delete operations with cleanup
├── r2dc_drop.py             # Drop tables/namespaces with optional purge to cleanup files
├── requirements.txt         # Python dependencies
└── README.md                # This file
```

## Write Modes

When inserting data, you can specify different modes:

- `append` (default): Add new data to existing table
- `overwrite`: Replace all existing data
- `error`: Throw error if table exists
- `ignore`: Silently ignore if table exists

## Cleanup Operations

The delete script includes several cleanup operations:

1. **Expire Snapshots**: Marks old snapshots for deletion
2. **Remove Orphan Files**: Removes data files no longer referenced by any snapshot
3. **Cleanup Metadata**: Removes old metadata files

## Examples

### Complete Workflow Example

```bash
# 1. Create namespace
python r2dc_create.py --namespace sales_data

# 2. Create table from SQL file
python r2dc_create.py --sql-file tables/sales.sql

# 3. Insert data from CSV
python r2dc_insert.py --csv-file data/sales_2024.csv --table sales_data.sales

# 4. Verify data
python r2dc_insert.py --show sales_data.sales --limit 10

# 5. Delete old records with cleanup
python r2dc_delete.py --table sales_data.sales --where "date < '2024-01-01'" --cleanup

# 6. Drop table and remove all files from storage (when done)
python r2dc_drop.py --table sales_data.sales --purge
```

## Additional Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Cloudflare R2 Data Catalog](https://developers.cloudflare.com/r2/data-catalog/)
