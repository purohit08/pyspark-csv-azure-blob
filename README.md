# PySpark CSV Export to Azure Blob Storage

Export PySpark Unity Catalog tables as **single, clean CSV files** to Azure Blob Storage — no part files, no folders, no `_SUCCESS` markers.

## The Problem

Spark's built-in `.csv()` writer produces a **directory** of part files:

```
output/
    _SUCCESS
    part-00000-xxxx.csv
    part-00001-xxxx.csv
```

Most downstream consumers expect a single `file.csv`. This script solves that.

## How It Works

1. Reads tables from Databricks Unity Catalog
2. Converts `DecimalType` columns to `DoubleType` (avoids CSV serialization issues)
3. Converts the DataFrame to Pandas and writes a local CSV
4. Copies the single file to Azure Blob Storage via `dbutils.fs.cp`

## Output

```
pangeacontainer/
    data/table1.csv
    data/table2.csv
```

## Setup

### Prerequisites

- Databricks Runtime with PySpark
- Access to Unity Catalog tables
- Azure Blob Storage account
- Databricks secret scope with storage account key

### Configure Secrets

```bash
# Using Databricks CLI
databricks secrets create-scope --scope your-scope
databricks secrets put --scope your-scope --key storage-account-key
```

### Update Configuration

Edit `export_tables_to_blob.py` and update:

```python
TABLE_1 = "your_catalog.your_schema.your_table_1"
TABLE_2 = "your_catalog.your_schema.your_table_2"
ACCOUNT_NAME = "your_storage_account"
CONTAINER = "your_container"
```

## Usage

Run in a Databricks notebook:

```python
%run ./export_tables_to_blob
```

Or copy the script contents into a notebook cell and execute.

## Limitations

- `toPandas()` pulls all data into driver memory — suitable for small-to-medium datasets (< ~10M rows)
- For larger datasets, consider Spark's native partitioned output or Delta format

## Why Not `coalesce(1)` + Hadoop Rename?

| Approach | Single File? | Reliable on Azure? | Simple? |
|----------|:---:|:---:|:---:|
| Spark `.csv()` | No | Yes | Yes |
| `coalesce(1)` + Hadoop rename | Yes | Unreliable | No |
| **Pandas + `dbutils.fs.cp`** | **Yes** | **Yes** | **Yes** |

The Hadoop `FileSystem.rename()` approach can silently fail on Azure Blob Storage (`wasbs://`), leaving behind empty blobs and undeleted temp folders.

## License

MIT
