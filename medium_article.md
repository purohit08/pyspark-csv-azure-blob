# How to Export PySpark Tables as Single CSV Files to Azure Blob Storage

## The Problem Every Data Engineer Faces

You have tables in Databricks Unity Catalog. You need them as clean, single CSV files in Azure Blob Storage. Sounds simple, right?

Not quite.

When you use PySpark's built-in `.csv()` writer, you don't get a single CSV file. You get a **folder** full of part files, a `_SUCCESS` marker, and CRC files. Something like this:

```
data/output/
    _SUCCESS
    part-00000-xxxx-xxxx.csv
    part-00001-xxxx-xxxx.csv
    .part-00000-xxxx-xxxx.csv.crc
```

This is because Spark distributes writes across partitions. Great for performance, terrible when your downstream consumer expects a single `table.csv` file.

In this article, I'll walk you through a clean solution that produces **actual single CSV files** in Azure Blob Storage — no folders, no part files, no junk.

---

## The Naive Approach (And Why It Falls Short)

Your first instinct might be to use `coalesce(1)` to force a single partition:

```python
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
```

This reduces the output to **one** part file, but you still get a folder:

```
data/output/
    _SUCCESS
    part-00000-xxxx-xxxx.csv
```

You could try using Hadoop's `FileSystem` API to rename the part file and delete the folder. But on Azure Blob Storage via `wasbs://`, this rename operation can be unreliable — blobs aren't a real filesystem, and folder semantics are emulated.

---

## The Clean Solution: Pandas + dbutils

The trick is to skip Spark's file writer entirely for the final output. Instead:

1. Convert the Spark DataFrame to Pandas
2. Write a local CSV file
3. Copy that single file to Blob Storage using `dbutils.fs.cp`

Here's the complete, production-ready script:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import pandas as pd

# ── Configuration ────────────────────────────────────────────
TABLE_1 = "xyz"
TABLE_2 = "abc"

ACCOUNT_NAME = "xaccount"
CONTAINER = "xcontainer"

# IMPORTANT: Never hardcode keys. Use Databricks secret scope.
ACCOUNT_KEY = dbutils.secrets.get(scope="your-scope", key="storage-account-key")

spark.conf.set(
    f"fs.azure.account.key.{ACCOUNT_NAME}.blob.core.windows.net",
    ACCOUNT_KEY
)

# ── Helper: Fix Decimal Columns ─────────────────────────────
def fix_decimal_columns(df):
    """
    Converts DecimalType columns to DoubleType.
    Decimal types can cause serialization issues in CSV output.
    """
    return df.select([
        F.col(f.name).cast(DoubleType()).alias(f.name)
        if "DecimalType" in str(f.dataType)
        else F.col(f.name)
        for f in df.schema.fields
    ])

# ── Upload Function ──────────────────────────────────────────
def upload_df_as_single_csv(df, table_label, blob_path):
    """
    Uploads a Spark DataFrame as a single, clean CSV file
    to Azure Blob Storage.
    """
    print(f"\nUploading {table_label} as single CSV...")
    df = fix_decimal_columns(df)

    # Step 1: Convert to Pandas and write locally
    local_path = f"/tmp/{table_label}.csv"
    df.toPandas().to_csv(local_path, index=False, quoting=1)

    # Step 2: Copy to Blob Storage
    dest = (
        f"wasbs://{CONTAINER}@{ACCOUNT_NAME}"
        f".blob.core.windows.net/{blob_path}"
    )
    dbutils.fs.cp(f"file:{local_path}", dest)

    print(f"Done: {dest}")

# ── Execute ──────────────────────────────────────────────────
df1 = spark.table(TABLE_1)
df2 = spark.table(TABLE_2)

upload_df_as_single_csv(
    df1, "xyz", "data/table1.csv"
)
upload_df_as_single_csv(
    df2, "abc", "data/table2.csv"
)
```

After running this, your Blob Storage will contain:

```
pangeacontainer/
    data/table1.csv
    data/table2.csv
```

Clean. Simple. No folders. No part files.

---

## Key Design Decisions Explained

### Why convert DecimalType to DoubleType?

PySpark's `DecimalType` can produce unexpected formatting in CSV output — trailing zeros, inconsistent precision, and serialization quirks. Casting to `DoubleType` before writing avoids these issues.

### Why Pandas instead of Spark's CSV writer?

Spark's `.csv()` writer is designed for distributed output. It always writes to a directory with partition files. While you can use `coalesce(1)` and then rename the part file using Hadoop's FileSystem API, this approach is:

- **Fragile on Azure Blob Storage** — rename operations on `wasbs://` can silently fail
- **More code to maintain** — you need Hadoop FileSystem boilerplate
- **No cleaner in practice** — you're fighting the framework

The Pandas approach is straightforward: write locally, copy once.

### Why `quoting=1`?

The `quoting=1` parameter (which maps to `csv.QUOTE_ALL` in Python) wraps every field in double quotes. This prevents issues with commas, newlines, or special characters inside field values from breaking the CSV structure.

### Why `dbutils.fs.cp` instead of Azure SDK?

In a Databricks environment, `dbutils.fs.cp` is the simplest way to move files between the local filesystem and cloud storage. It uses the already-configured Spark Hadoop settings, so you don't need to install or configure the Azure SDK separately.

---

## When NOT to Use This Approach

The `toPandas()` call pulls the entire dataset into the driver node's memory. This works well when:

- Your table has fewer than ~10 million rows
- The data fits in the driver's available RAM (typically 4–16 GB)

If your tables are larger, consider these alternatives:

- **Stick with Spark's partitioned output** and have the consumer handle multiple files
- **Use `coalesce(1)` with Spark** and accept the folder structure
- **Write to Delta format** and use a separate job to export

---

## Verifying the Upload

Run this in a subsequent notebook cell to confirm both files landed correctly:

```python
base_url = (
    f"wasbs://{CONTAINER}@{ACCOUNT_NAME}"
    f".blob.core.windows.net/data"
)
files = dbutils.fs.ls(base_url)
for f in files:
    print(f.name, f.size)
```

You can also read them back:

```python
check = spark.read.option("header", "true").csv(
    f"wasbs://{CONTAINER}@{ACCOUNT_NAME}"
    f".blob.core.windows.net/data/table1.csv"
)
check.show(5)
check.printSchema()
```

---

## Security Reminder

Never hardcode storage account keys in your notebooks. Use Databricks secret scopes:

```python
ACCOUNT_KEY = dbutils.secrets.get(
    scope="your-scope",
    key="storage-account-key"
)
```

Set up a secret scope via the Databricks CLI or UI, and store your Azure Storage account key there. This keeps credentials out of version control and notebook exports.

---

## Summary

| Approach | Single File? | Reliable on Azure? | Simple? |
|----------|:---:|:---:|:---:|
| Spark `.csv()` | No | Yes | Yes |
| Spark `coalesce(1)` | Folder with 1 part file | Yes | Yes |
| `coalesce(1)` + Hadoop rename | Yes | Unreliable | No |
| **Pandas + `dbutils.fs.cp`** | **Yes** | **Yes** | **Yes** |

The Pandas approach wins for small-to-medium datasets. It's simple, reliable, and produces exactly what downstream consumers expect: a single CSV file.

---

*If you found this useful, feel free to follow for more data engineering tips. The full code is available on [GitHub](https://github.com/your-username/pyspark-csv-azure-blob).*
