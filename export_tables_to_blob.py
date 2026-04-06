"""
Export PySpark Unity Catalog tables as single CSV files to Azure Blob Storage.

Usage:
    Run this script in a Databricks notebook environment.
    Update the configuration variables below before running.

Requirements:
    - Databricks Runtime with PySpark
    - Access to Unity Catalog tables
    - Azure Blob Storage account with valid credentials
    - Databricks secret scope configured with storage account key
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# ── Configuration ──────────────────────────────────────────────────────────────
# Update these values to match your environment

TABLE_1 = "xyz"
TABLE_2 = "abc"

ACCOUNT_NAME = "xaccount"
CONTAINER = "xcontainer"

# IMPORTANT: Never hardcode keys. Use Databricks secret scope.
ACCOUNT_KEY = dbutils.secrets.get(scope="your-scope", key="storage-account-key")

spark.conf.set(
    f"fs.azure.account.key.{ACCOUNT_NAME}.blob.core.windows.net",
    ACCOUNT_KEY,
)


# ── Helper Functions ───────────────────────────────────────────────────────────

def fix_decimal_columns(df):
    """
    Convert all DecimalType columns to DoubleType.

    DecimalType can cause serialization issues in CSV output
    (trailing zeros, inconsistent precision). Casting to Double
    produces cleaner numeric output.

    Args:
        df: PySpark DataFrame

    Returns:
        DataFrame with DecimalType columns cast to DoubleType
    """
    return df.select(
        [
            F.col(f.name).cast(DoubleType()).alias(f.name)
            if "DecimalType" in str(f.dataType)
            else F.col(f.name)
            for f in df.schema.fields
        ]
    )


def upload_df_as_single_csv(df, table_label, blob_path):
    """
    Upload a Spark DataFrame as a single, clean CSV file to Azure Blob Storage.

    This avoids Spark's default behavior of writing a folder with part files.
    Instead, it converts to Pandas, writes a local CSV, and copies it to blob.

    Note: toPandas() pulls all data to the driver. Only use this for
    small-to-medium datasets that fit in driver memory.

    Args:
        df: PySpark DataFrame to upload
        table_label: Descriptive label for logging (also used as temp filename)
        blob_path: Destination path in blob storage (e.g., "data/table1.csv")
    """
    print(f"\nUploading {table_label} as single CSV...")
    df = fix_decimal_columns(df)

    # Step 1: Convert to Pandas and write locally
    local_path = f"/tmp/{table_label}.csv"
    df.toPandas().to_csv(local_path, index=False, quoting=1)

    # Step 2: Copy local file to Blob Storage
    dest = f"wasbs://{CONTAINER}@{ACCOUNT_NAME}.blob.core.windows.net/{blob_path}"
    dbutils.fs.cp(f"file:{local_path}", dest)

    print(f"Done: {dest}")


# ── Main Execution ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Read tables from Unity Catalog
    df1 = spark.table(TABLE_1)
    df2 = spark.table(TABLE_2)

    # Upload as single CSV files
    upload_df_as_single_csv(df1, "xyz", "data/table1.csv")
    upload_df_as_single_csv(
        df2, "abc", "data/table2.csv"
    )

    # ── Verification ───────────────────────────────────────────────────────────
    print("\n── Verifying uploads ──")
    base_url = f"wasbs://{CONTAINER}@{ACCOUNT_NAME}.blob.core.windows.net/data"
    files = dbutils.fs.ls(base_url)
    for f in files:
        print(f"  {f.name}  ({f.size} bytes)")
