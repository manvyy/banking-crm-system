"""
Phase 2: Data Ingestion — Extract CRM data from MySQL and load into HDFS.

Task Alignment:
  - Task 11: Data Ingestion Task
  - Task 12: Hadoop Task
  - Task 13: HDFS Architecture Task

This script connects to the operational MySQL database, extracts both the
customers and transactions tables, saves them as CSV locally, then uploads
them into the HDFS Data Lake at /banking_crm/raw/.
"""

import pandas as pd
from sqlalchemy import create_engine
import subprocess
import os

# ──────────────────────────────────────────────
# 1. Configuration
# ──────────────────────────────────────────────
MYSQL_URI = "mysql+pymysql://root:mypass@crm_mysql:3306/banking_crm"
HDFS_RAW_DIR = "/banking_crm/raw"
LOCAL_SCRIPT_DIR = "/opt/spark/scripts"  # mounted volume inside Spark container

TABLES = ["customers", "transactions"]


def create_hdfs_directories():
    """Create the required HDFS directory structure using the hadoop superuser."""
    dirs = [
        "/banking_crm",
        "/banking_crm/raw",
        "/banking_crm/processed",
        "/banking_crm/warehouse",
        "/banking_crm/quality_reports",
        "/banking_crm/dashboard",
    ]
    for d in dirs:
        cmd = f"HADOOP_USER_NAME=hadoop hdfs dfs -mkdir -p {d}"
        print(f"  Creating HDFS dir: {d}")
        os.system(cmd)


def extract_table(engine, table_name):
    """Extract a single table from MySQL to a local CSV file."""
    print(f"  Extracting table: {table_name}")
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, engine)
    
    local_path = os.path.join(LOCAL_SCRIPT_DIR, f"raw_{table_name}.csv")
    df.to_csv(local_path, index=False)
    print(f"  ✓ Saved {len(df)} rows to {local_path}")
    return local_path


def upload_to_hdfs(local_path, table_name):
    """Upload a local CSV file to the HDFS raw zone."""
    hdfs_path = f"{HDFS_RAW_DIR}/raw_{table_name}.csv"
    cmd = f"HADOOP_USER_NAME=hadoop hdfs dfs -put -f {local_path} {hdfs_path}"
    print(f"  Uploading to HDFS: {hdfs_path}")
    os.system(cmd)
    print(f"  ✓ Upload complete: {hdfs_path}")


def main():
    print("=" * 60)
    print("  BANKING CRM — DATA INGESTION PIPELINE")
    print("=" * 60)

    # Step 1: Create HDFS directory structure
    print("\n[Step 1/3] Creating HDFS directory structure...")
    create_hdfs_directories()

    # Step 2: Connect to MySQL and extract tables
    print("\n[Step 2/3] Connecting to MySQL...")
    engine = create_engine(MYSQL_URI)

    for table in TABLES:
        local_path = extract_table(engine, table)
        
        # Step 3: Upload each extracted CSV to HDFS
        print(f"\n[Step 3/3] Uploading {table} to HDFS...")
        upload_to_hdfs(local_path, table)

    print("\n" + "=" * 60)
    print("  ✅ INGESTION COMPLETE — All tables loaded into HDFS!")
    print("=" * 60)

    # Verify uploads
    print("\nVerifying HDFS contents:")
    os.system(f"HADOOP_USER_NAME=hadoop hdfs dfs -ls {HDFS_RAW_DIR}/")


if __name__ == "__main__":
    main()
