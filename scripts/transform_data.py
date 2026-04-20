"""
Phase 3: Data Processing & Quality Checks — Spark ETL with optimization.

Task Alignment:
  - Task 14: Spark Basics Task
  - Task 15: Spark DataFrames Task
  - Task 17: PySpark Advanced Task
  - Task 29: Data Quality Task

This script reads raw transaction data from HDFS, performs transformations,
applies Spark optimizations (partitioning, caching), runs comprehensive
data quality validation checks, and writes the clean processed data back
to HDFS in Parquet format for the warehouse layer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, sum as spark_sum, isnan, isnull,
    to_timestamp, lit, current_timestamp, round as spark_round,
    year, month, dayofweek, hour
)
from pyspark.sql.types import DoubleType, TimestampType
import json
import os


def create_spark_session():
    """Initialize an optimized Spark Session (Task 14)."""
    print("=" * 60)
    print("  BANKING CRM — SPARK DATA PROCESSING PIPELINE")
    print("=" * 60)
    
    spark = SparkSession.builder \
        .appName("BankingCRM_ETL") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw_data(spark):
    """Read raw transaction data from HDFS (Task 15)."""
    print("\n[Step 1/5] Reading raw data from HDFS...")
    
    raw_tx_path = "hdfs://namenode:9000/banking_crm/raw/raw_transactions.csv"
    
    df = spark.read.csv(raw_tx_path, header=True, inferSchema=True)
    
    # Cache the raw DataFrame for reuse in quality checks
    df.cache()
    raw_count = df.count()
    print(f"  ✓ Loaded {raw_count} raw transactions")
    print(f"  Schema: {df.columns}")
    
    return df, raw_count


def run_data_quality_checks(df, raw_count):
    """Comprehensive data quality validation (Task 29)."""
    print("\n[Step 2/5] Running Data Quality Checks...")
    
    quality_report = {
        "pipeline_run": "banking_crm_etl",
        "total_raw_records": raw_count,
        "checks": []
    }
    
    # ── Check 1: Null values in critical columns ──
    critical_cols = ["transaction_id", "customer_id", "amount", "transaction_date"]
    null_results = {}
    for c in critical_cols:
        null_count = df.filter(col(c).isNull() | (col(c) == "")).count()
        null_results[c] = null_count
    
    null_check_passed = all(v == 0 for v in null_results.values())
    quality_report["checks"].append({
        "check": "null_critical_columns",
        "status": "PASS" if null_check_passed else "FAIL",
        "details": null_results
    })
    print(f"  {'✓' if null_check_passed else '✗'} Null check: {null_results}")
    
    # ── Check 2: Amount range validation (must be > 0) ──
    invalid_amounts = df.filter(col("amount") <= 0).count()
    amount_check_passed = invalid_amounts == 0
    quality_report["checks"].append({
        "check": "amount_positive",
        "status": "PASS" if amount_check_passed else "FAIL",
        "invalid_count": invalid_amounts
    })
    print(f"  {'✓' if amount_check_passed else '✗'} Amount validation: {invalid_amounts} invalid")
    
    # ── Check 3: Duplicate transaction IDs ──
    total_ids = df.select("transaction_id").count()
    unique_ids = df.select("transaction_id").distinct().count()
    duplicate_count = total_ids - unique_ids
    dup_check_passed = duplicate_count == 0
    quality_report["checks"].append({
        "check": "unique_transaction_ids",
        "status": "PASS" if dup_check_passed else "FAIL",
        "duplicate_count": duplicate_count
    })
    print(f"  {'✓' if dup_check_passed else '✗'} Duplicate check: {duplicate_count} duplicates")
    
    # ── Check 4: Valid transaction types ──
    valid_types = {"Deposit", "Withdrawal", "Transfer", "Payment"}
    actual_types = set(row["transaction_type"] for row in df.select("transaction_type").distinct().collect())
    invalid_types = actual_types - valid_types
    type_check_passed = len(invalid_types) == 0
    quality_report["checks"].append({
        "check": "valid_transaction_types",
        "status": "PASS" if type_check_passed else "FAIL",
        "valid_types": list(valid_types),
        "invalid_types_found": list(invalid_types)
    })
    print(f"  {'✓' if type_check_passed else '✗'} Transaction type check: types={actual_types}")
    
    # ── Check 5: Valid status values ──
    valid_statuses = {"Completed", "Pending", "Failed"}
    actual_statuses = set(row["status"] for row in df.select("status").distinct().collect())
    invalid_statuses = actual_statuses - valid_statuses
    status_check_passed = len(invalid_statuses) == 0
    quality_report["checks"].append({
        "check": "valid_status_values",
        "status": "PASS" if status_check_passed else "FAIL",
        "valid_statuses": list(valid_statuses),
        "invalid_statuses_found": list(invalid_statuses)
    })
    print(f"  {'✓' if status_check_passed else '✗'} Status check: statuses={actual_statuses}")
    
    # ── Summary statistics ──
    amount_stats = df.select(
        spark_round(col("amount").cast(DoubleType()), 2).alias("amount")
    ).summary("count", "min", "max", "mean", "stddev").collect()
    
    stats_dict = {row["summary"]: row["amount"] for row in amount_stats}
    quality_report["summary_statistics"] = {
        "amount_stats": stats_dict,
        "status_distribution": {
            row["status"]: row["cnt"] 
            for row in df.groupBy("status").agg(count("*").alias("cnt")).collect()
        },
        "transaction_type_distribution": {
            row["transaction_type"]: row["cnt"]
            for row in df.groupBy("transaction_type").agg(count("*").alias("cnt")).collect()
        }
    }
    
    # Overall result
    all_passed = all(c["status"] == "PASS" for c in quality_report["checks"])
    quality_report["overall_status"] = "PASS" if all_passed else "FAIL"
    print(f"\n  ══ Overall Quality: {'✅ PASS' if all_passed else '❌ FAIL'} ══")
    
    return quality_report


def transform_data(df):
    """Apply data transformations (Task 15 & Task 17)."""
    print("\n[Step 3/5] Applying transformations...")
    
    # 1. Filter out failed transactions (data cleansing)
    df_cleaned = df.filter(col("status") != "Failed")
    print(f"  ✓ Removed Failed transactions: {df.count() - df_cleaned.count()} rows removed")
    
    # 2. Anomaly detection: flag transactions over $4,500
    df_flagged = df_cleaned.withColumn(
        "is_anomaly",
        when(col("amount") > 4500, True).otherwise(False)
    )
    
    # 3. Add temporal features for analytics
    df_enriched = df_flagged \
        .withColumn("tx_year", year(col("transaction_date"))) \
        .withColumn("tx_month", month(col("transaction_date"))) \
        .withColumn("tx_day_of_week", dayofweek(col("transaction_date"))) \
        .withColumn("tx_hour", hour(col("transaction_date")))
    
    # 4. Normalize amount to 2 decimal places
    df_normalized = df_enriched.withColumn(
        "amount", spark_round(col("amount").cast(DoubleType()), 2)
    )
    
    print(f"  ✓ Enriched with temporal features (year, month, day_of_week, hour)")
    print(f"  ✓ Anomaly flagging complete")
    
    anomaly_count = df_normalized.filter(col("is_anomaly") == True).count()
    print(f"  ✓ Anomalies detected: {anomaly_count}")
    
    return df_normalized


def optimize_and_write(df):
    """Optimize Spark job and write to HDFS as Parquet (Task 17)."""
    print("\n[Step 4/5] Optimizing and writing processed data...")
    
    # Optimization: Repartition by transaction_type for efficient downstream queries
    df_partitioned = df.repartition(4, "transaction_type")
    
    # Cache the partitioned DataFrame before writing
    df_partitioned.cache()
    final_count = df_partitioned.count()
    print(f"  ✓ Repartitioned into 4 partitions by transaction_type")
    print(f"  ✓ Final record count: {final_count}")
    
    # Write as Parquet (columnar format, optimized for analytics)
    output_path = "hdfs://namenode:9000/banking_crm/processed/fact_transactions"
    df_partitioned.write.mode("overwrite") \
        .partitionBy("transaction_type") \
        .parquet(output_path)
    
    print(f"  ✓ Written to {output_path} (partitioned by transaction_type)")
    
    return final_count


def save_quality_report(spark, quality_report):
    """Save the data quality report to HDFS as JSON."""
    print("\n[Step 5/5] Saving data quality report...")
    
    report_path = "/opt/spark/scripts/quality_report.json"
    with open(report_path, "w") as f:
        json.dump(quality_report, f, indent=2, default=str)
    print(f"  ✓ Quality report saved to {report_path}")
    
    # Also upload to HDFS
    hdfs_report_path = "/banking_crm/quality_reports/quality_report.json"
    os.system(f"HADOOP_USER_NAME=hadoop hdfs dfs -put -f {report_path} {hdfs_report_path}")
    print(f"  ✓ Quality report uploaded to HDFS: {hdfs_report_path}")


def main():
    spark = create_spark_session()
    
    try:
        # Read raw data
        df_raw, raw_count = read_raw_data(spark)
        
        # Run quality checks on raw data
        quality_report = run_data_quality_checks(df_raw, raw_count)
        
        # Transform data
        df_processed = transform_data(df_raw)
        
        # Optimize and write
        final_count = optimize_and_write(df_processed)
        quality_report["processed_record_count"] = final_count
        
        # Save quality report
        save_quality_report(spark, quality_report)
        
        print("\n" + "=" * 60)
        print("  ✅ SPARK PROCESSING PIPELINE COMPLETE!")
        print(f"  Raw: {raw_count} → Processed: {final_count}")
        print("=" * 60)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
