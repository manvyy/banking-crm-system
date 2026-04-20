"""
Phase 4: Local Data Warehousing (OLAP) — Star Schema Design & Spark SQL.

Task Alignment:
  - Task 9:  Data Warehousing Task
  - Task 16: Spark SQL Task

This script reads the processed transaction data and raw customer data,
builds a star schema with fact and dimension tables, writes them to HDFS
as Parquet, and runs sample analytical queries using Spark SQL.

Star Schema Design:
  ┌─────────────────┐
  │  dim_customers   │
  │─────────────────│
  │ customer_id (PK)│
  │ first_name       │
  │ last_name        │
  │ email            │
  │ account_type     │
  │ join_date        │
  │ tenure_days      │
  └────────┬────────┘
           │
  ┌────────┴────────────────────────────┐
  │          fact_transactions           │
  │─────────────────────────────────────│
  │ transaction_id (PK)                 │
  │ customer_id (FK → dim_customers)    │
  │ date_key (FK → dim_date)            │
  │ type_key (FK → dim_transaction_type)│
  │ amount                              │
  │ status                              │
  │ is_anomaly                          │
  └──┬──────────────────────────────┬───┘
     │                              │
  ┌──┴──────────┐   ┌──────────────┴──┐
  │  dim_date    │   │dim_tx_type      │
  │─────────────│   │─────────────────│
  │ date_key(PK)│   │ type_key (PK)   │
  │ full_date   │   │ type_name       │
  │ year        │   │ type_description│
  │ quarter     │   └─────────────────┘
  │ month       │
  │ month_name  │
  │ day         │
  │ day_of_week │
  │ day_name    │
  │ is_weekend  │
  └─────────────┘
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, monotonically_increasing_id, datediff, current_date,
    year, quarter, month, dayofmonth, dayofweek, date_format,
    when, countDistinct, sum as spark_sum, avg as spark_avg,
    count, desc, round as spark_round, max as spark_max,
    min as spark_min, to_date
)
from pyspark.sql.types import IntegerType


def create_spark_session():
    """Initialize Spark Session for warehouse building."""
    print("=" * 60)
    print("  BANKING CRM — DATA WAREHOUSE BUILDER")
    print("=" * 60)

    spark = SparkSession.builder \
        .appName("BankingCRM_Warehouse") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ═══════════════════════════════════════════════════════════
# DIMENSION TABLE BUILDERS
# ═══════════════════════════════════════════════════════════

def build_dim_customers(spark):
    """Build the Customer Dimension table from raw customer CSV."""
    print("\n[Dimension 1/3] Building dim_customers...")

    raw_path = "hdfs://namenode:9000/banking_crm/raw/raw_customers.csv"
    df_raw = spark.read.csv(raw_path, header=True, inferSchema=True)

    dim_customers = df_raw.select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("phone_number"),
        col("account_type"),
        col("join_date"),
        datediff(current_date(), col("join_date")).alias("tenure_days")
    )

    dim_customers.cache()
    count_val = dim_customers.count()
    print(f"  ✓ dim_customers: {count_val} records")

    # Write to HDFS
    output_path = "hdfs://namenode:9000/banking_crm/warehouse/dim_customers"
    dim_customers.write.mode("overwrite").parquet(output_path)
    print(f"  ✓ Written to {output_path}")

    return dim_customers


def build_dim_date(spark, df_transactions):
    """Build the Date Dimension table from transaction dates."""
    print("\n[Dimension 2/3] Building dim_date...")

    # Extract unique dates from transactions
    df_dates = df_transactions.select(
        to_date(col("transaction_date")).alias("full_date")
    ).distinct()

    dim_date = df_dates.select(
        date_format(col("full_date"), "yyyyMMdd").cast(IntegerType()).alias("date_key"),
        col("full_date"),
        year(col("full_date")).alias("year"),
        quarter(col("full_date")).alias("quarter"),
        month(col("full_date")).alias("month"),
        date_format(col("full_date"), "MMMM").alias("month_name"),
        dayofmonth(col("full_date")).alias("day"),
        dayofweek(col("full_date")).alias("day_of_week"),
        date_format(col("full_date"), "EEEE").alias("day_name"),
        when(dayofweek(col("full_date")).isin(1, 7), True)
            .otherwise(False).alias("is_weekend")
    ).orderBy("date_key")

    dim_date.cache()
    count_val = dim_date.count()
    print(f"  ✓ dim_date: {count_val} unique dates")

    output_path = "hdfs://namenode:9000/banking_crm/warehouse/dim_date"
    dim_date.write.mode("overwrite").parquet(output_path)
    print(f"  ✓ Written to {output_path}")

    return dim_date


def build_dim_transaction_type(spark):
    """Build the Transaction Type Dimension table."""
    print("\n[Dimension 3/3] Building dim_transaction_type...")

    type_data = [
        (1, "Deposit", "Funds added to account"),
        (2, "Withdrawal", "Funds removed from account"),
        (3, "Transfer", "Funds moved between accounts"),
        (4, "Payment", "Payment for goods or services"),
    ]

    dim_tx_type = spark.createDataFrame(
        type_data,
        ["type_key", "type_name", "type_description"]
    )

    dim_tx_type.cache()
    print(f"  ✓ dim_transaction_type: {dim_tx_type.count()} types")

    output_path = "hdfs://namenode:9000/banking_crm/warehouse/dim_transaction_type"
    dim_tx_type.write.mode("overwrite").parquet(output_path)
    print(f"  ✓ Written to {output_path}")

    return dim_tx_type


# ═══════════════════════════════════════════════════════════
# FACT TABLE BUILDER
# ═══════════════════════════════════════════════════════════

def build_fact_transactions(spark, dim_tx_type):
    """Build the Fact Transactions table with foreign keys to dimensions."""
    print("\n[Fact Table] Building fact_transactions...")

    # Read processed transaction data from Phase 3
    processed_path = "hdfs://namenode:9000/banking_crm/processed/fact_transactions"
    df_processed = spark.read.parquet(processed_path)

    # Create date_key from transaction_date
    df_with_keys = df_processed.withColumn(
        "date_key",
        date_format(to_date(col("transaction_date")), "yyyyMMdd").cast(IntegerType())
    )

    # Join with dim_transaction_type to get type_key
    df_fact = df_with_keys.join(
        dim_tx_type.select("type_key", "type_name"),
        df_with_keys["transaction_type"] == dim_tx_type["type_name"],
        "left"
    ).select(
        col("transaction_id"),
        col("customer_id"),
        col("date_key"),
        col("type_key"),
        col("transaction_type"),
        col("amount"),
        col("status"),
        col("is_anomaly"),
        col("transaction_date"),
        col("tx_year"),
        col("tx_month"),
        col("tx_day_of_week"),
        col("tx_hour")
    )

    df_fact.cache()
    count_val = df_fact.count()
    print(f"  ✓ fact_transactions: {count_val} records")

    output_path = "hdfs://namenode:9000/banking_crm/warehouse/fact_transactions"
    df_fact.write.mode("overwrite").partitionBy("tx_year", "tx_month").parquet(output_path)
    print(f"  ✓ Written to {output_path} (partitioned by year/month)")

    return df_fact


# ═══════════════════════════════════════════════════════════
# ANALYTICAL QUERIES (Spark SQL)
# ═══════════════════════════════════════════════════════════

def run_analytical_queries(spark, fact_df, dim_customers, dim_date):
    """Run sample analytical queries using Spark SQL (Task 16)."""
    print("\n" + "=" * 60)
    print("  SPARK SQL — ANALYTICAL QUERIES")
    print("=" * 60)

    # Register temp views for Spark SQL
    fact_df.createOrReplaceTempView("fact_transactions")
    dim_customers.createOrReplaceTempView("dim_customers")
    dim_date.createOrReplaceTempView("dim_date")

    # ── Query 1: Monthly Revenue Trend ──
    print("\n── Query 1: Monthly Revenue Trend ──")
    q1 = spark.sql("""
        SELECT 
            tx_year,
            tx_month,
            COUNT(*) AS total_transactions,
            ROUND(SUM(amount), 2) AS total_revenue,
            ROUND(AVG(amount), 2) AS avg_transaction_amount
        FROM fact_transactions
        GROUP BY tx_year, tx_month
        ORDER BY tx_year, tx_month
    """)
    q1.show(20, truncate=False)

    # ── Query 2: Revenue by Account Type ──
    print("\n── Query 2: Revenue by Customer Account Type ──")
    q2 = spark.sql("""
        SELECT 
            c.account_type,
            COUNT(*) AS transaction_count,
            ROUND(SUM(f.amount), 2) AS total_revenue,
            ROUND(AVG(f.amount), 2) AS avg_amount,
            COUNT(DISTINCT f.customer_id) AS unique_customers
        FROM fact_transactions f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        GROUP BY c.account_type
        ORDER BY total_revenue DESC
    """)
    q2.show(truncate=False)

    # ── Query 3: Top 10 Customers by Transaction Volume ──
    print("\n── Query 3: Top 10 Customers by Total Spend ──")
    q3 = spark.sql("""
        SELECT 
            c.customer_id,
            CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
            c.account_type,
            COUNT(*) AS transaction_count,
            ROUND(SUM(f.amount), 2) AS total_spend
        FROM fact_transactions f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.account_type
        ORDER BY total_spend DESC
        LIMIT 10
    """)
    q3.show(truncate=False)

    # ── Query 4: Weekend vs Weekday Analysis ──
    print("\n── Query 4: Weekend vs Weekday Transaction Patterns ──")
    q4 = spark.sql("""
        SELECT 
            d.is_weekend,
            CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS period,
            COUNT(*) AS transaction_count,
            ROUND(SUM(f.amount), 2) AS total_revenue,
            ROUND(AVG(f.amount), 2) AS avg_amount
        FROM fact_transactions f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.is_weekend
        ORDER BY d.is_weekend
    """)
    q4.show(truncate=False)

    # ── Query 5: Anomaly Summary ──
    print("\n── Query 5: Anomaly Detection Summary ──")
    q5 = spark.sql("""
        SELECT 
            transaction_type,
            is_anomaly,
            COUNT(*) AS count,
            ROUND(SUM(amount), 2) AS total_amount,
            ROUND(AVG(amount), 2) AS avg_amount
        FROM fact_transactions
        GROUP BY transaction_type, is_anomaly
        ORDER BY transaction_type, is_anomaly
    """)
    q5.show(truncate=False)

    # ── Query 6: Customer Tenure Analysis ──
    print("\n── Query 6: Revenue by Customer Tenure Segment ──")
    q6 = spark.sql("""
        SELECT 
            CASE 
                WHEN c.tenure_days < 365 THEN '< 1 Year'
                WHEN c.tenure_days < 730 THEN '1-2 Years'
                WHEN c.tenure_days < 1095 THEN '2-3 Years'
                ELSE '3+ Years'
            END AS tenure_segment,
            COUNT(DISTINCT c.customer_id) AS customer_count,
            COUNT(*) AS transaction_count,
            ROUND(SUM(f.amount), 2) AS total_revenue
        FROM fact_transactions f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        GROUP BY 
            CASE 
                WHEN c.tenure_days < 365 THEN '< 1 Year'
                WHEN c.tenure_days < 730 THEN '1-2 Years'
                WHEN c.tenure_days < 1095 THEN '2-3 Years'
                ELSE '3+ Years'
            END
        ORDER BY total_revenue DESC
    """)
    q6.show(truncate=False)

    # Save query results for dashboard
    q1.write.mode("overwrite").json(
        "hdfs://namenode:9000/banking_crm/warehouse/analytics/monthly_revenue"
    )
    q2.write.mode("overwrite").json(
        "hdfs://namenode:9000/banking_crm/warehouse/analytics/revenue_by_account"
    )


def main():
    spark = create_spark_session()

    try:
        # Build dimension tables
        dim_customers = build_dim_customers(spark)
        
        # Read processed data to extract dates
        processed_path = "hdfs://namenode:9000/banking_crm/processed/fact_transactions"
        df_processed = spark.read.parquet(processed_path)
        
        dim_date = build_dim_date(spark, df_processed)
        dim_tx_type = build_dim_transaction_type(spark)

        # Build fact table
        fact_df = build_fact_transactions(spark, dim_tx_type)

        # Run analytical queries
        run_analytical_queries(spark, fact_df, dim_customers, dim_date)

        print("\n" + "=" * 60)
        print("  ✅ DATA WAREHOUSE BUILD COMPLETE!")
        print("  Tables: dim_customers, dim_date, dim_transaction_type, fact_transactions")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
