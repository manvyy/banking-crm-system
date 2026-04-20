"""
Phase 6: Dashboarding & Visualization — Banking CRM Analytics.

Task Alignment:
  - Task 30: Final Project Task

This script connects to the local Spark SQL warehouse, reads the
star schema data, and generates comprehensive banking metric
visualizations completing the ingestion-to-dashboard pipeline.

Generated Charts:
  1. Monthly Transaction Volume & Revenue
  2. Transaction Type Distribution
  3. Top 10 Customers by Total Spend
  4. Revenue by Account Type
  5. Anomaly Detection Rate Over Time
  6. Daily Transaction Heatmap (Day-of-Week × Hour)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg as spark_avg,
    round as spark_round, when, desc, concat, lit
)
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for server environments

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import os


# ── Output directory ──
OUTPUT_DIR = "/opt/spark/scripts/dashboard_output"


def create_spark_session():
    """Initialize Spark Session for dashboard generation."""
    print("=" * 60)
    print("  BANKING CRM — DASHBOARD GENERATOR")
    print("=" * 60)

    spark = SparkSession.builder \
        .appName("BankingCRM_Dashboard") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_warehouse_data(spark):
    """Load star schema tables from HDFS warehouse."""
    print("\n[Step 1] Loading warehouse data...")

    data = {}
    tables = {
        "fact": "hdfs://namenode:9000/banking_crm/warehouse/fact_transactions",
        "dim_customers": "hdfs://namenode:9000/banking_crm/warehouse/dim_customers",
        "dim_date": "hdfs://namenode:9000/banking_crm/warehouse/dim_date",
        "dim_tx_type": "hdfs://namenode:9000/banking_crm/warehouse/dim_transaction_type",
    }

    for name, path in tables.items():
        data[name] = spark.read.parquet(path)
        data[name].cache()
        print(f"  ✓ {name}: {data[name].count()} records")

    return data


def setup_style():
    """Configure matplotlib style for professional-looking charts."""
    plt.rcParams.update({
        "figure.facecolor": "#0f0f23",
        "axes.facecolor": "#1a1a2e",
        "axes.edgecolor": "#30305a",
        "axes.labelcolor": "#e0e0ff",
        "text.color": "#e0e0ff",
        "xtick.color": "#a0a0d0",
        "ytick.color": "#a0a0d0",
        "grid.color": "#25254a",
        "grid.alpha": 0.5,
        "font.size": 11,
        "axes.titlesize": 14,
        "axes.titleweight": "bold",
        "figure.titlesize": 16,
        "figure.titleweight": "bold",
        "legend.facecolor": "#1a1a2e",
        "legend.edgecolor": "#30305a",
    })


# ═══════════════════════════════════════════════════════════
# CHART 1: Monthly Revenue Trend
# ═══════════════════════════════════════════════════════════

def chart_monthly_revenue(data):
    """Generate monthly transaction volume and revenue chart."""
    print("\n[Chart 1/6] Monthly Revenue Trend...")

    df = data["fact"].groupBy("tx_year", "tx_month").agg(
        count("*").alias("tx_count"),
        spark_round(spark_sum("amount"), 2).alias("total_revenue")
    ).orderBy("tx_year", "tx_month").toPandas()

    df["period"] = df["tx_year"].astype(str) + "-" + df["tx_month"].astype(str).str.zfill(2)

    fig, ax1 = plt.subplots(figsize=(14, 6))

    # Bar chart for transaction count
    colors = ["#6366f1" if i % 2 == 0 else "#818cf8" for i in range(len(df))]
    bars = ax1.bar(df["period"], df["tx_count"], color=colors, alpha=0.8, label="Transaction Count")
    ax1.set_xlabel("Month")
    ax1.set_ylabel("Transaction Count", color="#818cf8")
    ax1.tick_params(axis="y", labelcolor="#818cf8")

    # Line chart for revenue on secondary axis
    ax2 = ax1.twinx()
    ax2.plot(df["period"], df["total_revenue"], color="#f472b6", marker="o",
             linewidth=2.5, markersize=6, label="Total Revenue ($)")
    ax2.set_ylabel("Total Revenue ($)", color="#f472b6")
    ax2.tick_params(axis="y", labelcolor="#f472b6")
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))

    plt.title("Monthly Transaction Volume & Revenue", pad=20)
    ax1.set_xticklabels(df["period"], rotation=45, ha="right")

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    ax1.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "01_monthly_revenue.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✓ Saved 01_monthly_revenue.png")


# ═══════════════════════════════════════════════════════════
# CHART 2: Transaction Type Distribution
# ═══════════════════════════════════════════════════════════

def chart_transaction_type_distribution(data):
    """Generate transaction type distribution pie chart."""
    print("\n[Chart 2/6] Transaction Type Distribution...")

    df = data["fact"].groupBy("transaction_type").agg(
        count("*").alias("count"),
        spark_round(spark_sum("amount"), 2).alias("total")
    ).toPandas()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    colors = ["#6366f1", "#f472b6", "#34d399", "#fbbf24"]
    explode = [0.02] * len(df)

    # Count distribution
    wedges1, texts1, autotexts1 = ax1.pie(
        df["count"], labels=df["transaction_type"], autopct="%1.1f%%",
        colors=colors, explode=explode, startangle=90,
        textprops={"color": "#e0e0ff", "fontsize": 10}
    )
    ax1.set_title("By Count")

    # Revenue distribution
    wedges2, texts2, autotexts2 = ax2.pie(
        df["total"], labels=df["transaction_type"], autopct="%1.1f%%",
        colors=colors, explode=explode, startangle=90,
        textprops={"color": "#e0e0ff", "fontsize": 10}
    )
    ax2.set_title("By Revenue ($)")

    plt.suptitle("Transaction Type Distribution", y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "02_transaction_type_dist.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✓ Saved 02_transaction_type_dist.png")


# ═══════════════════════════════════════════════════════════
# CHART 3: Top 10 Customers by Spend
# ═══════════════════════════════════════════════════════════

def chart_top_customers(data):
    """Generate top 10 customers by total spend."""
    print("\n[Chart 3/6] Top 10 Customers by Total Spend...")

    df = data["fact"].join(
        data["dim_customers"].select("customer_id", "first_name", "last_name", "account_type"),
        "customer_id"
    ).groupBy("customer_id", "first_name", "last_name", "account_type").agg(
        spark_round(spark_sum("amount"), 2).alias("total_spend"),
        count("*").alias("tx_count")
    ).orderBy(desc("total_spend")).limit(10).toPandas()

    df["name"] = df["first_name"] + " " + df["last_name"]

    fig, ax = plt.subplots(figsize=(12, 6))

    acct_colors = {"Checking": "#6366f1", "Savings": "#34d399", "Credit": "#f472b6"}
    bar_colors = [acct_colors.get(t, "#818cf8") for t in df["account_type"]]

    bars = ax.barh(df["name"], df["total_spend"], color=bar_colors, alpha=0.85, height=0.6)

    # Add value labels
    for bar, val in zip(bars, df["total_spend"]):
        ax.text(bar.get_width() + 500, bar.get_y() + bar.get_height()/2,
                f"${val:,.0f}", va="center", fontsize=9, color="#e0e0ff")

    ax.set_xlabel("Total Spend ($)")
    ax.set_title("Top 10 Customers by Total Spend")
    ax.invert_yaxis()
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.grid(axis="x", alpha=0.3)

    # Legend for account types
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor=c, label=l) for l, c in acct_colors.items()]
    ax.legend(handles=legend_elements, title="Account Type", loc="lower right")

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "03_top_customers.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✓ Saved 03_top_customers.png")


# ═══════════════════════════════════════════════════════════
# CHART 4: Revenue by Account Type
# ═══════════════════════════════════════════════════════════

def chart_revenue_by_account_type(data):
    """Generate revenue breakdown by account type."""
    print("\n[Chart 4/6] Revenue by Account Type...")

    df = data["fact"].join(
        data["dim_customers"].select("customer_id", "account_type"),
        "customer_id"
    ).groupBy("account_type", "transaction_type").agg(
        spark_round(spark_sum("amount"), 2).alias("revenue")
    ).toPandas()

    pivot = df.pivot_table(index="account_type", columns="transaction_type",
                           values="revenue", fill_value=0)

    fig, ax = plt.subplots(figsize=(10, 6))

    colors = ["#6366f1", "#f472b6", "#34d399", "#fbbf24"]
    pivot.plot(kind="bar", stacked=True, ax=ax, color=colors[:len(pivot.columns)], alpha=0.85)

    ax.set_xlabel("Account Type")
    ax.set_ylabel("Revenue ($)")
    ax.set_title("Revenue Breakdown by Account Type & Transaction Type")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.legend(title="Transaction Type", bbox_to_anchor=(1.05, 1), loc="upper left")
    ax.set_xticklabels(pivot.index, rotation=0)
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "04_revenue_by_account.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✓ Saved 04_revenue_by_account.png")


# ═══════════════════════════════════════════════════════════
# CHART 5: Anomaly Rate Over Time
# ═══════════════════════════════════════════════════════════

def chart_anomaly_rate(data):
    """Generate anomaly detection rate over time."""
    print("\n[Chart 5/6] Anomaly Rate Over Time...")

    df = data["fact"].groupBy("tx_year", "tx_month").agg(
        count("*").alias("total"),
        spark_sum(when(col("is_anomaly") == True, 1).otherwise(0)).alias("anomalies")
    ).orderBy("tx_year", "tx_month").toPandas()

    df["period"] = df["tx_year"].astype(str) + "-" + df["tx_month"].astype(str).str.zfill(2)
    df["anomaly_rate"] = (df["anomalies"] / df["total"] * 100).round(2)

    fig, ax1 = plt.subplots(figsize=(14, 6))

    # Anomaly count bars
    ax1.bar(df["period"], df["anomalies"], color="#ef4444", alpha=0.6, label="Anomaly Count")
    ax1.set_xlabel("Month")
    ax1.set_ylabel("Anomaly Count", color="#ef4444")
    ax1.tick_params(axis="y", labelcolor="#ef4444")

    # Anomaly rate line
    ax2 = ax1.twinx()
    ax2.plot(df["period"], df["anomaly_rate"], color="#fbbf24", marker="D",
             linewidth=2.5, markersize=5, label="Anomaly Rate (%)")
    ax2.set_ylabel("Anomaly Rate (%)", color="#fbbf24")
    ax2.tick_params(axis="y", labelcolor="#fbbf24")

    plt.title("Anomaly Detection Rate Over Time", pad=20)
    ax1.set_xticklabels(df["period"], rotation=45, ha="right")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")

    ax1.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "05_anomaly_rate.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✓ Saved 05_anomaly_rate.png")


# ═══════════════════════════════════════════════════════════
# CHART 6: Transaction Heatmap (Day-of-Week × Hour)
# ═══════════════════════════════════════════════════════════

def chart_transaction_heatmap(data):
    """Generate transaction volume heatmap by day-of-week and hour."""
    print("\n[Chart 6/6] Transaction Heatmap...")

    df = data["fact"].groupBy("tx_day_of_week", "tx_hour").agg(
        count("*").alias("tx_count")
    ).toPandas()

    # Create pivot table
    pivot = df.pivot_table(
        index="tx_day_of_week", columns="tx_hour", values="tx_count", fill_value=0
    ).reindex(range(1, 8))  # Days 1-7

    day_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

    fig, ax = plt.subplots(figsize=(16, 5))

    # Custom colormap
    from matplotlib.colors import LinearSegmentedColormap
    colors_list = ["#0f0f23", "#1e1b4b", "#3730a3", "#6366f1", "#818cf8", "#c4b5fd"]
    cmap = LinearSegmentedColormap.from_list("banking", colors_list)

    im = ax.imshow(pivot.values, aspect="auto", cmap=cmap, interpolation="nearest")

    ax.set_xticks(range(24))
    ax.set_xticklabels([f"{h:02d}:00" for h in range(24)], rotation=45, ha="right", fontsize=8)
    ax.set_yticks(range(7))
    ax.set_yticklabels(day_labels)
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Day of Week")
    ax.set_title("Transaction Volume Heatmap (Day × Hour)")

    # Add colorbar
    cbar = plt.colorbar(im, ax=ax, pad=0.02)
    cbar.set_label("Transaction Count", color="#e0e0ff")
    cbar.ax.yaxis.set_tick_params(color="#a0a0d0")
    plt.setp(plt.getp(cbar.ax.axes, "yticklabels"), color="#a0a0d0")

    # Add text annotations for high values
    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            val = pivot.values[i, j]
            if val > pivot.values.mean():
                ax.text(j, i, f"{int(val)}", ha="center", va="center",
                       fontsize=7, color="white", fontweight="bold")

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "06_transaction_heatmap.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  ✓ Saved 06_transaction_heatmap.png")


# ═══════════════════════════════════════════════════════════
# SUMMARY DASHBOARD (All Charts Combined)
# ═══════════════════════════════════════════════════════════

def generate_summary(data):
    """Generate a text-based summary of key metrics."""
    print("\n[Summary] Generating pipeline metrics summary...")

    fact = data["fact"]
    customers = data["dim_customers"]

    total_tx = fact.count()
    total_revenue = fact.agg(spark_round(spark_sum("amount"), 2)).collect()[0][0]
    avg_tx = fact.agg(spark_round(spark_avg("amount"), 2)).collect()[0][0]
    total_customers = customers.count()
    anomaly_count = fact.filter(col("is_anomaly") == True).count()
    anomaly_rate = round(anomaly_count / total_tx * 100, 2)

    summary = f"""
╔══════════════════════════════════════════════════════════╗
║            BANKING CRM — DASHBOARD SUMMARY              ║
╠══════════════════════════════════════════════════════════╣
║                                                          ║
║  Total Transactions:    {total_tx:>10,}                      ║
║  Total Revenue:       ${total_revenue:>12,.2f}                ║
║  Avg Transaction:     ${avg_tx:>12,.2f}                ║
║  Total Customers:       {total_customers:>10,}                      ║
║  Anomalies Detected:    {anomaly_count:>10,}                      ║
║  Anomaly Rate:          {anomaly_rate:>9}%                      ║
║                                                          ║
╚══════════════════════════════════════════════════════════╝
"""
    print(summary)

    # Save summary to file
    with open(os.path.join(OUTPUT_DIR, "summary_metrics.txt"), "w") as f:
        f.write(summary)


def main():
    spark = create_spark_session()

    try:
        # Create output directory
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Setup chart style
        setup_style()

        # Load data
        data = load_warehouse_data(spark)

        # Generate all charts
        chart_monthly_revenue(data)
        chart_transaction_type_distribution(data)
        chart_top_customers(data)
        chart_revenue_by_account_type(data)
        chart_anomaly_rate(data)
        chart_transaction_heatmap(data)

        # Generate summary
        generate_summary(data)

        # Upload charts to HDFS
        print("\n[Upload] Uploading dashboard to HDFS...")
        os.system(f"HADOOP_USER_NAME=hadoop hdfs dfs -put -f {OUTPUT_DIR}/* /banking_crm/dashboard/")

        print("\n" + "=" * 60)
        print("  ✅ DASHBOARD GENERATION COMPLETE!")
        print(f"  Charts saved to: {OUTPUT_DIR}/")
        print(f"  And uploaded to: hdfs:///banking_crm/dashboard/")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
