"""
Phase 5: Workflow Orchestration — Apache Airflow DAG.

Task Alignment:
  - Task 22: Airflow Basics Task
  - Task 23: Airflow Advanced Task

This DAG orchestrates the complete Banking CRM ETL pipeline:
  1. Extract data from MySQL → HDFS
  2. Transform & validate data using PySpark
  3. Build star schema data warehouse
  4. Verify pipeline outputs on HDFS

Features:
  - Scheduled to run daily at 2:00 AM
  - Retry logic: 2 retries with 5-minute delay
  - Task dependencies enforce correct execution order
  - Email alerts on failure (configurable)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# ═══════════════════════════════════════════════════════════
# DAG Configuration
# ═══════════════════════════════════════════════════════════

default_args = {
    "owner": "banking_crm_team",
    "depends_on_past": False,
    "email": ["admin@bankingcrm.local"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

dag = DAG(
    dag_id="banking_crm_etl_pipeline",
    default_args=default_args,
    description="End-to-end Banking CRM ETL: MySQL → HDFS → Spark → Warehouse",
    schedule_interval="0 2 * * *",  # Daily at 2:00 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["banking", "etl", "crm", "data-engineering"],
    max_active_runs=1,
)


# ═══════════════════════════════════════════════════════════
# Task 1: Create HDFS Directory Structure
# ═══════════════════════════════════════════════════════════

create_hdfs_dirs = BashOperator(
    task_id="create_hdfs_directories",
    bash_command="""
        echo "Creating HDFS directory structure..."
        HADOOP_USER_NAME=hadoop hdfs dfs -mkdir -p /banking_crm/raw
        HADOOP_USER_NAME=hadoop hdfs dfs -mkdir -p /banking_crm/processed
        HADOOP_USER_NAME=hadoop hdfs dfs -mkdir -p /banking_crm/warehouse
        HADOOP_USER_NAME=hadoop hdfs dfs -mkdir -p /banking_crm/quality_reports
        HADOOP_USER_NAME=hadoop hdfs dfs -mkdir -p /banking_crm/dashboard
        echo "✅ HDFS directories created."
    """,
    dag=dag,
)


# ═══════════════════════════════════════════════════════════
# Task 2: Extract Data from MySQL to HDFS
# ═══════════════════════════════════════════════════════════

extract_data = BashOperator(
    task_id="extract_data_to_hdfs",
    bash_command="""
        echo "Starting data extraction from MySQL..."
        cd /opt/airflow/scripts
        python extract_to_hdfs.py
        echo "✅ Extraction complete."
    """,
    dag=dag,
)


# ═══════════════════════════════════════════════════════════
# Task 3: Transform Data with PySpark
# ═══════════════════════════════════════════════════════════

transform_data = BashOperator(
    task_id="transform_and_validate_data",
    bash_command="""
        echo "Starting Spark transformation and quality checks..."
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 1g \
            --executor-memory 1g \
            /opt/airflow/scripts/transform_data.py
        echo "✅ Transformation complete."
    """,
    dag=dag,
)


# ═══════════════════════════════════════════════════════════
# Task 4: Build Data Warehouse (Star Schema)
# ═══════════════════════════════════════════════════════════

build_warehouse = BashOperator(
    task_id="build_data_warehouse",
    bash_command="""
        echo "Building star schema data warehouse..."
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 1g \
            --executor-memory 1g \
            /opt/airflow/scripts/build_warehouse.py
        echo "✅ Warehouse build complete."
    """,
    dag=dag,
)


# ═══════════════════════════════════════════════════════════
# Task 5: Verify Pipeline Outputs
# ═══════════════════════════════════════════════════════════

def verify_pipeline_outputs(**kwargs):
    """Verify that all expected outputs exist on HDFS."""
    import subprocess
    
    expected_paths = [
        "/banking_crm/raw/raw_customers.csv",
        "/banking_crm/raw/raw_transactions.csv",
        "/banking_crm/processed/fact_transactions",
        "/banking_crm/warehouse/dim_customers",
        "/banking_crm/warehouse/dim_date",
        "/banking_crm/warehouse/dim_transaction_type",
        "/banking_crm/warehouse/fact_transactions",
    ]
    
    all_passed = True
    for path in expected_paths:
        result = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", path],
            env={"HADOOP_USER_NAME": "hadoop", **__import__("os").environ},
            capture_output=True
        )
        status = "✓ EXISTS" if result.returncode == 0 else "✗ MISSING"
        if result.returncode != 0:
            all_passed = False
        print(f"  {status}: {path}")
    
    if not all_passed:
        raise Exception("Pipeline verification failed — some outputs are missing!")
    
    print("\n✅ All pipeline outputs verified successfully!")


verify_outputs = PythonOperator(
    task_id="verify_pipeline_outputs",
    python_callable=verify_pipeline_outputs,
    dag=dag,
)


# ═══════════════════════════════════════════════════════════
# Task 6: Generate Dashboard
# ═══════════════════════════════════════════════════════════

generate_dashboard = BashOperator(
    task_id="generate_dashboard",
    bash_command="""
        echo "Generating dashboard visualizations..."
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 1g \
            --executor-memory 1g \
            /opt/airflow/scripts/dashboard.py
        echo "✅ Dashboard generation complete."
    """,
    dag=dag,
)


# ═══════════════════════════════════════════════════════════
# Pipeline — Task Dependencies (DAG Structure)
# ═══════════════════════════════════════════════════════════
#
#  create_hdfs_dirs → extract_data → transform_data → build_warehouse → verify_outputs → generate_dashboard
#

create_hdfs_dirs >> extract_data >> transform_data >> build_warehouse >> verify_outputs >> generate_dashboard
