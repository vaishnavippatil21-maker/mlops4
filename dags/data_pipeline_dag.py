from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# PATHS
DATA_FILE = "/opt/airflow/data/input_data.csv"
OUTPUT_DIR = "/opt/airflow/output"
PROCESSED_FILE = os.path.join(OUTPUT_DIR, "processed_data.csv")
VALIDATION_FILE = os.path.join(OUTPUT_DIR, "validation_report.txt")
FINAL_REPORT = os.path.join(OUTPUT_DIR, "final_report.txt")

# TASK 1: DATA EXTRACTION
def extract_data():
    print("Starting Data Extraction...")
    df = pd.read_csv(DATA_FILE)
    print("Data extracted successfully.")
    print(f"Number of records extracted: {len(df)}")
    print("Columns:")
    print(list(df.columns))
    print("\nFirst 5 records:")
    print(df.head())

# TASK 2: DATA VALIDATION
def validate_data():
    print("Starting Data Validation...")
    df = pd.read_csv(DATA_FILE)
    missing_values = df.isnull().sum()
    total_missing = missing_values.sum()
    print("\nMissing values by column:")
    print(missing_values)
    print(f"\nTotal missing values: {total_missing}")
    with open(VALIDATION_FILE, "w") as file:
        file.write("DATA VALIDATION REPORT\n")
        file.write(f"Total records: {len(df)}\n")
        file.write(f"Total missing values: {total_missing}\n\n")
        file.write("Missing values by column:\n")
        for column, count in missing_values.items():
            file.write(f"{column}: {count}\n")
    print("Validation report generated successfully.")

# TASK 3: DATA PROCESSING
def process_data():
    print("Starting Data Processing...")
    df = pd.read_csv(DATA_FILE)
    print(f"Records before processing: {len(df)}")
    # Fill missing numerical values
    df["age"] = df["age"].fillna(df["age"].median())
    df["salary"] = df["salary"].fillna(df["salary"].median())
    # Remove duplicate records
    df = df.drop_duplicates()
    # Save processed dataset
    df.to_csv(PROCESSED_FILE, index=False)
    print(f"Records after processing: {len(df)}")
    print("Processed data saved successfully.")

# TASK 4: REPORT GENERATION
def generate_report():
    print("Generating final report...")
    df_original = pd.read_csv(DATA_FILE)
    df_processed = pd.read_csv(PROCESSED_FILE)
    missing_values = df_original.isnull().sum().sum()
    with open(FINAL_REPORT, "w") as file:
        file.write("AIRFLOW DATA PIPELINE REPORT\n")
        file.write(f"Number of records processed: {len(df_processed)}\n")
        file.write(f"Missing values identified: {missing_values}\n")
        file.write("Processing completion status: SUCCESS\n")
    print("Final report generated successfully.")

# TASK 5: NOTIFICATION
def send_notification():
    print("=" * 50)
    print("NOTIFICATION")
    print("=" * 50)
    print("Data pipeline completed successfully!")
    print("All Airflow tasks finished successfully.")
    print("=" * 50)

# AIRFLOW DAG
with DAG(
    dag_id="data_pipeline_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["MLOps", "Assignment4"]
) as dag:
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data
    )
    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_data
    )
    process = PythonOperator(
        task_id="process",
        python_callable=process_data
    )
    report = PythonOperator(
        task_id="report",
        python_callable=generate_report
    )
    notify = PythonOperator(
        task_id="notify",
        python_callable=send_notification
    )
    # Task dependency
    extract >> validate >> process >> report >> notify