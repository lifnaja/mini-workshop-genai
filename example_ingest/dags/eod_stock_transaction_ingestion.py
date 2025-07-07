from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import timedelta
import pendulum
import io
import csv
import tempfile
import os
import pandas as pd # Import pandas
import re           # Import re for regex
from datetime import datetime # Import datetime for date parsing

# --- Constant Variables ---
# SFTP Configuration
SFTP_SOURCE_PATH = '/mock_stock_transactions.csv'

# GCS Configuration
GCS_BUCKET_NAME = 'sample-ingest' # **เปลี่ยนเป็นชื่อ GCS bucket ของคุณ**
GCS_DESTINATION_PATH = 'raw_data/eod_transactions/transactions.csv'

# BigQuery Configuration
BQ_PROJECT_ID = 'deb05-463203' # **เปลี่ยนเป็น Project ID ของคุณ**
BQ_DATASET_ID = 'sample_ingest'     # **เปลี่ยนเป็น Dataset ID ของคุณ**
BQ_TABLE_ID = 'eod_transactions' # **เปลี่ยนเป็น Table ID ของคุณ**
BQ_DESTINATION_TABLE = f'{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}'

# Airflow Connection IDs
SFTP_CONN_ID = 'sftp_conn'
GCP_CONN_ID = 'gcp_conn'

# Expected CSV Schema (Column Names)
EXPECTED_CSV_HEADERS = [
    'type_of_order',
    'order_no',
    'account_no',
    'sec_symbol',
    'trading_datetime',
    'trading_unit',
    'trading_price',
    'trading_amt'
]
# --- End Constant Variables ---

# กำหนด default_args สำหรับ DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- Python Function for Schema Validation ---
def _check_csv_schema(bucket_name, object_name, expected_headers, gcp_conn_id):
    """
    Checks if the CSV file in GCS has the expected headers by downloading it locally.
    Raises a ValueError if the headers do not match.
    """
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)

    with tempfile.NamedTemporaryFile(mode='w+', delete=True, suffix='.csv') as temp_file:
        local_path = temp_file.name

        print(f"Downloading gs://{bucket_name}/{object_name} to {local_path} for schema check.")
        try:
            gcs_hook.download(bucket_name=bucket_name, object_name=object_name, filename=local_path)
        except Exception as e:
            raise ValueError(f"Failed to download file '{object_name}' from GCS bucket '{bucket_name}': {e}")

        try:
            with open(local_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                try:
                    actual_headers = next(reader)
                except StopIteration:
                    raise ValueError(f"CSV file '{object_name}' is empty or has no header after download.")

                if actual_headers and actual_headers[0].startswith('\ufeff'):
                    actual_headers[0] = actual_headers[0][1:]

                actual_headers = [header.strip() for header in actual_headers]
                expected_headers = [header.strip() for header in expected_headers]

                if actual_headers != expected_headers:
                    raise ValueError(
                        f"Schema mismatch for file '{object_name}'.\n"
                        f"Expected: {expected_headers}\n"
                        f"Actual:   {actual_headers}"
                    )
                print(f"Schema check passed for file '{object_name}'. Headers match expected.")
        except Exception as e:
            raise ValueError(f"Error reading local CSV file '{local_path}' or processing headers: {e}")

# --- End Python Function for Schema Validation ---


# --- Python Function for Data Quality Checks ---
def _run_data_quality_checks(bucket_name, object_name, gcp_conn_id):
    """
    Performs data quality checks on the downloaded CSV file.
    Raises a ValueError if any business rule is violated.
    """
    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)

    with tempfile.NamedTemporaryFile(mode='w+', delete=True, suffix='.csv') as temp_file:
        local_path = temp_file.name

        print(f"Downloading gs://{bucket_name}/{object_name} to {local_path} for data quality check.")
        try:
            gcs_hook.download(bucket_name=bucket_name, object_name=object_name, filename=local_path)
        except Exception as e:
            raise ValueError(f"Failed to download file '{object_name}' from GCS bucket '{bucket_name}' for DQ check: {e}")

        try:
            # Read CSV into a pandas DataFrame
            df = pd.read_csv(local_path, encoding='utf-8')
            print(f"Loaded {len(df)} rows for data quality checks.")

            # 1. ทุก column ห้ามมีค่า NULL หรือค่าว่าง
            # Check for actual NaN (NULL) values
            null_check = df.isnull().sum()
            null_cols = null_check[null_check > 0]
            if not null_cols.empty:
                raise ValueError(f"DQ Check Failed: Null values found in columns:\n{null_cols}")

            # Check for empty strings after stripping whitespace
            # Convert all columns to string type first to apply strip()
            # Then check if any empty string exists
            for col in df.columns:
                if df[col].dtype == 'object': # Only apply to object (string) columns
                    if (df[col].astype(str).str.strip() == '').any():
                        raise ValueError(f"DQ Check Failed: Empty string or whitespace-only value found in column '{col}'.")
            print("DQ Check 1 (No NULL/Empty values): Passed.")

            # 2. type_of_order มีได้แค่ มีแค่ BUY หรือ SELL
            invalid_order_types = df[~df['type_of_order'].isin(['BUY', 'SELL'])]
            if not invalid_order_types.empty:
                raise ValueError(
                    f"DQ Check Failed: Invalid 'type_of_order' found. "
                    f"Expected 'BUY' or 'SELL'. Found: {invalid_order_types['type_of_order'].unique()}"
                )
            print("DQ Check 2 (type_of_order values): Passed.")

            # 3. order_no ห้ามมี duplicates
            duplicates_order_no = df[df.duplicated(subset=['order_no'], keep=False)]
            if not duplicates_order_no.empty:
                raise ValueError(
                    f"DQ Check Failed: Duplicate 'order_no' found. "
                    f"Sample duplicates:\n{duplicates_order_no['order_no'].tolist()[:5]}"
                )
            print("DQ Check 3 (order_no uniqueness): Passed.")

            # 4. trading_datetime ต้องเป็น format 2025-06-08 22:56:23.603794
            # Regex for YYYY-MM-DD HH:MM:SS.microseconds
            datetime_pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}$'
            invalid_datetime = df[~df['trading_datetime'].astype(str).str.fullmatch(datetime_pattern)]
            if not invalid_datetime.empty:
                raise ValueError(
                    f"DQ Check Failed: Invalid 'trading_datetime' format found. "
                    f"Expected 'YYYY-MM-DD HH:MM:SS.microseconds'. Sample invalid values:\n"
                    f"{invalid_datetime['trading_datetime'].tolist()[:5]}"
                )
            # Optional: Further parse to datetime object to ensure convertibility
            try:
                pd.to_datetime(df['trading_datetime'], format='%Y-%m-%d %H:%M:%S.%f')
            except ValueError as e:
                raise ValueError(f"DQ Check Failed: 'trading_datetime' values cannot be parsed to datetime: {e}")
            print("DQ Check 4 (trading_datetime format): Passed.")

            # 5. trading_amt = trading_unit * trading_price
            # Convert to numeric types first, handle potential errors with errors='coerce'
            df['trading_unit'] = pd.to_numeric(df['trading_unit'], errors='coerce')
            df['trading_price'] = pd.to_numeric(df['trading_price'], errors='coerce')
            df['trading_amt'] = pd.to_numeric(df['trading_amt'], errors='coerce')

            # Drop rows where conversion failed (already covered by NULL check, but good practice)
            df_cleaned = df.dropna(subset=['trading_unit', 'trading_price', 'trading_amt'])

            # Calculate expected amount and compare, allowing for small floating point discrepancies
            # Use np.isclose for robust floating-point comparison
            import numpy as np # Import numpy for np.isclose
            expected_amt = df_cleaned['trading_unit'] * df_cleaned['trading_price']
            # Allow for a small relative tolerance (rtol) or absolute tolerance (atol) for floats
            discrepancy_check = ~np.isclose(df_cleaned['trading_amt'], expected_amt, rtol=1e-6, atol=1e-6)

            if discrepancy_check.any():
                invalid_amt_rows = df_cleaned[discrepancy_check]
                # To show more context, maybe print the first few problematic rows
                print(f"Sample problematic rows for DQ Check 5:\n{invalid_amt_rows[['trading_unit', 'trading_price', 'trading_amt']].head()}")
                raise ValueError(
                    f"DQ Check Failed: 'trading_amt' does not match 'trading_unit' * 'trading_price' "
                    f"for {len(invalid_amt_rows)} row(s)."
                )
            print("DQ Check 5 (trading_amt calculation): Passed.")

            print("All Data Quality Checks Passed!")

        except Exception as e:
            # Re-raise the exception to fail the Airflow task
            raise e

# --- End Python Function for Data Quality Checks ---


# กำหนด DAG
with DAG(
    dag_id='eod_stock_data_ingestion',
    default_args=default_args,
    description='Ingest End-of-Day stock trading data from SFTP to GCS and then to BigQuery',
    schedule='@daily',
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=['eod', 'stock', 'sftp', 'gcs', 'bigquery'],
) as dag:

    # Task 1: โหลดไฟล์ CSV จาก SFTP ไปยัง GCS
    transfer_sftp_to_gcs = SFTPToGCSOperator(
        task_id='transfer_sftp_to_gcs',
        sftp_conn_id=SFTP_CONN_ID,
        source_path=SFTP_SOURCE_PATH,
        destination_path=GCS_DESTINATION_PATH,
        destination_bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID,
    )

    # Task 2: ตรวจสอบ Schema ของไฟล์ CSV ใน GCS
    check_csv_schema = PythonOperator(
        task_id='check_csv_schema',
        python_callable=_check_csv_schema,
        op_kwargs={
            'bucket_name': GCS_BUCKET_NAME,
            'object_name': GCS_DESTINATION_PATH,
            'expected_headers': EXPECTED_CSV_HEADERS,
            'gcp_conn_id': GCP_CONN_ID,
        },
    )

    # Task 3: รัน Data Quality Checks
    run_data_quality_checks = PythonOperator(
        task_id='run_data_quality_checks',
        python_callable=_run_data_quality_checks,
        op_kwargs={
            'bucket_name': GCS_BUCKET_NAME,
            'object_name': GCS_DESTINATION_PATH,
            'gcp_conn_id': GCP_CONN_ID,
        },
    )

    # Task 4: โหลดข้อมูลจาก GCS ไปยัง BigQuery
    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        bucket=GCS_BUCKET_NAME,
        source_objects=[GCS_DESTINATION_PATH],
        destination_project_dataset_table=BQ_DESTINATION_TABLE,
        source_format='CSV',
        autodetect=True,
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id=GCP_CONN_ID,
    )

    # กำหนดลำดับการทำงานของ Tasks
    transfer_sftp_to_gcs >> check_csv_schema >> run_data_quality_checks >> load_gcs_to_bigquery