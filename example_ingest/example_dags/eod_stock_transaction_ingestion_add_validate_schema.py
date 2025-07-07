from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import timedelta
import pendulum
import io
import csv
import tempfile # Import tempfile สำหรับสร้างไฟล์ชั่วคราว
import os       # Import os สำหรับจัดการไฟล์ (ถ้าจำเป็น, แต่ tempfile จัดการเองได้)

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

    # Use tempfile to create a temporary file that will be automatically deleted
    with tempfile.NamedTemporaryFile(mode='w+', delete=True, suffix='.csv') as temp_file:
        local_path = temp_file.name # Get the path to the temporary file

        print(f"Downloading gs://{bucket_name}/{object_name} to {local_path} for schema check.")
        try:
            gcs_hook.download(bucket_name=bucket_name, object_name=object_name, filename=local_path)
        except Exception as e:
            raise ValueError(f"Failed to download file '{object_name}' from GCS bucket '{bucket_name}': {e}")

        # After download, read the file from the local path
        # You might need to specify encoding if your CSV is not utf-8
        try:
            with open(local_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                try:
                    actual_headers = next(reader) # Read the first row (header)
                except StopIteration:
                    raise ValueError(f"CSV file '{object_name}' is empty or has no header after download.")

                # Remove BOM if present (common for CSVs from Windows)
                if actual_headers and actual_headers[0].startswith('\ufeff'):
                    actual_headers[0] = actual_headers[0][1:]

                # Strip whitespace from headers for robust comparison
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

# --- End Python Function ---


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

    # Task 3: โหลดข้อมูลจาก GCS ไปยัง BigQuery
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
    transfer_sftp_to_gcs >> check_csv_schema >> load_gcs_to_bigquery