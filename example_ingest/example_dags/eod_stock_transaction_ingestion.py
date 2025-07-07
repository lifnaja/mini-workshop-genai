from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pendulum

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
    # ตรวจสอบให้แน่ใจว่าได้ตั้งค่า Connection ID 'sftp_conn' และ 'gcp_conn' ใน Airflow UI แล้ว
    transfer_sftp_to_gcs = SFTPToGCSOperator(
        task_id='transfer_sftp_to_gcs',
        sftp_conn_id=SFTP_CONN_ID,
        source_path=SFTP_SOURCE_PATH,
        destination_path=GCS_DESTINATION_PATH,
        destination_bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID,
    )

    # Task 2: โหลดข้อมูลจาก GCS ไปยัง BigQuery
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
    transfer_sftp_to_gcs >> load_gcs_to_bigquery