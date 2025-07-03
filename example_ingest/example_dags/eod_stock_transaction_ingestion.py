from airflow import DAG
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pendulum
from airflow.operators.python import PythonOperator
import paramiko

BUCKET_NAME = "sample-ingest"
TABLE_ID = "deb05-463203.sample_ingest.eod_transactions"

with DAG(
    dag_id="eod_stock_transaction_ingestion",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Bangkok"),
    schedule=None,
    catchup=False,
    tags=["eod", "stock", "transaction", "sftp", "gcs", "bigquery"],
    description="Ingest End-of-Day stock transaction data from SFTP to GCS and then to BigQuery.",
) as dag:


    def _check_sftp_file():
        transport = paramiko.Transport(("sftp-server", 22))
        transport.connect(username="demo", password="demo")
        sftp = paramiko.SFTPClient.from_transport(transport)

        try:
            sftp.stat("/stock_trades.csv")  # ถ้าไม่เจอจะ raise
            print("✅ File exists")
        except FileNotFoundError:
            print("❌ File not found")
            
        finally:
            sftp.close()
            transport.close()

    check_sftp_file = PythonOperator(
        task_id="check_sftp_file",
        python_callable=_check_sftp_file,
    )

    sftp_to_gcs = SFTPToGCSOperator(
        task_id="sftp_to_gcs_eod_transactions",
        source_path="/stock_trades.csv",
        destination_bucket=BUCKET_NAME,
        destination_path="raw_data/eod_transactions/transactions.csv",
        gcp_conn_id="gcp_conn",
        sftp_conn_id="sftp_connection",
    )

    
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_eod_transactions",
        bucket=BUCKET_NAME,
        source_objects=["raw_data/eod_transactions/transactions.csv"],
        destination_project_dataset_table=TABLE_ID,
        autodetect=True,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id="gcp_conn",
    )

    check_sftp_file >> sftp_to_gcs  >> gcs_to_bigquery
