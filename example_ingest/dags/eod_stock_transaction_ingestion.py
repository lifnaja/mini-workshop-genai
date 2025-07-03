from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import numpy as np
import pandas as pd
from pandas.api.types import is_string_dtype, is_numeric_dtype, is_datetime64_any_dtype
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

    def _validate_csv_schema(**context):
        expected_schema = {
            "type_of_order": "string",
            "order_no": "string",
            "account_no": "string",
            "sec_symbol": "string",
            "trading_datetime": "datetime64[ns]",
            "trading_unit": "float64",
            "trading_price": "float64",
            "trading_amt": "float64",
        }

        gcs_path = "raw_data/eod_transactions/transactions.csv"
        local_path = "/tmp/temp_eod.csv"

        gcs_hook = GCSHook(gcp_conn_id="gcp_conn")
        gcs_hook.download(bucket_name=BUCKET_NAME, object_name=gcs_path, filename=local_path)

        # Read file with Pandas
        df = pd.read_csv(local_path, parse_dates=["trading_datetime"])

        # Check if column names match
        if list(df.columns) != list(expected_schema.keys()):
            raise ValueError(f"Column mismatch. Expected: {list(expected_schema.keys())}, Got: {list(df.columns)}")

        # Check dtype
        for col, expected_type in expected_schema.items():
            actual_dtype = df[col].dtype

            if expected_type == "string" and not is_string_dtype(df[col]):
                raise TypeError(f"Column '{col}' expected to be string-like, but got {actual_dtype}")
            elif expected_type.startswith("float") and not is_numeric_dtype(df[col]):
                raise TypeError(f"Column '{col}' expected to be numeric, but got {actual_dtype}")
            elif expected_type.startswith("datetime") and not is_datetime64_any_dtype(df[col]):
                raise TypeError(f"Column '{col}' expected to be datetime, but got {actual_dtype}")

        print("✅ Schema validated successfully")

    validate_task = PythonOperator(
        task_id="validate_csv_schema",
        python_callable=_validate_csv_schema,
    )

    def _data_quality_gate(**context):
        gcs_path = "raw_data/eod_transactions/transactions.csv"
        local_file = "/tmp/temp_eod.csv"

        gcs_hook = GCSHook(gcp_conn_id="gcp_conn")
        gcs_hook.download(bucket_name=BUCKET_NAME, object_name=gcs_path, filename=local_file)

        df = pd.read_csv(local_file)

        # Rule 1: ทุก column ห้ามมี NULL หรือว่าง
        if df.isnull().values.any():
            raise ValueError("❌ พบ NULL ในข้อมูล")
        if (df == "").any().any():
            raise ValueError("❌ พบค่าค่าว่าง ('') ในข้อมูล")

        # Rule 2: type_of_order ต้องเป็น BUY หรือ SELL
        valid_orders = {"BUY", "SELL"}
        invalid_orders = df[~df["type_of_order"].isin(valid_orders)]
        if not invalid_orders.empty:
            raise ValueError(f"❌ พบค่า type_of_order ที่ไม่ถูกต้อง: {invalid_orders['type_of_order'].unique()}")

        # Rule 3: order_no ห้ามซ้ำ
        if df["order_no"].duplicated().any():
            duplicates = df[df["order_no"].duplicated()]["order_no"].tolist()
            raise ValueError(f"❌ พบ order_no ซ้ำ: {duplicates}")

        # Rule 4: trading_datetime ต้องเป็น format 2025-06-01 09:01:38
        def is_valid_datetime_format(dt_str):
            try:
                datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                return True
            except ValueError:
                return False

        invalid_dt = df[~df["trading_datetime"].apply(is_valid_datetime_format)]
        if not invalid_dt.empty:
            raise ValueError(f"❌ พบ trading_datetime format ไม่ถูกต้อง ใน {len(invalid_dt)} records")

        # Rule 5: trading_amt = trading_unit * trading_price (ตรวจสอบความแม่นยำ 2 ตำแหน่ง)
        df["calc_amt"] = df["trading_unit"] * df["trading_price"]
        if not np.allclose(df["trading_amt"], df["calc_amt"], rtol=1e-2):
            raise ValueError("❌ trading_amt ไม่ตรงกับ trading_unit * trading_price")

        if (df["trading_unit"] <= 0).any() or (df["trading_price"] <= 0).any():
            raise ValueError("❌ trading_unit or trading_price has non-positive value")

        print("✅ Data quality passed")

    data_quality_task = PythonOperator(
        task_id="data_quality_gate",
        python_callable=_data_quality_gate,
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

    (
        check_sftp_file
        >> sftp_to_gcs
        >> validate_task
        >> data_quality_task
        >> gcs_to_bigquery
    )
