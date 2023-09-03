import json
import os

from google.cloud import bigquery
from google.oauth2 import service_account


DATA_FOLDER = "data"

#for partition table decision -> partition when it contains "created at"

# ตัวอย่างการกำหนด Path ของ Keyfile ในแบบที่ใช้ Environment Variable มาช่วย
# จะทำให้เราไม่ต้อง Hardcode Path ของไฟล์ไว้ในโค้ดของเรา
# keyfile = os.environ.get("KEYFILE_PATH")

# keyfile = "YOUR_KEYFILE_PATH"
keyfile = "services/deb2-de2-loading-data-to-bigquery.json"
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "deb2-2100001-395008"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)
dataset_name = "my_deb_workshop"

#define function
def fn_write_log(table_id, table):
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

def fn_load_wo_partition(table_name):
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    data = table_name
    file_path = f"{DATA_FOLDER}/{data}.csv"
    with open(file_path, "rb") as f:
        table_id = f"{project_id}.{dataset_name}.{data}"
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

    fn_write_log(table_id, client.get_table(table_id))

# ----------


def fn_load_w_partition(table_name, partition_col, partition_value):
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_col,
            # field="created_at",
        ),
    )

    dt = partition_value
    partition = dt.replace("-", "") #to make it into ฺBigquery partition pattern
    data = table_name
    file_path = f"{DATA_FOLDER}/{data}.csv"
    with open(file_path, "rb") as f:
        table_id = f"{project_id}.{dataset_name}.{data}${partition}"
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

    fn_write_log(table_id, client.get_table(table_id))


#create last delta date/datetime configuration
#to insert as incremental for "Date"

#main program
fn_load_wo_partition("addresses")
fn_load_w_partition("events", "created_at", "2021-02-10")
fn_load_w_partition("orders", "created_at", "2021-02-10")
fn_load_wo_partition("order_items")
fn_load_wo_partition("products")
fn_load_wo_partition("promos")
fn_load_w_partition("users", "created_at", "2020-10-23")
