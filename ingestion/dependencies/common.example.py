from datetime import datetime
from google.cloud import storage, bigquery
import os
import csv
import sys
import os

X_RIOT_TOKEN="YOUR_RIOT_TOKEN"
HOST="RIOT_HOST"
HOST_EU="RIOT_HOST_EU"

SUMMONERS_SIZE="NUMBER_SUMMONERS_PER_TIER_DIVISION"

CLOUD_STORAGE_BUCKET="GCS_BUCKET"
CLOUD_STORAGE_DATA_DIR="GCS_DATA_DIRECTORY"
CLOUD_STORAGE_DATA_TEMP='GCS_TEMP_DATA_DIRECTORY'
CLOUD_STORAGE_SIDE_INPUT_DIR="GCS_SIDE_INPUT_DIRECTORY"
CHAMPIONS_TABLE = "PROJECT.DATASET.CHAMPS_TABLE"
MATCHES_TABLE = "PROJECT.DATASET.MATCHES_TABLE"

current_directory = os.path.dirname(__file__)
data_directory = os.path.join(current_directory, '..', 'data')

# enable if run in local
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "PATH_TO_GOOGLE_ACCOUNT_SERVICE_AUTH_FILE"

storage_client = storage.Client()
bigquery_client = bigquery.Client()
bucket = storage_client.get_bucket(CLOUD_STORAGE_BUCKET)

def get_date_label(date: datetime):
    year = date.year
    month = date.month
    day = date.day

    return str(year) + str(month).rjust(2,'0') + str(day).rjust(2,'0')

def read_csv(path):
    with open(path, "r", encoding='utf-8') as f:
        csv_reader = csv.DictReader(f)
        data = list(csv_reader)

    return data 

def write_csv(data, path, mode, keys):
    try:
        with open(path, mode, newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)
    except:
        print(sys.exc_info()[0], sys.exc_info()[1])

def load_csv_to_bigquery(file_path, table_id):

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
    )

    with open(file_path, "rb") as source_file:
        job = bigquery_client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.