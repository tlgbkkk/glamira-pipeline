import logging
import os
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT")
DATASET_ID = os.environ.get("DATASET_ID")

bq_client = bigquery.Client()


def trigger_bigquery_load(event, context):
    file_name = event['name']
    bucket_name = event['bucket']

    if not file_name.endswith('.jsonl') or 'exports/' not in file_name:
        logging.info(f"Skipping irrelevant file: {file_name}")
        return

    try:
        parts = file_name.split('/')
        table_name = parts[1]
    except IndexError:
        logging.warning(f"Could not parse table name from file: {file_name}")
        return

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    uri = f"gs://{bucket_name}/{file_name}"

    logging.info(f"Starting BigQuery load job: {uri} -> {table_id}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,

        ignore_unknown_values=True,

        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        ],

        max_bad_records=100
    )

    try:
        load_job = bq_client.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config
        )

        load_job.result()

        table = bq_client.get_table(table_id)
        logging.info(
            f"Success! Loaded {load_job.output_rows} rows. "
            f"Table {table_id} now has {table.num_rows} rows."
        )

    except Exception as e:
        logging.error(f"❌ Failed to load data into BigQuery: {e}")