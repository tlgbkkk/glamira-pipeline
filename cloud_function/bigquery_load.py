import logging
from google.cloud import bigquery
from src.config import PROJECT_ID, DATASET_ID

bq_client = bigquery.Client()

def trigger_bigquery_load(event, context):
    file_name = event['name']
    bucket_name = event['bucket']

    if not file_name.endswith('.jsonl') or 'exports/' not in file_name:
        logging.info(f"Skipping irrelevant file: {file_name}")
        return

    try:
        table_name = file_name.split('/')[1]
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
        ignore_unknown_values=True
    )

    try:
        load_job = bq_client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        load_job.result()

        table = bq_client.get_table(table_id)
        logging.info(f"Success! Loaded {load_job.output_rows} rows. Table {table_id} now has {table.num_rows} rows.")
    except Exception as e:
        logging.error(f"Failed to load data into BigQuery: {e}")