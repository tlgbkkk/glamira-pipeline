import io
import json
import logging
import os

from google.cloud import bigquery, storage

PROJECT_ID = os.environ.get("GCP_PROJECT")
DATASET_ID = os.environ.get("DATASET_ID")

bq_client  = bigquery.Client()
gcs_client = storage.Client()

BATCH_SIZE = 5000

SCHEMAS = {
    "summary_raw": [
        bigquery.SchemaField("_id",                "STRING", mode="NULLABLE"),
        bigquery.SchemaField("time_stamp",         "INT64",  mode="NULLABLE"),
        bigquery.SchemaField("ip",                 "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_agent",         "STRING", mode="NULLABLE"),
        bigquery.SchemaField("resolution",         "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_id_db",         "STRING", mode="NULLABLE"),
        bigquery.SchemaField("device_id",          "STRING", mode="NULLABLE"),
        bigquery.SchemaField("api_version",        "STRING", mode="NULLABLE"),
        bigquery.SchemaField("store_id",           "STRING", mode="NULLABLE"),
        bigquery.SchemaField("local_time",         "STRING", mode="NULLABLE"),
        bigquery.SchemaField("show_recommendation","STRING", mode="NULLABLE"),
        bigquery.SchemaField("current_url",        "STRING", mode="NULLABLE"),
        bigquery.SchemaField("referrer_url",       "STRING", mode="NULLABLE"),
        bigquery.SchemaField("email_address",      "STRING", mode="NULLABLE"),
        bigquery.SchemaField("collection",         "STRING", mode="NULLABLE"),
        bigquery.SchemaField("alloy",              "STRING", mode="NULLABLE"),
        bigquery.SchemaField("diamond",            "STRING", mode="NULLABLE"),
        bigquery.SchemaField("shapediamond",       "STRING", mode="NULLABLE"),
        bigquery.SchemaField("cat_id",             "STRING", mode="NULLABLE"),
        bigquery.SchemaField("collect_id",         "STRING", mode="NULLABLE"),
    ],

    "ip_locations": [
        bigquery.SchemaField("_id",     "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ip",      "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city",    "STRING", mode="NULLABLE"),
    ],

    "products": [
        bigquery.SchemaField("_id",         "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("product_id",  "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("sku",         "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("name",        "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("category",    "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("collection",  "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("type",        "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("status",      "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("url",         "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("base_price",  "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("full_price",  "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("sale_price",  "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("currency",    "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("gold_weight", "STRING",  mode="NULLABLE"),
    ],
}

PRICE_FIELDS = {"base_price", "full_price", "sale_price"}

def parse_price(value):
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if "." in s and "," in s:
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        else:
            s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        logging.warning(f"Cannot parse price: '{value}'")
        return None


def flatten_oid(row):
    if isinstance(row.get("_id"), dict) and "$oid" in row["_id"]:
        row["_id"] = row["_id"]["$oid"]
    return row


def transform_summary_raw(row):
    row = flatten_oid(row)
    option = row.pop("option", {}) or {}
    row["alloy"]        = option.get("alloy", "")
    row["diamond"]      = option.get("diamond", "")
    row["shapediamond"] = option.get("shapediamond", "")
    return row


def transform_ip_locations(row):
    return flatten_oid(row)


def transform_products(row):
    row = flatten_oid(row)
    for field in PRICE_FIELDS:
        if field in row:
            row[field] = parse_price(row[field])
    return row


TRANSFORMS = {
    "summary_raw":  transform_summary_raw,
    "ip_locations": transform_ip_locations,
    "products":     transform_products,
}


def iter_lines_from_gcs(bucket_name, file_name):
    blob = gcs_client.bucket(bucket_name).blob(file_name)
    with blob.open("rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield line


def load_batch_to_bq(batch_lines, table_id, job_config):
    data = "\n".join(batch_lines).encode("utf-8")
    load_job = bq_client.load_table_from_file(
        io.BytesIO(data),
        table_id,
        job_config=job_config,
    )
    load_job.result()
    return load_job.output_rows


def trigger_bigquery_load(event, context):
    file_name   = event["name"]
    bucket_name = event["bucket"]

    if not file_name.endswith(".jsonl") or "exports/" not in file_name:
        logging.info(f"Skipping: {file_name}")
        return

    try:
        table_name = file_name.split("/")[1]
    except IndexError:
        logging.warning(f"Cannot parse table name from: {file_name}")
        return

    table_id  = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    schema    = SCHEMAS.get(table_name)
    transform = TRANSFORMS.get(table_name)

    logging.info(f"Processing gs://{bucket_name}/{file_name} -> {table_id}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
        max_bad_records=100,
    )
    if schema:
        job_config.schema = schema
    else:
        job_config.autodetect = True
        job_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        ]

    total_rows  = 0
    batch       = []
    batch_count = 0

    for raw_line in iter_lines_from_gcs(bucket_name, file_name):
        try:
            row = json.loads(raw_line)
        except json.JSONDecodeError as e:
            logging.warning(f"Skipping bad JSON: {e}")
            continue

        if transform:
            row = transform(row)

        batch.append(json.dumps(row, ensure_ascii=False))

        if len(batch) >= BATCH_SIZE:
            batch_count += 1
            try:
                n = load_batch_to_bq(batch, table_id, job_config)
                total_rows += n
                logging.info(f"Batch {batch_count}: loaded {n} rows (total so far: {total_rows})")
            except Exception as e:
                logging.error(f"Batch {batch_count} failed: {e}")
                raise
            finally:
                batch.clear()  # free ram

    if batch:
        batch_count += 1
        try:
            n = load_batch_to_bq(batch, table_id, job_config)
            total_rows += n
            logging.info(f"Batch {batch_count} (final): loaded {n} rows")
        except Exception as e:
            logging.error(f"Final batch failed: {e}")
            raise
        finally:
            batch.clear()

    logging.info(f"Done. Total loaded: {total_rows} rows -> {table_id}")