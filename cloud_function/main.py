import io
import json
import logging
import time
import os

from google.api_core import exceptions as gcp_exceptions
from google.cloud import bigquery, storage

PROJECT_ID = os.environ.get("GCP_PROJECT")
DATASET_ID = os.environ.get("DATASET_ID")

bq_client  = bigquery.Client()
gcs_client = storage.Client()

SCHEMAS = {
    "summary_raw": [
        bigquery.SchemaField("_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("time_stamp", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("ip", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_agent", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("resolution", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_id_db", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("device_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("api_version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("store_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("local_time", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("show_recommendation", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("current_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("referrer_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("email_address", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("collection", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("price", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("product_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("cat_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("collect_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("order_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("key_search", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("utm_source", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("utm_medium", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("is_paypal", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("viewing_product_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("recommendation", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("recommendation_clicked_position", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("recommendation_product_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("recommendation_product_position", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("option", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("option_label", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("option_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value_label", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("quality", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("quality_label", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("alloy", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("diamond", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("shapediamond", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("stone", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("finish", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("pearlcolor", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Kollektion", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("kollektion_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("price", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("category_id", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("cart_products", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("product_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("amount", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("price", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("option", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("option_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("option_label", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("value_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("value_label", "STRING", mode="NULLABLE"),
            ]),
        ]),
    ],
    "ip_locations": [
        bigquery.SchemaField("_id",     "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ip",      "STRING", mode="NULLABLE"),
        bigquery.SchemaField("region",  "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city",    "STRING", mode="NULLABLE"),
    ],  
    "product_dictionary": [
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

def flatten_oid(row):
    # {"$oid": "abc"} -> "abc"
    if isinstance(row.get("_id"), dict) and "$oid" in row["_id"]:
        row["_id"] = row["_id"]["$oid"]
    return row


def parse_price(value):
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    s = s.strip(". '")
    s = s.strip()

    s = s.replace("'", "")

    s = s.replace(" ", "")

    if "." in s and "," in s:
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        else:
            s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    elif s.count(".") > 1:
        s = s.replace(".", "")

    s = s.lstrip(".")

    if not s:
        return None

    try:
        return float(s)
    except ValueError:
        logging.warning(f"Cannot parse price: '{value}'")
        return None


def transform_summary_raw(row):
    row = flatten_oid(row)

    option_raw = row.get("option")
    if isinstance(option_raw, dict):
        row["option"] = [option_raw]
    elif isinstance(option_raw, list):
        row["option"] = option_raw
    else:
        row["option"] = []

    if "price" in row:
        row["price"] = parse_price(row["price"])

    # cart_products
    cart_products = row.get("cart_products")
    if isinstance(cart_products, list):
        cleaned = []
        for cp in cart_products:
            if not isinstance(cp, dict):
                continue

            # cart_products.option: object -> array
            cp_option = cp.get("option")
            if isinstance(cp_option, dict):
                cp["option"] = [cp_option]
            elif isinstance(cp_option, list):
                cp["option"] = cp_option
            else:
                cp["option"] = []

            # cart_products.price
            if "price" in cp:
                cp["price"] = parse_price(cp["price"])

            cleaned.append(cp)
        row["cart_products"] = cleaned
    else:
        row["cart_products"] = []

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
    "product_dictionary":     transform_products,
}

def process_file(bucket_name, file_name, transform):
    blob = gcs_client.bucket(bucket_name).blob(file_name)
    out  = []
    bad  = 0
    with blob.open("rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as e:
                bad += 1
                logging.warning(f"Bad JSON: {e}")
                continue
            if transform:
                row = transform(row)
            out.append(json.dumps(row, ensure_ascii=False))
    if bad:
        logging.warning(f"Skipped {bad} bad JSON lines")
    return "\n".join(out).encode("utf-8")


def load_to_bq(data, table_id, job_config, max_retries=5):
    for attempt in range(1, max_retries + 1):
        try:
            job = bq_client.load_table_from_file(
                io.BytesIO(data), table_id, job_config=job_config
            )
            job.result()
            return job.output_rows
        except gcp_exceptions.TooManyRequests:
            wait = 2 ** attempt  # 2, 4, 8, 16, 32 giay
            logging.warning(f"429 rate limit, retry {attempt}/{max_retries} sau {wait}s")
            if attempt == max_retries:
                raise
            time.sleep(wait)

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

    data = process_file(bucket_name, file_name, transform)
    logging.info(f"Transformed: {len(data) / 1024 / 1024:.1f} MB")

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

    n = load_to_bq(data, table_id, job_config)
    logging.info(f"Done. Loaded {n} rows -> {table_id}")