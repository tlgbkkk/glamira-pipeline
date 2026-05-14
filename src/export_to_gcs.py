import os
import logging
from pymongo import MongoClient
from google.cloud import storage
from bson import json_util
from src.config import MONGO_URI, DB_NAME, BUCKET_NAME

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("export_gcs.log"),
        logging.StreamHandler()
    ]
)

def upload_local_file_to_gcs(bucket, local_file_path, destination_blob_name):
    try:
        if not os.path.exists(local_file_path):
            logging.error(f"File not found: {local_file_path}")
            return False

        blob = bucket.blob(destination_blob_name)
        logging.info(f"Uploading {local_file_path} to gs://{BUCKET_NAME}/{destination_blob_name}...")
        blob.upload_from_filename(local_file_path)
        logging.info("Upload successful!")
        return True
    except Exception as e:
        logging.error(f"Failed to upload {local_file_path}: {e}")
        return False


def export_mongo_collection_to_gcs(db, bucket, collection_name, destination_folder, batch_size=100000, limit=None):
    collection = db[collection_name]
    total_docs = collection.count_documents({}) if limit is None else limit
    logging.info(f"Starting export for '{collection_name}'. Total docs to process: {total_docs}")

    if total_docs == 0:
        logging.warning(f"Collection {collection_name} is empty. Skipping.")
        return

    cursor = collection.find({})
    if limit:
        cursor = cursor.limit(limit)

    chunk_index = 1
    docs_processed = 0
    current_chunk_data = []

    try:
        for doc in cursor:
            current_chunk_data.append(json_util.dumps(doc))
            docs_processed += 1

            if len(current_chunk_data) >= batch_size or docs_processed == total_docs:
                local_filename = f"{collection_name}_part_{chunk_index}.jsonl"
                gcs_blob_name = f"{destination_folder}/{local_filename}"

                logging.info(f"Writing {len(current_chunk_data)} rows to {local_filename}...")
                with open(local_filename, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(current_chunk_data) + '\n')

                upload_local_file_to_gcs(bucket, local_filename, gcs_blob_name)

                current_chunk_data.clear()
                os.remove(local_filename)

                logging.info(f"Progress: {docs_processed}/{total_docs} ({(docs_processed / total_docs) * 100:.2f}%)")
                chunk_index += 1

    except Exception as e:
        logging.error(f"Error while exporting collection {collection_name}: {e}")


def main():
    logging.info("=== STARTING DATA EXPORT TO GCS ===")

    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        db.command("ping")  # Test connection
        logging.info("Connected to MongoDB successfully.")
    except Exception as e:
        logging.error(f"MongoDB connection failed: {e}")
        return

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        logging.info("Connected to Google Cloud Storage successfully.")
    except Exception as e:
        logging.error(f"GCS connection failed: {e}. (Did you set up default credentials?)")
        return

    IS_TESTING = False
    limit_docs = 1000 if IS_TESTING else None

    export_mongo_collection_to_gcs(
        db=db,
        bucket=bucket,
        collection_name="ip_locations",
        destination_folder="exports/ip_locations",
        batch_size=200000,
        limit=limit_docs
    )

    export_mongo_collection_to_gcs(
        db=db,
        bucket=bucket,
        collection_name="product_dictionary",
        destination_folder="exports/product_dictionary",
        batch_size=100000,
        limit=limit_docs
    )

    export_mongo_collection_to_gcs(
        db=db,
        bucket=bucket,
        collection_name="summary",
        destination_folder="exports/summary_raw",
        batch_size=200000,
        limit=limit_docs
    )

    logging.info("=== ALL EXPORT TASKS COMPLETED ===")


if __name__ == "__main__":
    main()