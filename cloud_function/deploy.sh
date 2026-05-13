#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)

CONFIG_PATH="../src/config.py"

if [ -f "$CONFIG_PATH" ]; then
    BUCKET_NAME=$(grep "BUCKET_NAME" $CONFIG_PATH | sed -E 's/.*"([^"]+)".*/\1/')
else
    echo "Not found $CONFIG_PATH!"
    exit 1
fi

echo "Deploying Cloud Function on Project: $PROJECT_ID..."

gcloud functions deploy gcs_to_bq_trigger \
--runtime python310 \
--trigger-resource $BUCKET_NAME \
--trigger-event google.storage.object.finalize \
--entry-point trigger_bigquery_load \
--set-env-vars GCP_PROJECT=$PROJECT_ID \
--region us-central1 \
--allow-unauthenticated