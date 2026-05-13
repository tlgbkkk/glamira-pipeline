#!/bin/bash

set -e

PROJECT_ID=$(gcloud config get-value project)

BUCKET_NAME="gcs-project-data"
DATASET_ID="raw_layer"

echo "----------------------------------------------------------"
echo "Deploying Cloud Function"
echo "Project ID: $PROJECT_ID"
echo "Bucket: $BUCKET_NAME"
echo "----------------------------------------------------------"

gcloud functions deploy gcs_to_bq_trigger \
    --no-gen2 \
    --runtime python310 \
    --trigger-resource $BUCKET_NAME \
    --trigger-event google.storage.object.finalize \
    --entry-point trigger_bigquery_load \
    --set-env-vars GCP_PROJECT=$PROJECT_ID,DATASET_ID=$DATASET_ID \
    --region us-central1 \
    --allow-unauthenticated

echo "----------------------------------------------------------"
echo "Deploy completed!"
echo "----------------------------------------------------------"