#!/bin/bash
set -e

PROJECT_ID=$(gcloud config get-value project)
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')
BUCKET_NAME="gcs-project-data"
DATASET_ID="raw_layer"
REGION="us-central1"

FUNCTION_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

echo "=========================================================="
echo "DEPLOYING CLOUD FUNCTION"
echo "PROJECT_ID    : $PROJECT_ID"
echo "FUNCTION_SA   : $FUNCTION_SA"
echo "BUCKET        : $BUCKET_NAME"
echo "DATASET       : $DATASET_ID"
echo "=========================================================="

echo "1. Enabling APIs..."
gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com --quiet

echo "2. Granting Permissions..."

# BigQuery: tao job + doc/ghi data vao dataset
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${FUNCTION_SA}" \
    --role="roles/bigquery.jobUser" --quiet > /dev/null

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${FUNCTION_SA}" \
    --role="roles/bigquery.dataEditor" --quiet > /dev/null

gcloud storage buckets add-iam-policy-binding gs://${BUCKET_NAME} \
    --member="serviceAccount:${FUNCTION_SA}" \
    --role="roles/storage.objectViewer" --quiet > /dev/null

echo "3. Deploying Function..."
gcloud functions deploy gcs_to_bq_trigger \
    --no-gen2 \
    --runtime python310 \
    --trigger-resource $BUCKET_NAME \
    --trigger-event google.storage.object.finalize \
    --entry-point trigger_bigquery_load \
    --set-env-vars GCP_PROJECT=$PROJECT_ID,DATASET_ID=$DATASET_ID \
    --region $REGION \
    --timeout 540s \
    --memory 2048MB \
    --quiet

echo "=========================================================="
echo "DEPLOY SUCCESS!"
echo "=========================================================="