#!/bin/bash

set -e

PROJECT_ID=$(gcloud config get-value project)
BUCKET_NAME="gcs-project-data"
DATASET_ID="raw_layer"
REGION="us-central1"

FUNCTION_SA="${PROJECT_ID}@appspot.gserviceaccount.com"

echo "=========================================================="
echo "PROJECT_ID: $PROJECT_ID"
echo "BUCKET_NAME: $BUCKET_NAME"
echo "DATASET_ID: $DATASET_ID"
echo "FUNCTION_SA: $FUNCTION_SA"
echo "=========================================================="

echo ""
echo "1. Enable required APIs"
echo "----------------------------------------------------------"

gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    artifactregistry.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com

echo ""
echo "2. Grant BigQuery permissions"
echo "----------------------------------------------------------"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${FUNCTION_SA}" \
    --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${FUNCTION_SA}" \
    --role="roles/bigquery.dataEditor"

echo ""
echo "3. Grant GCS read permissions"
echo "----------------------------------------------------------"

gsutil iam ch \
    serviceAccount:${FUNCTION_SA}:objectViewer \
    gs://${BUCKET_NAME}

echo ""
echo "4. Deploy Cloud Function"
echo "----------------------------------------------------------"

gcloud functions deploy gcs_to_bq_trigger \
    --no-gen2 \
    --runtime python310 \
    --trigger-resource $BUCKET_NAME \
    --trigger-event google.storage.object.finalize \
    --entry-point trigger_bigquery_load \
    --set-env-vars GCP_PROJECT=$PROJECT_ID,DATASET_ID=$DATASET_ID \
    --region $REGION \
    --allow-unauthenticated

echo ""
echo "=========================================================="
echo "DEPLOY SUCCESS!"
echo "=========================================================="
