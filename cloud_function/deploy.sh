#!/bin/bash

set -e

PROJECT_ID=$(gcloud config get-value project)
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")

BUCKET_NAME="gcs-project-data"
DATASET_ID="raw_layer"
REGION="us-central1"

echo "----------------------------------------------------------"
echo "PROJECT_ID: $PROJECT_ID"
echo "PROJECT_NUMBER: $PROJECT_NUMBER"
echo "BUCKET_NAME: $BUCKET_NAME"
echo "DATASET_ID: $DATASET_ID"
echo "----------------------------------------------------------"

echo "Enabling required APIs..."
gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    artifactregistry.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com \
    eventarc.googleapis.com

echo "----------------------------------------------------------"
echo "Getting default Compute Engine service account..."
FUNCTION_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

echo "FUNCTION_SA: $FUNCTION_SA"
echo "----------------------------------------------------------"

echo "Granting BigQuery permissions to Cloud Function SA..."

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$FUNCTION_SA" \
    --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$FUNCTION_SA" \
    --role="roles/bigquery.dataEditor"

echo "----------------------------------------------------------"
echo "Granting GCS access to Cloud Function SA..."

gsutil iam ch \
serviceAccount:$FUNCTION_SA:objectViewer \
gs://$BUCKET_NAME

echo "----------------------------------------------------------"
echo "Granting BigQuery service account access to GCS bucket..."

gsutil iam ch \
serviceAccount:bq-${PROJECT_NUMBER}@bigquery-encryption.iam.gserviceaccount.com:objectViewer \
gs://$BUCKET_NAME

echo "----------------------------------------------------------"
echo "Deploying Cloud Function..."

gcloud functions deploy gcs_to_bq_trigger \
    --no-gen2 \
    --runtime python310 \
    --trigger-resource $BUCKET_NAME \
    --trigger-event google.storage.object.finalize \
    --entry-point trigger_bigquery_load \
    --set-env-vars GCP_PROJECT=$PROJECT_ID,DATASET_ID=$DATASET_ID \
    --region $REGION \
    --allow-unauthenticated

echo "----------------------------------------------------------"
echo "Deploy completed successfully!"
echo "----------------------------------------------------------"