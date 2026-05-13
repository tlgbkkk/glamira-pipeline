#!/bin/bash

# get project id
PROJECT_ID=$(gcloud config get-value project)

echo "Deploying Cloud Function on Project: $PROJECT_ID..."

gcloud functions deploy gcs_to_bq_trigger \
--runtime python310 \
--trigger-resource gcs-project-raw-data \
--trigger-event google.storage.object.finalize \
--entry-point trigger_bigquery_load \
--set-env-vars GCP_PROJECT=$PROJECT_ID \
--region us-central1 \
--allow-unauthenticated