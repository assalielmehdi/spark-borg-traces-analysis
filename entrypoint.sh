#!/bin/bash

# Authenticate with service account key to access GCP resources
gcloud auth activate-service-account --key-file=/key.json

# Set project ID
gcloud config set project "$GCP_PROJECT"

# Create bucket to store our JAR
gsutil mb "gs://$GCP_BUCKET_NAME"

# Copy JAR to bucket
gsutil cp /app.jar "gs://$GCP_BUCKET_NAME/app.jar"

# Create Dataproc cluster
gcloud dataproc clusters create "$GCP_CLUSTER_NAME" \
  --project="$GCP_PROJECT" \
  --region="$GCP_REGION" \
  --single-node

# Submit spark job to cluster
gcloud dataproc jobs submit spark \
  --cluster="$GCP_CLUSTER_NAME" \
  --class=Main \
  --jars="gs://$GCP_BUCKET_NAME/app.jar" \
  --region="$GCP_REGION" \
  -- "$APP_MACHINE_LOGS" "$APP_JOB_LOGS" "$APP_OUT_DIR"
