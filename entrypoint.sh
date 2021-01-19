#!/bin/bash

# Authenticate with service account key to access GCP resources
gcloud auth activate-service-account --key-file=/key.json

# Set project ID
gcloud config set project "$GCP_PROJECT"

# Create bucket to store our JAR
gsutil mb "$GCP_BUCKET_NAME"

# Copy JAR to bucket
gsutil cp /app.jar "$GCP_BUCKET_NAME/app.jar"

# Create Dataproc cluster
gcloud dataproc clusters create "$GCP_CLUSTER_NAME" \
  --project="$GCP_PROJECT" \
  --region="$GCP_REGION"

# Submit spark job to cluster
gcloud dataproc jobs submit spark \
  --cluster="$GCP_CLUSTER_NAME" \
  --class=Main \
  --jars="$GCP_BUCKET_NAME/app.jar" \
  --region="$GCP_REGION" \
  -- "$APP_MACHINE_LOGS" "$APP_JOB_LOGS" "$APP_TASK_LOGS" "$APP_TASK_USAGE_LOGS" "$APP_OUT_PATH"

# Delete spark job
gcloud dataproc jobs delete "$SPARK_JOB_ID" \
  --region="$GCP_REGION" \
  --quiet

# Delete Dataproc cluster
gcloud dataproc clusters delete "$GCP_CLUSTER_NAME" \
  --project="$GCP_PROJECT" \
  --region="$GCP_REGION" \
  --quiet

# Delete Dataproc buckets
gsutil rm -r gs://dataproc*

# Delete JAR
gsutil rm "$GCP_BUCKET_NAME/app.jar"

# Delete bucket
gsutil rm -r "$GCP_BUCKET_NAME"