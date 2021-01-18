# ======== BUILD ========

FROM mozilla/sbt AS BUILD_STAGE

WORKDIR /app

COPY src/ src/

COPY build.sbt build.sbt

RUN sbt clean compile package

# ======== DEPLOY ========

FROM google/cloud-sdk:alpine

COPY --from=BUILD_STAGE /app/target/scala-2.12/*.jar /app.jar

COPY key.json /key.json

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENV GCP_PROJECT=data-management-project-2020
ENV GCP_REGION=europe-west2
ENV GCP_BUCKET_NAME=spark-deploy-13542
ENV GCP_CLUSTER_NAME=spark-deploy

ENV APP_MACHINE_LOGS=gs://clusterdata-2011-2/machine_events/*.gz
ENV APP_JOB_LOGS=gs://clusterdata-2011-2/job_events/part-00000-of-00500.csv.gz
ENV APP_OUT_DIR=dummy

#ENTRYPOINT ["/entrypoint.sh"]