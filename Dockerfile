# ======== BUILD ========

FROM mozilla/sbt AS BUILD_STAGE

WORKDIR /app

ENV SCALA_VERSION "2.11.12"

COPY src/ src/

COPY build.sbt build.sbt

RUN sbt clean compile package

# ======== DEPLOY ========

FROM google/cloud-sdk:alpine

COPY --from=BUILD_STAGE /app/target/scala-2.11/*.jar /app.jar

COPY key.json /key.json

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENV GCP_PROJECT=data-management-project-2020
ENV GCP_REGION=europe-west1
ENV GCP_BUCKET_NAME=gs://ldsm-spark-deploy-assalielmehdi
ENV GCP_CLUSTER_NAME=ldsm-spark-deploy-assalielmehdi

ENV APP_MACHINE_LOGS=gs://clusterdata-2011-2/machine_events/*.gz
ENV APP_JOB_LOGS=gs://clusterdata-2011-2/job_events/*.gz
ENV APP_TASK_LOGS=gs://clusterdata-2011-2/task_events/*.gz
ENV APP_TASK_USAGE_LOGS=gs://clusterdata-2011-2/task_usage/*.gz
ENV APP_OUT_PATH=gs://ldsm-spark-deploy-assalielmehdi

#ENTRYPOINT ["/entrypoint.sh"]