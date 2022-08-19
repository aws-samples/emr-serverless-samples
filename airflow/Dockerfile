FROM python:3.9

ENV AIRFLOW_VAR_EMR_SERVERLESS_APPLICATION_ID=
ENV AIRFLOW_VAR_EMR_SERVERLESS_JOB_ROLE=
ENV AIRFLOW_VAR_EMR_SERVERLESS_LOG_BUCKET=
ENV AWS_DEFAULT_REGION=us-east-1
ENV AWS_ACCESS_KEY_ID=
ENV AWS_SECRET_ACCESS_KEY=
ENV AWS_SESSION_TOKEN=

WORKDIR /opt/emr

COPY requirements-dev.txt requirements.txt
RUN python3 -m pip install -r requirements.txt

ENV AIRFLOW_HOME=/opt/emr/airflow

RUN airflow db init

COPY dags ./airflow/dags
COPY emr_serverless ./airflow/dags/emr_serverless
COPY tests ./tests

CMD pytest tests/
