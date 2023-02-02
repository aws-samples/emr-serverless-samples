FROM public.ecr.aws/emr-serverless/spark/emr-6.9.0:latest

USER root

RUN python3 -m pip install -U pip
RUN python3 -m pip install seaborn

USER hadoop:hadoop