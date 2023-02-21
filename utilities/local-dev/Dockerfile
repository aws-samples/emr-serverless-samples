ARG EMR_RELEASE=6.9.0

FROM public.ecr.aws/emr-serverless/spark/emr-$EMR_RELEASE:latest

# When running locally, we don't have access to EC2 metadata
ENV AWS_EC2_METADATA_DISABLED=true

USER root

# Install sudo in case the user needs to install packages
RUN yum install -y sudo && yum clean all
RUN echo -e 'hadoop ALL=(ALL)NOPASSWD:ALL' > /etc/sudoers.d/hadoop

# Remove the custom resource manager and spark master
RUN sed -Ei 's/^(spark\.submit\.customResourceManager)/#\1/' /etc/spark/conf/spark-defaults.conf
RUN sed -Ei 's/spark\.master.*/spark.master\tlocal[*]/' /etc/spark/conf/spark-defaults.conf

# Configure log4j to ignore EC2 metadata access failure messages
RUN echo -e "\n\nlogger.metadata.name = com.amazonaws.internal.InstanceMetadataServiceResourceFetcher\nlogger.metadata.level = fatal\nlogger.ec2meta.name = com.amazonaws.util.EC2MetadataUtils\nlogger.ec2meta.level = fatal\n" >> /etc/spark/conf/log4j2.properties

# Enable the Spark UI
RUN sed -Ei 's/^spark\.ui\.enabled.*/spark.ui.enabled\ttrue/' /etc/spark/conf/spark-defaults.conf

# Use the Glue Data Catalog
RUN echo -e "\n# Enable Glue Data Catalog\nspark.sql.catalogImplementation\thive\nspark.hadoop.hive.metastore.client.factory.class\tcom.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory\n" >> /etc/spark/conf/spark-defaults.conf

# Upgrade to AWS CLI v2
RUN yum install -y git unzip
RUN if [ "$TARGETARCH" = "arm64" ]; then curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"; else curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"; fi && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws awscliv2.zip

# ipykernel depends on pusutil, which does not publish wheels for aarch64
RUN if [ "$TARGETARCH" != "amd64" ]; then yum install -y gcc python3-devel; fi

# Upgrade pip first
RUN python3 -m pip install -U pip

# Enable Jupyter notebooks - can be used when running in a devcontainer or by exposing 8080 and running jupyter server
ENV PATH "/home/hadoop/.local/bin:$PATH"
RUN python3 -m pip install ipykernel jupyter-server
EXPOSE 8080

USER hadoop:hadoop

ENTRYPOINT [ "/bin/bash" ]