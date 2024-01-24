ARG RELEASE="emr-7.0.0"
ARG RELEASE_TAG="latest"

FROM public.ecr.aws/emr-serverless/spark/${RELEASE}:${RELEASE_TAG}

# Switch to the root user
USER root

# Update Spark config for local development
RUN echo -e "\nspark.master\tlocal[*]\n" >> /etc/spark/conf/spark-defaults.conf

# Use the Glue Data Catalog
RUN echo -e "\n# Enable Glue Data Catalog\nspark.sql.catalogImplementation\thive\nspark.hadoop.hive.metastore.client.factory.class\tcom.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory\n" >> /etc/spark/conf/spark-defaults.conf

# Configure log4j to ignore EC2 metadata access failure-related error messages
RUN if [ -f /etc/spark/conf/log4j.properties ]; then echo -e "\n\nlog4j.logger.com.amazonaws.internal.InstanceMetadataServiceResourceFetcher=FATAL\nlog4j.logger.com.amazonaws.util.EC2MetadataUtils=FATAL" >> /etc/spark/conf/log4j.properties; fi
RUN if [ -f /etc/spark/conf/log4j2.properties ]; then echo -e "\n\nlogger.metadata.name = com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.internal.InstanceMetadataServiceResourceFetcher\nlogger.metadata.level = fatal\nlogger.ec2meta.name = com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.util.EC2MetadataUtils\nlogger.ec2meta.level = fatal\n" >> /etc/spark/conf/log4j2.properties; fi

# Don't log INFO messages to the console 
RUN if [ -f /etc/spark/conf/log4j.properties ]; then sed -i s/log4j.rootCategory=.*/log4j.rootCategory=WARN,console/ /etc/spark/conf/log4j.properties; fi
RUN if [ -f /etc/spark/conf/log4j2.properties ]; then sed -i 's/rootLogger.level = info/rootLogger.level = warn/' /etc/spark/conf/log4j2.properties; fi

# Allow hadoop user to sudo for admin tasks
RUN dnf install -y sudo && \
     echo "hadoop ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Upgrade to AWS CLI v2
RUN dnf install -y git unzip
RUN if [ "$TARGETARCH" = "arm64" ]; then curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"; else curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"; fi && \
     unzip awscliv2.zip && \
     ./aws/install && \
     rm -rf aws awscliv2.zip

# ipykernel depends on pusutil, which does not publish wheels for aarch64
RUN if [ "$TARGETARCH" != "amd64" ]; then dnf install -y gcc python3-devel; fi

# Upgrade pip first
RUN dnf remove -y python3-pip && python3 -m ensurepip --upgrade && python3 -m pip install -U pip

# Enable Jupyter notebooks and pytest
RUN python3 -m pip install \
     ipykernel \
     pytest

# Switch back to the default user
USER hadoop:hadoop