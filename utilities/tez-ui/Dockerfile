FROM amazonlinux:2
FROM amazoncorretto:8
FROM maven:3.6-amazoncorretto-8

LABEL org.opencontainers.image.source https://github.com/aws-samples/emr-serverless-samples
LABEL org.opencontainers.image.url https://github.com/aws-samples/emr-serverless-samples/tree/main/utilities/tez-ui
LABEL org.opencontainers.image.documentation https://github.com/aws-samples/emr-serverless-samples/blob/main/utilities/tez-ui/README.md

RUN yum install -y procps awscli rsync patch

WORKDIR /tmp/
ENV ENTRYPOINT /usr/bin/entrypoint.sh
ENV TEZ_HOME /hadoop/usr/lib/tez
ENV YARN_HOME /hadoop/usr/lib/hadoop-yarn
ENV HADOOP_HOME /hadoop/usr/lib/hadoop
ENV HDFS_HOME /hadoop/usr/lib/hadoop-hdfs
ENV TEZ_HOME /hadoop/usr/lib/tez
ENV HADOOP_CONF /hadoop/etc/hadoop/conf

RUN curl -o ./apache-tez-0.9.2-bin.tar.gz https://archive.apache.org/dist/tez/0.9.2/apache-tez-0.9.2-bin.tar.gz && \
    curl -o ./hadoop-2.10.1.tar.gz https://archive.apache.org/dist/hadoop/common/hadoop-2.10.1/hadoop-2.10.1.tar.gz && \
    tar -xzf hadoop-2.10.1.tar.gz && \
    tar -xzf apache-tez-0.9.2-bin.tar.gz

RUN mkdir -p $HADOOP_HOME/lib && \
    mkdir -p $TEZ_HOME && \
    mkdir -p $HADOOP_CONF && \
    mkdir -p $YARN_HOME && \
    mkdir -p $HDFS_HOME &&\
    mkdir -p /tmp/tez-ui

COPY hadoop-layout.sh $HADOOP_HOME/libexec/hadoop-layout.sh
COPY yarn-site.xml .
COPY pom.xml .

RUN mvn dependency:copy-dependencies -DoutputDirectory=/tmp/tez-ui/ && \
    cp /tmp/tez-ui/joda-time-2.9.3.jar $HADOOP_HOME/lib/ && \
    cp /tmp/tez-ui/jetty-runner-*.jar $TEZ_HOME && \
    cp /tmp/tez-ui/tez-yarn-timeline-cache-plugin*.jar $TEZ_HOME

COPY event-log-sync.sh .
COPY entrypoint.sh /usr/bin/entrypoint.sh
COPY tez-ui.patch /tmp/

RUN chmod 744 $ENTRYPOINT

ENTRYPOINT [ "/usr/bin/entrypoint.sh" ]

EXPOSE 8088
EXPOSE 8188
EXPOSE 9999

CMD tail -f /dev/null