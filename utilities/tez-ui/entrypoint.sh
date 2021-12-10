#!/bin/bash

if [ -z "$JOB_RUN_ID" ]; then
    echo "JOB_RUN_ID is not set"
    exit
fi

if [ -z "$APPLICATION_ID" ]; then
    echo "APPLICATION_ID is not set"
    exit
fi

if [ -z "$S3_LOG_URI" ]; then
    echo "S3_LOG_URI is not set"
    exit
fi

function cpp(){
    [ -d $2 ] || mkdir $2
    cp -r $1 $2
}
cpp "hadoop-2.10.1/share/hadoop/hdfs/*.jar" /hadoop/usr/lib/hadoop-hdfs/
cpp "hadoop-2.10.1/share/hadoop/hdfs/*.jar" /hadoop/usr/lib/hadoop-hdfs/
cpp "hadoop-2.10.1/share/hadoop/hdfs/lib/*.jar" /hadoop/usr/lib/hadoop-hdfs/
cpp "hadoop-2.10.1/share/hadoop/common/lib/*.jar" /hadoop/usr/lib/hadoop/
cpp "hadoop-2.10.1/share/hadoop/common/*.jar" /hadoop/usr/lib/hadoop/
cpp "hadoop-2.10.1/share/hadoop/yarn/lib/*.jar" /hadoop/usr/lib/hadoop-yarn
cpp "hadoop-2.10.1/share/hadoop/yarn/*.jar" /hadoop/usr/lib/hadoop-yarn
cpp hadoop-2.10.1/share/hadoop/yarn/timelineservice/ /hadoop/usr/lib/hadoop-yarn/
cpp "apache-tez-0.9.2-bin/*" /hadoop/usr/lib/tez/

rm -rf $TEZ_HOME/lib/slf4j-log4j12-*

cpp hadoop-2.10.1/bin/yarn /hadoop/usr/lib/hadoop-yarn/bin

cp hadoop-2.10.1/etc/hadoop/* /hadoop/etc/hadoop/conf/
cp yarn-site.xml /hadoop/etc/hadoop/conf/
cpp hadoop-2.10.1/sbin/yarn-daemon.sh /hadoop/usr/lib/hadoop-yarn/sbin/
cpp hadoop-2.10.1/libexec /hadoop/usr/lib/hadoop/

bash event-log-sync.sh > event-log-sync.log &

export HADOOP_BASE_PATH=/hadoop
export HADOOP_COMMON_HOME=$HADOOP_BASE_PATH/usr/lib/hadoop
export HADOOP_LIBEXEC_DIR=$HADOOP_BASE_PATH/usr/lib/hadoop/libexec
export HADOOP_YARN_HOME=$HADOOP_BASE_PATH/usr/lib/hadoop-yarn
export HADOOP_HDFS_HOME=$HADOOP_BASE_PATH/usr/lib/hadoop-hdfs
export HADOOP_MAPRED_HOME=$HADOOP_BASE_PATH/usr/lib/hadoop
export HADOOP_CONF_DIR=$HADOOP_BASE_PATH/etc/hadoop/conf
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HADOOP_BASE_PATH/usr/lib/tez/*:$HADOOP_BASE_PATH/usr/lib/tez/lib/*:$HADOOP_BASE_PATH/usr/share/aws/aws-java-sdk/*
export TEZ_HOME=$HADOOP_BASE_PATH/usr/lib/tez
export PATH=$HADOOP_CLASSPATH:$PATH:$HADOOP_BASE_PATH/usr/lib/hadoop/bin
export USER=`id -u -n`

t=JOB_RUN_ID
sed -i "s#<value>$t</value>#<value>$JOB_RUN_ID</value>#" /hadoop/etc/hadoop/conf/yarn-site.xml
t=APPLICATION_ID
sed -i "s#<value>$t</value>#<value>$APPLICATION_ID</value>#" /hadoop/etc/hadoop/conf/yarn-site.xml

bash $HADOOP_BASE_PATH/usr/lib/hadoop-yarn/sbin/yarn-daemon.sh start resourcemanager
sleep 5s
bash $HADOOP_BASE_PATH/usr/lib/hadoop-yarn/sbin/yarn-daemon.sh start timelineserver

rm -rf hadoop-2.10.1* apache-tez-0.9.2-bin*

mkdir -p /hadoop/usr/lib/tez/logs/
java -jar $TEZ_HOME/jetty-runner-*.jar --port 9999 --path /tez-ui/ $TEZ_HOME/tez-ui*.war > $TEZ_HOME/logs/tez-ui.log &

echo "*****************************************************************"
echo "Launching Tez UI. Access it using: http://localhost:9999/tez-ui/"
echo "*****************************************************************"

tail -F $TEZ_HOME/logs/tez-ui.log