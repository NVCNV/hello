#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin
mypath="$(cd "$(dirname "$0")"; pwd)"
cd $mypath
ANALY_DATE=$1
ANALY_HOUR=$2
hdfs dfs -mkdir -p /datang/fuck/${ANALY_DATE}/${ANALY_HOUR}
