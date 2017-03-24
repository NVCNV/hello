#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin
mypath="$(cd "$(dirname "$0")"; pwd)"
cd $mypath
FILE=`echo /dt/tmp/*`
ANALY_DATE=`echo ${FILE:8:8}`
ANALY_HOUR=`echo ${FILE:16:2}`
hdfs dfs -mkdir -p /datang/fuck/${ANALY_DATE}/${ANALY_HOUR}
