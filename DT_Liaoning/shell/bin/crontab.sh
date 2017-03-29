#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export SPARK_HOME=/opt/app/spark
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin::$SPARK_HOME/bin
ANALY_DATE=`date +%Y%m%d`
ANALY_HOUR="`date -d ' -0 hour' +%H`"

JAR=/dt/lib/DT_Core-1.0-SNAPSHOT.jar
URL=https://172.30.4.222:8443
USERNAME=azkaban
PASSWD=azkaban
PROJECT=Test
FLOW=JobComplete
KEYSTORE=/opt/app/azkaban/web/conf/keystore
mkdir -p /data1/taiyue/TEMP/volte_orgn/${ANALY_DATE}/${ANALY_HOUR}/
mkdir -p /data1/taiyue/TEMP/volte_rx/${ANALY_DATE}/${ANALY_HOUR}/
mkdir -p /data1/taiyue/TEMP/volte_sv/${ANALY_DATE}/${ANALY_HOUR}/
mkdir -p /data1/taiyue/TEMP/s1mme_orgn/${ANALY_DATE}/${ANALY_HOUR}/
mkdir -p /data1/taiyue/TEMP/s1u_http_orgn/${ANALY_DATE}/${ANALY_HOUR}/
mkdir -p  /appfs/hadoopfile/LTE_MRO_SOURCE/${ANALY_DATE}/${ANALY_HOUR}/
mkdir -p  /appfs/hadoopfile/TB_XDR_IFC_UU/${ANALY_DATE}/${ANALY_HOUR}/
mkdir -p  /appfs/hadoopfile/TB_XDR_IFC_X2/${ANALY_DATE}/${ANALY_HOUR}/

hdfs dfs -get /datang/volte_orgn/20170227/08/*     /data1/taiyue/TEMP/volte_orgn/${ANALY_DATE}/${ANALY_HOUR}/
hdfs dfs -get /datang/volte_rx/20170227/08/*       /data1/taiyue/TEMP/volte_rx/${ANALY_DATE}/${ANALY_HOUR}/
hdfs dfs -get /datang/volte_sv/20170227/08/*       /data1/taiyue/TEMP/volte_sv/${ANALY_DATE}/${ANALY_HOUR}/
hdfs dfs -get /datang/s1mme_orgn/20170227/08/*     /data1/taiyue/TEMP/s1mme_orgn/${ANALY_DATE}/${ANALY_HOUR}/
hdfs dfs -get /datang/s1u_http_orgn/20170227/08/*  /data1/taiyue/TEMP/s1u_http_orgn/${ANALY_DATE}/${ANALY_HOUR}/
hdfs dfs -get /datang/LTE_MRO_SOURCE/20170227/08/*   /appfs/hadoopfile/LTE_MRO_SOURCE/${ANALY_DATE}/${ANALY_HOUR}/
hdfs dfs -get /datang/TB_XDR_IFC_UU/20170227/08/*   /appfs/hadoopfile/TB_XDR_IFC_UU/${ANALY_DATE}/${ANALY_HOUR}/
hdfs dfs -get /datang/TB_XDR_IFC_X2/20170227/08/*   /appfs/hadoopfile/TB_XDR_IFC_X2/${ANALY_DATE}/${ANALY_HOUR}/

java -jar $JAR $URL $USERNAME $PASSWD $PROJECT $FLOW $PASSWD $KEYSTORE $KEYSTORE $ANALY_DATE $ANALY_HOUR