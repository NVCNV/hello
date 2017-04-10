#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export SPARK_HOME=/opt/app/spark
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin::$SPARK_HOME/bin
ANALY_DATE=$1
ANALY_HOUR=$2
MAIN_CLASS=com.dtmobile.spark.job.AnalyJob
JAR=/dt/lib/DT_Liaoning-1.0-SNAPSHOT.jar
MASTER=spark://192.168.3.2:7077

/opt/app/spark/bin/spark-submit  \
 --class $MAIN_CLASS \
 --executor-memory 4G \
 --executor-cores 2 \
 $JAR $ANALY_DATE $ANALY_HOUR datang dcl $MASTER