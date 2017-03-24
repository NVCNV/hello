#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export SPARK_HOME=/opt/app/spark
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin::$SPARK_HOME/bin
FILE=`echo /dt/tmp/*`
ANALY_DATE=`echo ${FILE:8:8}`
ANALY_HOUR=`echo ${FILE:16:2}`

MAIN_CLASS=com.dtmobile.spark.job.AnalyJob
JAR=/dt/lib/spark_core-1.0-SNAPSHOT.jar
MASTER=/app/workspace/DT_Spark/job/bin/nsspAnaly.sh
/opt/app/spark/bin/spark-submit  --class $MAIN_CLASS --executor-memory 4G --executor-cores 2 $JAR $ANALY_DATE $ANALY_HOUR liaoning lndcl spark://172.30.4.188:7077