#!/usr/bin/env bash
ANALY_DATE=$1
ANALY_HOUR=$2
SDB=$3
DDB=$4
MASTERIP=$5
OracleUrl=$6
SOURCEDIR=$7
#MASTER=spark://172.30.4.189:7077 --executor-memory 4G \

MAIN_CLASS=com.dtmobile.spark.job.AnalyJob
JAR=/dt/lib/DT_Shanxi-1.0-SNAPSHOT.jar
MASTER=spark://$MASTERIP:7077

/opt/app/spark/bin/spark-submit  \
 --class $MAIN_CLASS \
 --executor-memory 4G \
 --executor-cores 2 \
 $JAR $ANALY_DATE $ANALY_HOUR $SDB $DDB $MASTER $OracleUrl $SOURCEDIR