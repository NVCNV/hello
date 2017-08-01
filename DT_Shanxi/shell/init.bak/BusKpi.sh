#!/bin/bash

ANALY_DATE=$1
ANALY_HOUR=$2
SDB=$3
DDB=$4
MASTERIP=$5
ORACL=$6 
SOURCEDIR=$7
VERSINCONTROL=$8



echo "ANALY_DATE:ANALY_HOUR:MASTERIP:SDB:DDB:ORACL:PATH"
MAIN_CLASS=com.dtmobile.spark.job.AnalyJob
JAR=/dt/lib/DT_shanxiUserKpi-1.0-SNAPSHOT.jar
MASTER=spark://${MASTERIP}:7077

/opt/app/spark/bin/spark-submit \
 --class $MAIN_CLASS \
 --executor-memory 4G \
 --executor-cores 2 \
 $JAR $ANALY_DATE $ANALY_HOUR $SDB $DDB $MASTER $ORACL $SOURCEDIR $VERSINCONTROL


