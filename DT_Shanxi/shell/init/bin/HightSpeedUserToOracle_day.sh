#!/bin/bash
TAKING_DATE=$1
TAKING_HOUR=$2

HDFS_ADDR=" hdfs://dtcluster/datang2/output"

DB_ADDR="userid=scott/tiger@hadoop"

LOCALDIR="/dt/NewData"
CTLDIR="/dt/ctl"


rm -rf ${LOCALDIR}/$TAKING_DATE
#mkdir ${LOCALDIR}/$TAKING_DATE


echo "------->hdfs dfs -getmerge ${HDFS_ADDR}/u4/${TAKING_DATE}/u4*  ${LOCALDIR}/${TAKING_DATE}/u4"
hdfs dfs -getmerge ${HDFS_ADDR}/u4/${TAKING_DATE}/u4*  ${LOCALDIR}/${TAKING_DATE}/u4.dat
mkdir -p dt/sqlldrLog/u4/${TAKING_DATE}/
sqlldr ${DB_ADDR} control=${CTLDIR}/u4.ctl data=${LOCALDIR}/${TAKING_DATE}/u4.dat log=/dt/sqlldrLog/u4/${TAKING_DATE}

echo "------->hdfs dfs -getmerge ${HDFS_ADDR}/updowntrain/${TAKING_DATE}/* ${LOCALDIR}/${TAKING_DATE}/upordown"
hdfs dfs -getmerge ${HDFS_ADDR}/updowntrain/${TAKING_DATE}/* ${LOCALDIR}/${TAKING_DATE}/upordown.dat
mkdir -p /dt/sqlldrLog/updowntrain/${TAKING_DATE}/
sqlldr ${DB_ADDR} control=${CTLDIR}/updowntrain.ctl data=${LOCALDIR}/${TAKING_DATE}/upordown.dat log=/dt/sqlldrLog/updowntrain/${TAKING_DATE}

exit 0
