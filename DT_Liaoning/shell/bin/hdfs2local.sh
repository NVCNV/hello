#!/bin/bash

export HADOOP_CONF_DIR=/etc/hadoop/conf

ANALY_DATE=$1
ANALY_HOUR=$2
mypath="$(cd "$(dirname "$0")"; pwd)"
cd $mypath
#SOURCE_SVR="hdfs://nameservice1:8020/datang"
SOURCE_SVR="hdfs://nameservice1:8020/ws/detail"

JAR_PACKAGK="/cup/d4/datang/bin/LiaoNingFilter.jar"
JAR_MAIN="cn.com.dtmobile.hadoop.biz.LiaoNingFilter.job.DataJob"

HIVE_TABLES="s1mme_orgn sgs_orgn volte_rx volte_sv s1u_http_orgn"

GX=${SOURCE_SVR}/volte_rx/p1_day=${ANALY_DATE}/*${ANALY_DATE}${ANALY_HOUR}*.ok1
S1=${SOURCE_SVR}/s1mme_orgn/p1_day=${ANALY_DATE}/*${ANALY_DATE}${ANALY_HOUR}*.ok1
SV=${SOURCE_SVR}/volte_sv/p1_day=${ANALY_DATE}/*${ANALY_DATE}${ANALY_HOUR}*.ok1
SGS=${SOURCE_SVR}/sgs_orgn/p1_day=${ANALY_DATE}/*${ANALY_DATE}${ANALY_HOUR}*.ok1
MW=${SOURCE_SVR}/volte_orgn/p1_day=${ANALY_DATE}/*${ANALY_DATE}${ANALY_HOUR}*.ok1
HTTP=${SOURCE_SVR}/s1u_http_orgn/p1_day=${ANALY_DATE}/*${ANALY_DATE}${ANALY_HOUR}*.ok1
OUTPUT=/datang/FILTER/${ANALY_DATE}${ANALY_HOUR}
NODATA=/datang/nodata
hdfs dfs -test -e ${GX}
if [ $? -ne 0 ];then
GX=${NODATA}
fi
hdfs dfs -test -e ${S1}
if [ $? -ne 0 ];then
S1=${NODATA}
fi
hdfs dfs -test -e ${SV} 
if [ $? -ne 0 ];then
SV=${NODATA}
fi
hdfs dfs -test -e ${SGS}
if [ $? -ne 0 ]; then
SGS=${NODATA}
fi
hdfs dfs -test -e ${MW}
if [ $? -ne 0 ]; then
MW=${NODATA}
fi
hdfs dfs -test -e ${HTTP}
if [ $? -ne 0 ]; then
HTTP=${NODATA}
fi

T_PROCESS=/datang/t_process/t_process.csv
echo "`date '+/%y/%m/%d %H:%M:%S'` INFO begni,exit..."
HADOOP=`which hadoop`
#${HADOOP} fs -rm -R ${OUTPUT}
${HADOOP} jar ${JAR_PACKAGK} ${JAR_MAIN} \
${GX} \
${S1} \
${SV} \
${SGS} \
${MW} \
${HTTP} \
${OUTPUT} \
${T_PROCESS} \
${ANALY_HOUR}

#exit

TEMP_DIR=/cup/d4/datang/TEMP
echo "get data begin!------------------------------------------"

for tableName in ${HIVE_TABLES}
do

file_name=`echo ${tableName}|sed 's/_//g'`


dir_name=`hdfs dfs -ls ${OUTPUT}/${file_name}* | tail -1 | awk '{print $8}'`
file_cut=`echo ${dir_name}|cut -f5 -d'/'|cut -f3 -d'-'`
file_b=`echo ${dir_name}|cut -f5 -d'/' --complement`
num=${file_cut:1:2}
echo "${file_name} : there are more ${num}00 files..."
rm -rf ${TEMP_DIR}/${tableName}/$ANALY_DATE/$ANALY_HOUR/*
mkdir -p ${TEMP_DIR}/${tableName}/$ANALY_DATE/$ANALY_HOUR

#hdfs dfs -get ${OUTPUT}/${fileName}* ${TEMP_DIR}/${tableName}/$ANALY_DATE/$ANALY_HOUR
if [ ${num} != "00" ];then
for (( j=0;j<${num};j++ ))
do
i=`printf "%02d" "$j"`

hdfs dfs -test -e ${file_b}/${file_name}-m-0${i}*

if [ $? -eq 0 ]; then

hdfs dfs -getmerge ${file_b}/${file_name}-m-0${i}* ${TEMP_DIR}/${tableName}/$ANALY_DATE/$ANALY_HOUR/${file_name}${i}.gz &
echo "###############################################"
echo "hdfs dfs -getmerge ${file_b}/${file_name}-m-0${i}* ${TEMP_DIR}/${tableName}/$ANALY_DATE/$ANALY_HOUR/${file_name}${i}.gz"
echo "###############################################"
fi
done
else
echo "this table has no data."
fi
done

orgn=volteorgn
rm -rf $TEMP_DIR/volte_orgn/$ANALY_DATE/$ANALY_HOUR/*
mkdir -p ${TEMP_DIR}/volte_orgn/$ANALY_DATE/$ANALY_HOUR
hdfs dfs -get ${OUTPUT}/${orgn}* ${TEMP_DIR}/volte_orgn/$ANALY_DATE/$ANALY_HOUR

DEL_HOUR="`date -d ' -6 hour' +%H`"
if [ $ANALY_HOUR = 00 ]
then
   ANALY_DATE="`date -d ' -1 day' +%Y%m%d`"
elif [ $ANALY_HOUR = 01 ]; then
   ANALY_DATE="`date -d ' -1 day' +%Y%m%d`"
elif [ $ANALY_HOUR = 02 ]; then
   ANALY_DATE="`date -d ' -1 day' +%Y%m%d`"
elif [ $ANALY_HOUR = 03 ]; then
   ANALY_DATE="`date -d ' -1 day' +%Y%m%d`"
elif [ $ANALY_HOUR = 04 ]; then
   ANALY_DATE="`date -d ' -1 day' +%Y%m%d`"
elif [ $ANALY_HOUR = 23 ]; then
   ANALY_DATE="`date -d ' -1 day' +%Y%m%d`"
else
   ANALY_DATE=$1
fi
hdfs dfs -rm -R -skipTrash /datang/FILTER/${ANALY_DATE}${DEL_HOUR}
echo "get data end!------------------------------------------"
echo "`date '+/%y/%m/%d %H:%M:%S'` INFO done ,exit..."
exit