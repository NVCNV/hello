#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin
mypath="$(cd "$(dirname "$0")"; pwd)"
cd $mypath
ANALY_DATE=$1
ANALY_HOUR=$2
echo "@@@@@@@@@@@@${ANALY_DATE}@@@@@@@@@@@@@"
echo "@@@@@@@@@@@@${ANALY_HOUR}@@@@@@@@@@@@@"
HIVE_PATH="hdfs://dtcluster/datang"
HIVE_TABLES="s1mme_orgn volte_rx s1u_http_orgn volte_sv volte_orgn"
LOCAL_PATH=/data1/taiyue/TEMP
echo "`date '+/%y/%m/%d %H:%M:%S'` INFO begin ..."
if [ ! -d "${LOCAL_PATH}" ]; then
    sh /dt/bin/sshfs.sh
fi
for tableName in ${HIVE_TABLES}
do
hdfs dfs -test -e ${HIVE_PATH}/${tableName}/${ANALY_DATE}/${ANALY_HOUR}
if [ $? -eq 0 ] ;then 
   hdfs dfs -rm -r ${HIVE_PATH}/${tableName}/${ANALY_DATE}/${ANALY_HOUR}
fi
echo "hdfs dfs -mkdir -p ${HIVE_PATH}/${tableName}/${ANALY_DATE}/${ANALY_HOUR}"
hdfs dfs -mkdir -p ${HIVE_PATH}/${tableName}/${ANALY_DATE}/${ANALY_HOUR}
echo "put ${LOCAL_PATH}/${tableName}/${ANALY_DATE}/${ANALY_HOUR} to ${HIVE_PATH}/${tableName}/${ANALY_DATE}/"
hdfs dfs -put ${LOCAL_PATH}/${tableName}/${ANALY_DATE}/${ANALY_HOUR} ${HIVE_PATH}/${tableName}/${ANALY_DATE}/
echo "rm -rf ${LOCAL_PATH}/${tableName}/${ANALY_DATE}/${ANALY_HOUR}"
rm -rf ${LOCAL_PATH}/${tableName}/${ANALY_DATE}/${ANALY_HOUR}
done
echo "`date '+/%y/%m/%d %H:%M:%S'` INFO done ,exit..."
echo "Success!"


