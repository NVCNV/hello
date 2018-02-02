#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin


ANALY_DATE=$1
ANALY_HOUR=$2

DB="dcl.db"
JAR_FILE="/dt/lib/DT_mobile.jar"
EXCEPTION_MAIN="cn.com.dtmobile.hadoop.biz.exception.job.ExceptionCommonJob"

SOURCE_SVR="hdfs://dtcluster/datang"
LTE_MRO_SOURCE="/user/hive/warehouse/${DB}/lte_mro_source/dt=${ANALY_DATE}/h=${ANALY_HOUR}/*"
SV_TABLE="${SOURCE_SVR}/volte_sv/${ANALY_DATE}/${ANALY_HOUR}/*"
EXCEPTION_MAP="${SOURCE_SVR}/exception_map/EXCEPTIONMAP.csv"
cellMR="hdfs://dtcluster/user/hive/warehouse/${DB}/cell_mr/dt=${ANALY_DATE}/h=${ANALY_HOUR}/*"
ETYPE_OUT="${SOURCE_SVR}/ETYPE_OUT/${ANALY_DATE}/${ANALY_HOUR}"
ltecell="hdfs://dtcluster/user/hive/warehouse/${DB}/ltecell/liaoning.csv"
T_PROCESS="${SOURCE_SVR}/t_process/t_process.csv"
EXCEPTION_OUT="hdfs://dtcluster/user/hive/warehouse/${DB}/exception_analysis/dt=${ANALY_DATE}/h=${ANALY_HOUR}"
NCELLINFO="${SOURCE_SVR}/nCellInfo/NCellInfo.csv"   #小区与邻区关系表
configFile="hdfs://dtcluster/datang/properties/Parameter.properties"      #异常可变参数配置文件

echo " -----------> hdfs dfs -rm -R -skipTrash ${SOURCE_SVR}/cell_mr.csv "
hdfs dfs -rm -R -skipTrash ${SOURCE_SVR}/cell_mr.csv

echo " -----------> hdfs dfs -getmerge ${cellMR} cell_mr.csv "
hdfs dfs -getmerge ${cellMR} cell_mr.csv

echo " -----------> hdfs dfs -put cell_mr.csv to ${SOURCE_SVR} "
hdfs dfs -put cell_mr.csv ${SOURCE_SVR}


echo " -----------> hdfs dfs  -rm -R ${EXCEPTION_OUT}"
hdfs dfs  -rm -R ${EXCEPTION_OUT}

time hadoop jar ${JAR_FILE} ${EXCEPTION_MAIN} \
${ETYPE_OUT}/TB_XDR_IFC_UU* \
${LTE_MRO_SOURCE} \
${ETYPE_OUT}/VOLTE_ORGN* \
${ETYPE_OUT}/S1MME_ORGN* \
${ETYPE_OUT}/TB_XDR_IFC_X2* \
${ETYPE_OUT}/rx* \
${SV_TABLE} \
${EXCEPTION_OUT} \
${EXCEPTION_MAP} \
${SOURCE_SVR}/cell_mr.csv \
${ltecell} \
${NCELLINFO} \
${configFile} \
0

echo "------------> rm EXCEPTION_OUT "=
hdfs dfs -rm -R -skipTrash ${ETYPE_OUT}

echo " -----------> rm local cell_mr "
rm -fr cell_mr.csv


hdfs dfs -rm ${EXCEPTION_OUT}/_SUCCESS