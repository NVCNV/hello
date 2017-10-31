#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin

ANALY_DATE=$1
ANALY_HOUR=$2
RESULTDB=$3
VERSION=$4

JAR_FILE="/dt/lib/dt_mobile.jar"
EXCEPTION_MAIN="cn.com.dtmobile.hadoop.biz.exception.job.ExceptionCommonJob"

SOURCE_SVR="hdfs://dtcluster/datang2/"
LTE_MRO_SOURCE=${SOURCE_SVR}/output/xdrnew/lte_mro_source/${ANALY_DATE}/${ANALY_HOUR}/*
SV_TABLE=${SOURCE_SVR}/output/xdrnew/tb_xdr_ifc_sv/${ANALY_DATE}/${ANALY_HOUR}/*
EXCEPTION_MAP="${SOURCE_SVR}/parameter/exception_map/EXCEPTIONMAP.csv"
cellMR=hdfs://dtcluster/user/hive/warehouse/${RESULTDB}.db/lte_cellmr_source/dt=${ANALY_DATE}/h=${ANALY_HOUR}/*
ETYPE_OUT=${SOURCE_SVR}/ETYPE_OUT/${ANALY_DATE}/${ANALY_HOUR}
ltecell="${SOURCE_SVR}/parameter/ltcell/shanxi.csv"
NCELLINFO="${SOURCE_SVR}/parameter/nCellInfo/NCellInfo.csv"   #小区与邻区关系表
configFile="${SOURCE_SVR}/parameter/properties/Parameter.properties"      #异常可变参数配置文件
EXCEPTION_OUT=hdfs://dtcluster/user/hive/warehouse/${RESULTDB}.db/exception_analysis/dt=${ANALY_DATE}/h=${ANALY_HOUR}

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
${LTE_MRO_SOURCE} ${ETYPE_OUT}/VOLTE_ORGN* \
${ETYPE_OUT}/S1MME_ORGN* \
${ETYPE_OUT}/TB_XDR_IFC_X2* \
${ETYPE_OUT}/rx* \
${SOURCE_SVR}/volte_sv.txt \
${EXCEPTION_OUT} \
${EXCEPTION_MAP} \
${SOURCE_SVR}/cell_mr.csv \
${ltecell} \
${NCELLINFO} \
${configFile} \
${VERSION}

echo "------------> rm EXCEPTION_OUT "=
#hdfs dfs -rm -R -skipTrash ${ETYPE_OUT}

echo " -----------> rm local cell_mr "
rm -fr cell_mr.csv