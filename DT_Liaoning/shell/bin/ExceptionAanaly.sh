#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin


ANALY_DATE=$1
ANALY_HOUR=$2


JAR_FILE="/dt/lib/DT_mobile.jar"
EXCEPTION_MAIN="cn.com.dtmobile.hadoop.biz.exception.job.ExceptionCommonJob"

SOURCE_SVR="hdfs://dtcluster/datang"
ETYPE_OUT=${SOURCE_SVR}/ETYPE_OUT
LTE_MRO_SOURCE=hdfs://dtcluster/user/hive/warehouse/dcl.db/lte_mro_source/dt=${ANALY_DATE}/h=${ANALY_HOUR}
SV_TABLE=${SOURCE_SVR}/volte_sv/${ANALY_DATE}/${ANALY_HOUR}/*
EXCEPTION_MAP=/datang/exception_map/EXCEPTIONMAP.tsv
cellMR=hdfs://dtcluster/user/hive/warehouse/dcl.db/cell_mr/dt=${ANALY_DATE}/h=${ANALY_HOUR}/000000_0
ltecell=/datang/ltcell/ltcell.csv
T_PROCESS=/datang/t_process/t_process.csv
EXCEPTION_OUT=hdfs://dtcluster/user/hive/warehouse/dcl.db/exception_analysis/dt=${ANALY_DATE}/h=${ANALY_HOUR}

hdfs dfs -rm -R ${EXCEPTION_OUT}
hadoop jar ${JAR_FILE} ${EXCEPTION_MAIN} \
${ETYPE_OUT}/TB_XDR_IFC_UU/* \
${LTE_MRO_SOURCE}/* \
${ETYPE_OUT}/VOLTE_ORGN/* \
${ETYPE_OUT}/S1MME_ORGN/* \
${ETYPE_OUT}/TB_XDR_IFC_X2/* \
${ETYPE_OUT}/RX/* \
${SOURCE_SVR}/volte_sv.txt \
${EXCEPTION_OUT} \
${EXCEPTION_MAP} \
${cellMR} \
${ltecell}