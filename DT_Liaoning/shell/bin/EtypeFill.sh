#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin

ANALY_DATE=$1
ANALY_HOUR=$2

#ANALY_DATE=`date +%Y%m%d`
#ANALY_HOUR="`date -d ' -0 hour' +%H`"
SOURCE_SVR="/datang"
DDL_SVR="hdfs://dtcluster/user/hive/warehouse/dcl.db/"
JAR_FILE="/dt/lib/DT_mobile.jar"
ETYPE_MAIN="cn.com.dtmobile.hadoop.biz.exception.job.ProcessJob"

VOLTE_RX=${SOURCE_SVR}/tb_xdr_ifc_gxrx/dt=${ANALY_DATE}/h=${ANALY_HOUR}/*
VOLTE_ORGN=${SOURCE_SVR}/volte_orgn/${ANALY_DATE}/${ANALY_HOUR}/*
S1MME_ORGN=${SOURCE_SVR}/s1mme_orgn/${ANALY_DATE}/${ANALY_HOUR}/*
TB_XDR_IFC_UU=${DDL_SVR}/tb_xdr_ifc_uu/dt=${ANALY_DATE}/h=${ANALY_HOUR}/*
TB_XDR_IFC_X2=${DDL_SVR}/tb_xdr_ifc_x2/dt=${ANALY_DATE}/h=${ANALY_HOUR}/*
MW_IP=${DDL_SVR}/MW_IP/mwip.csv
ETYPE_OUT=${SOURCE_SVR}/ETYPE_OUT/${ANALY_DATE}/${ANALY_HOUR}

hdfs dfs -rm -R -skipTrash ${ETYPE_OUT}

hadoop jar ${JAR_FILE} ${ETYPE_MAIN}  ${VOLTE_RX} ${VOLTE_ORGN} ${S1MME_ORGN} ${TB_XDR_IFC_UU} ${TB_XDR_IFC_X2} ${ETYPE_OUT} ${MW_IP}

hdfs dfs -rm -R -skipTrash ${ETYPE_OUT}/_SUCCESS