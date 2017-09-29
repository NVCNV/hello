#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin

ANALY_DATE=$1
ANALY_HOUR=$2

SOURCE_SVR="hdfs://dtcluster/datang2/output/xdrnew"
JAR_FILE="/dt/lib/dt_mobile.jar"
ETYPE_MAIN="cn.com.dtmobile.hadoop.biz.exception.job.ProcessJob"

VOLTE_RX=${SOURCE_SVR}/tb_xdr_ifc_gxrx/${ANALY_DATE}/${ANALY_HOUR}/*
VOLTE_ORGN=${SOURCE_SVR}/tb_xdr_ifc_mw/${ANALY_DATE}/${ANALY_HOUR}/*
S1MME_ORGN=${SOURCE_SVR}/tb_xdr_ifc_s1mme/${ANALY_DATE}/${ANALY_HOUR}/*
TB_XDR_IFC_UU=${SOURCE_SVR}/tb_xdr_ifc_uu/${ANALY_DATE}/${ANALY_HOUR}/*
TB_XDR_IFC_X2=${SOURCE_SVR}/tb_xdr_ifc_x2/${ANALY_DATE}/${ANALY_HOUR}/*
ETYPE_OUT=/datang2/ETYPE_OUT/${ANALY_DATE}/${ANALY_HOUR}

hdfs dfs -rm -R -skipTrash ${ETYPE_OUT}

hadoop jar ${JAR_FILE} ${ETYPE_MAIN} ${VOLTE_RX} ${VOLTE_ORGN} ${S1MME_ORGN} ${TB_XDR_IFC_UU} ${TB_XDR_IFC_X2} ${ETYPE_OUT}
