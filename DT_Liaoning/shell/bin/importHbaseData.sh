#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export HADOOP_CONF_DIR=/opt/app/hdconf
export HBASE_CONF_DIR=/opt/app/hbconf
export HBASE_HOME=/opt/app/hbase
export HADOOP_CLASSPATH=/opt/app/hbase/lib/*:/dt/lib/dthbase01.jar
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HBASE_HOME/bin

ANALY_DATE=$1
ANALY_HOUR=$2

input_dir=hdfs://dtcluster/user/hive/warehouse/dcl.db
#input_dir=hdfs://dtcluster/user/hive/warehouse/lndcl.db
yingcai_input_dir=hdfs://dtcluster/datang/
#yingcai_input_dir=hdfs://dtcluster/liaoning/
s1mme=${yingcai_input_dir}/s1mme_orgn/${ANALY_DATE}/${ANALY_HOUR}
x2=${input_dir}/tb_xdr_ifc_x2/dt=${ANALY_DATE}/h=${ANALY_HOUR}
uu=${input_dir}/tb_xdr_ifc_uu/dt=${ANALY_DATE}/h=${ANALY_HOUR}
gxrx=${yingcai_input_dir}/volte_rx/${ANALY_DATE}/${ANALY_HOUR}
mrosorce=${input_dir}/lte_mro_source/dt=${ANALY_DATE}/h=${ANALY_HOUR}
http=${yingcai_input_dir}/s1u_http_orgn/${ANALY_DATE}/${ANALY_HOUR}
mw=${yingcai_input_dir}/volte_orgn/${ANALY_DATE}/${ANALY_HOUR}
xdr_output_dir=hdfs://dtcluster/output/xdr
mrosorce_output_dir=hdfs://dtcluster/output/mrosource

hdfs dfs  -rm -R -skipTrash ${mrosorce_output_dir}
hdfs dfs  -rm -R -skipTrash ${xdr_output_dir}
time hadoop jar /dt/lib/dthbase01.jar cn.com.dtmobile.xdr.job.ImportCommonXdrDataJob ${s1mme} ${x2} ${uu} ${gxrx} ${mrosorce} ${http} ${mw} ${xdr_output_dir} -l true -c true
time hadoop jar /dt/lib/dthbase01.jar cn.com.dtmobile.xdr.job.ImportXdlDataJob ${mrosorce} ${mrosorce_output_dir} -l true -c true 8