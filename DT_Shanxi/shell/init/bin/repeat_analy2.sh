#!/bin/bash
export JAVA_HOME=/opt/app/java
export KYLIN_HOME=/opt/app/kylin
export MAVEN_HOME=/opt/app/maven
export ANT_HOME=/opt/app/ant
export SCALA_HOME=/opt/app/scala
export HADOOP_HOME=/opt/app/hadoop
export HIVE_HOME=/opt/app/hive
export HADOOP_CONF_DIR=/opt/app/hdconf
export HBASE_CONF_DIR=/opt/app/hbconf
export HBASE_HOME=/opt/app/hbase
export SPARK_HOME=/opt/app/spark
export PATH=$PATH:$JAVA_HOME/bin:$SCALA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$MAVEN_HOME/bin:$HBASE_HOME/bin:$HBASE_HOME/sbin:$HIVE_HOME/bin:$KYLIN_HOME/bin:$ANT_HOME/bin
mypath="$(cd "$(dirname "$0")"; pwd)"
cd $mypath


ANALY_DATE=$1
ANALY_HOUR=$2


./repeat_volteTrain.sh ${ANALY_DATE} ${ANALY_HOUR} 
./bushu/repeat_addpartion.sh ${ANALY_DATE} ${ANALY_HOUR} shanxikpi2
./bushu/kpiAnaly.sh ${ANALY_DATE} ${ANALY_HOUR} shanxikpi2 ddl_for_kpi


./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/mw/${ANALY_DATE}/${ANALY_HOUR}/tb* tb_xdr_ifc_gmmwmgmimjisc_new 19 2
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/s1mme/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_s1mme_new 20 2
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/uu/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_uu_new 21 2 
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/x2/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_x2_new 22 2 
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/sv_xdr/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_sv_new 25 2 
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/gxrx/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_gxrx_new 23 2 
#./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/sgs/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_sgs_new 24 2 >> job_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/ltemrosource/${ANALY_DATE}/${ANALY_HOUR} lte_mro_source_new 26 2
#./hdfs2db.sh hdfs://dtcluster/datang2/output/userHome/${ANALY_DATE}/${ANALY_HOUR} user_home 31 2  >> job_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/trainiden/${ANALY_DATE}/${ANALY_HOUR} numberiden 35 2 >> job_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/mw/${ANALY_DATE}/${ANALY_HOUR}/volte* volte_gtuser_data 28 2
./hdfs2db.sh hdfs://dtcluster/datang2/output/u1/${ANALY_DATE}/${ANALY_HOUR} u1 27 2
./hdfs2db.sh hdfs://dtcluster/datang2/output/u2/${ANALY_DATE}/${ANALY_HOUR} u2 27 2
./hdfs2db.sh hdfs://dtcluster/datang2/output/u3/${ANALY_DATE}/${ANALY_HOUR} u3 27 2
#./hdfs2db.sh hdfs://dtcluster/datang2/output/u4/${ANALY_DATE}/${ANALY_HOUR} u4 27 2 >> /dt/bin/sqoop_log/u4_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/updowntrain/${ANALY_DATE}_${ANALY_HOUR} upordown 29 2 >> /dt/bin/sqoop_log/upordown_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/business/${ANALY_DATE}/${ANALY_HOUR}/loc_guser_mark* loc_guser_mark 8 2
./hdfs2db.sh hdfs://dtcluster/datang2/output/free/${ANALY_DATE}/${ANALY_HOUR}/VOLTE_GT_FREE_USER* volte_gt_free_user_data 30 2
./hdfs2db.sh hdfs://dtcluster/datang2/output/business/${ANALY_DATE}/${ANALY_HOUR}/VOLTE_GT_BUSI_USER* volte_gt_busi_user_data 9 2
#./hdfs2db.sh hdfs://dtcluster/datang2/output/free/${ANALY_DATE}/${ANALY_HOUR}/COMM_USER_DATA* comm_user_data 10 2 >> job_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/business/${ANALY_DATE}/${ANALY_HOUR}/COMM_USER_DATA* comm_user_data 10 2 
./hdfs2db.sh hdfs://dtcluster/datang2/cellMR/${ANALY_DATE}/${ANALY_HOUR} VOLTE_CELLMR_DATA 32 2


#./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/mw/${ANALY_DATE}/${ANALY_HOUR}/tb* tb_xdr_ifc_gmmwmgmimjisc_new 19 2 >> /dt/bin/sqoop_log/mw_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/s1mme/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_s1mme_new 20 2 >> /dt/bin/sqoop_log/s1mme_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/uu/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_uu_new 21 2 >> /dt/bin/sqoop_log/uu_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/x2/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_x2_new 22 2 >> /dt/bin/sqoop_log/x2_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/sv_xdr/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_sv_new 25 2  >> /dt/bin/sqoop_log/sv_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/gxrx/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_gxrx_new 23 2 >> /dt/bin/sqoop_log/sgs_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/sgs/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_sgs_new 24 2 >> job_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/ltemrosource/${ANALY_DATE}/${ANALY_HOUR} lte_mro_source_new 26 2 >>/dt/bin/sqoop_log/mr_source_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/userHome/${ANALY_DATE}/${ANALY_HOUR} user_home 31 2  >> job_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/trainiden/${ANALY_DATE}/${ANALY_HOUR} numberiden 35 2 >> job_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/mw/${ANALY_DATE}/${ANALY_HOUR}/volte* volte_gtuser_data 28 2 >> /dt/bin/sqoop_log/gtuser_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/u1/${ANALY_DATE}/${ANALY_HOUR} u1 27 2 >> /dt/bin/sqoop_log/u1_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/u2/${ANALY_DATE}/${ANALY_HOUR} u2 27 2 >> /dt/bin/sqoop_log/u2_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/u3/${ANALY_DATE}/${ANALY_HOUR} u3 27 2 >> /dt/bin/sqoop_log/u3_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/u4/${ANALY_DATE}/${ANALY_HOUR} u4 27 2 >> /dt/bin/sqoop_log/u4_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/updowntrain/${ANALY_DATE}/${ANALY_HOUR} upordown 29 2 >> /dt/bin/sqoop_log/upordown_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/business/${ANALY_DATE}/${ANALY_HOUR}/loc_guser_mark* loc_guser_mark 8 2 >> /dt/bin/sqoop_log/business_guser_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/free/${ANALY_DATE}/${ANALY_HOUR}/VOLTE_GT_FREE_USER* volte_gt_free_user_data 30 2 >> /dt/bin/sqoop_log/free_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/business/${ANALY_DATE}/${ANALY_HOUR}/VOLTE_GT_BUSI_USER* volte_gt_busi_user_data 9 2 >>/dt/bin/sqoop_log/business_user_${ANALY_DATE}/${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/free/${ANALY_DATE}/${ANALY_HOUR}/COMM_USER_DATA* comm_user_data 10 2 >> job_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/business/${ANALY_DATE}/${ANALY_HOUR}/COMM_USER_DATA* comm_user_data 10 2 >> job_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/cellMR/${ANALY_DATE}/${ANALY_HOUR} VOLTE_CELLMR_DATA 32 2 >> job_logs.log 2>&1
 


