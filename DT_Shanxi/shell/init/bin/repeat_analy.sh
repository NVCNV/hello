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


ANALY_DATE=`date +%Y%m%d`
ANALY_HOUR="`date -d ' -2 hour' +%H`"

CUR_DATE=`date +%Y%m%d`
CUR_HOUR="`date -d ' -0 hour' +%H`"
#ANALY_DATE=$CUR_DATE
if [ $CUR_HOUR = 00 ]
then
   ANALY_DATE="`date -d ' -1 day' +%Y%m%d`"
elif [ $CUR_HOUR = 01 ]; then
   ANALY_DATE="`date -d ' -1 day' +%Y%m%d`"
else
   ANALY_DATE=$CUR_DATE
fi

echo $ANALY_DATE
echo $ANALY_HOUR

echo $ANALY_DATE
echo $ANALY_HOUR
mkdir /dt/bin/sqoop_log

./repeat_volteTrain.sh ${ANALY_DATE} ${ANALY_HOUR} >> job_logs.log 2>&1
./bushu/repeat_addpartion.sh ${ANALY_DATE} ${ANALY_HOUR} result init >> job_logs.log 2>&1
./bushu/kpiAnaly.sh ${ANALY_DATE} ${ANALY_HOUR} result  init >> job_logs.log 2>&1
sh cellMrFilter.sh  ${ANALY_DATE} ${ANALY_HOUR} init result datang2 >>  job_logs.log 2>&1

./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/tb_xdr_ifc_mw/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_gmmwmgmimjisc_new 19 2 >> /dt/bin/sqoop_log/mw_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/tb_xdr_ifc_s1mme/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_s1mme_new 20 2 >> /dt/bin/sqoop_log/s1mme_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/tb_xdr_ifc_uu/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_uu_new 21 2 >> /dt/bin/sqoop_log/uu_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/tb_xdr_ifc_x2/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_x2_new 22 2 >> /dt/bin/sqoop_log/x2_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/tb_xdr_ifc_sv/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_sv_new 25 2  >> /dt/bin/sqoop_log/sv_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/tb_xdr_ifc_gxrx/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_gxrx_new 23 2 >> /dt/bin/sqoop_log/sgs_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/sgs/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_sgs_new 24 2 >> job_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/lte_mro_source/${ANALY_DATE}/${ANALY_HOUR} lte_mro_source_new 26 2 >>/dt/bin/sqoop_log/mr_source_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/userHome/${ANALY_DATE}_${ANALY_HOUR} user_home 31 2  >> job_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/trainiden/${ANALY_DATE} numberiden 35 2 >> job_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/volte_gtuser_data/${ANALY_DATE}/${ANALY_HOUR} volte_gtuser_data 28 2 >> /dt/bin/sqoop_log/gtuser_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/u1/${ANALY_DATE}/${ANALY_HOUR} u1 27 2 >> /dt/bin/sqoop_log/u1_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/u2/${ANALY_DATE}/${ANALY_HOUR} u2 27 2 >> /dt/bin/sqoop_log/u2_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/u3/${ANALY_DATE}/${ANALY_HOUR} u3 27 2 >> /dt/bin/sqoop_log/u3_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/u4/${ANALY_DATE} u4 27 2 >> /dt/bin/sqoop_log/u4_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/updowntrain/${ANALY_DATE} upordown 29 2 >> /dt/bin/sqoop_log/upordown_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/business/${ANALY_DATE}/${ANALY_HOUR}/loc_guser_mark* loc_guser_mark 8 2 >> /dt/bin/sqoop_log/business_guser_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/free/${ANALY_DATE}/${ANALY_HOUR}/VOLTE_GT_FREE_USER* volte_gt_free_user_data 30 2 >> /dt/bin/sqoop_log/free_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/business/${ANALY_DATE}/${ANALY_HOUR}/VOLTE_GT_BUSI_USER* volte_gt_busi_user_data 9 2 >> /dt/bin/sqoop_log/business_user_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
#./hdfs2db.sh hdfs://dtcluster/datang2/output/free/${ANALY_DATE}/${ANALY_HOUR}/COMM_USER_DATA* comm_user_data 10 2 >> job_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/output/business/${ANALY_DATE}/${ANALY_HOUR}/COMM_USER_DATA* comm_user_data 10 2 >> /dt/bin/sqoop_log/comm_user_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1
./hdfs2db.sh hdfs://dtcluster/datang2/cellMR/${ANALY_DATE}/${ANALY_HOUR} VOLTE_CELLMR_DATA 32 2 >> /dt/bin/sqoop_log/volte_cellmr_${ANALY_DATE}_${ANALY_HOUR}_logs.log 2>&1



 


