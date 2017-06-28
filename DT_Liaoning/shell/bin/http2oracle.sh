#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
ANALY_DATE=$1
ANALY_HOUR=$2
SOURCE_DB=hdfs://dtcluster/httpkpi

sh /dt/bin/hdfs2db.sh ${SOURCE_DB}/cell_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 5 2

sh /dt/bin/hdfs2db.sh hdfs://dtcluster/httpkpi/cell_day_http 6 2

sh /dt/bin/hdfs2db.sh hdfs://dtcluster/httpkpi/imsi_cell_day_http 7 2

sh /dt/bin/hdfs2db.sh hdfs://dtcluster/httpkpi/imsi_cell_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 8 2

sh /dt/bin/hdfs2db.sh hdfs://dtcluster/httpkpi/sgw_day_http 9 2

sh /dt/bin/hdfs2db.sh hdfs://dtcluster/httpkpi/sgw_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 10 2

sh /dt/bin/hdfs2db.sh hdfs://dtcluster/httpkpi/sp_day_http 11 2

sh /dt/bin/hdfs2db.sh hdfs://dtcluster/httpkpi/sp_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 12 2

sh /dt/bin/hdfs2db.sh hdfs://dtcluster/httpkpi/t_xdr_event_msg/dt=${ANALY_DATE}/h=${ANALY_HOUR} 13 2

sh /dt/bin/hdfs2db.sh hdfs://dtcluster/httpkpi/tac_day_http 14 2

sh /dt/bin/hdfs2db.sh hdfs://dtcluster/httpkpi/tac_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 15 2

sh /dt/bin/hdfs2db.sh hdfs://dtcluster/httpkpi/imsi_day_http 16 2

sh /dt/bin/hdfs2db.sh hdfs://dtcluster/httpkpi/imsi_cell_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 17 2

sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/business.db/business_type_detail 18 4