#!/bin/bash
ANALY_DATE=$1
ANALY_HOUR=$2
DB=$3
hive<<EOF
set mapreduce.map.memory.mb=4096;set mapreduce.reduce.memory.mb=8192;set mapreduce.map.java.opts=-Xmx3482m;set mapreduce.reduce.java.opts=-Xmx6963m;
USE ${DB};
alter table mr_gt_cell_ana_base60 drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table mr_gt_cell_ana_base60 add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");

insert into mr_gt_cell_ana_base60 partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
(cellid,ttime,dir_state,avgrsrpx,commy,avgrsrqx,ltecoverratex,weakcoverratex,overlapcoverratex,overlapcoverratey,updiststrox,updiststroy,upsigrateavgx,upsigrateavgy,uebootx,uebooty,model3diststrox,model3diststroy)
select cellid,ttime,dir_state,kpi001,kpi002,kpi003,kpi004,kpi005,kpi006,kpi007,kpi011,kpi012,kpi014,kpi015,kpi016,kpi017,kpi018,kpi019
from shanxikpi.mro_kpi_mid_cell_hour
where dt="$ANALY_DATE" and h="$ANALY_HOUR";
EOF