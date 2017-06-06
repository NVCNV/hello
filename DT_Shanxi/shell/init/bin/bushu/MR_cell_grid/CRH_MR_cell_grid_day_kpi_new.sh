#!/bin/bash
ANALY_DATE=$1
DB=$2
CAL_DATE="${ANALY_DATE:0:(4)}-${ANALY_DATE:4:(2)}-${ANALY_DATE:6:(2)} 00:00:00"
hive<<EOF
set mapreduce.map.memory.mb=4096;set mapreduce.reduce.memory.mb=8192;set mapreduce.map.java.opts=-Xmx3482m;set mapreduce.reduce.java.opts=-Xmx6963m;
USE ${DB};
alter table mro_kpi_mid_imsi_day drop partition(dt="$ANALY_DATE");
alter table mro_kpi_mid_imsi_day add partition(dt="$ANALY_DATE");
insert into mro_kpi_mid_cell_grid_day partition(dt="$ANALY_DATE")
(cellid,rruid,gridid,ttime,dir_state,kpi001,kpi002,kpi003,kpi004,kpi005,kpi006,kpi007,kpi010,kpi011,kpi012,kpi013,kpi014,kpi015,kpi016,kpi017,kpi018,kpi019
)
select cellid,rruid,gridid,"$CAL_DATE",dir_state,sum(kpi001),sum(kpi002),sum(kpi003),sum(kpi004),sum(kpi005),sum(kpi006),sum(kpi007),sum(kpi010),
sum(kpi011),sum(kpi012),sum(kpi013),sum(kpi014),sum(kpi015),sum(kpi016),sum(kpi017),sum(kpi018),sum(kpi019)
from mro_kpi_mid_cell_grid_hour where dt="$ANALY_DATE"
group by cellid,rruid,gridid,dir_state;
EOF