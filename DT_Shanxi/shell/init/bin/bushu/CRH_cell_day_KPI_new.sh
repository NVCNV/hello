#!/bin/sh
ANALY_DATE=$1
DB=$2
CAL_DATE="${ANALY_DATE:0:(4)}-${ANALY_DATE:4:(2)}-${ANALY_DATE:6:(2)} 00:00:00"
echo $DB
hive<<EOF
set mapreduce.map.memory.mb=4096;set mapreduce.reduce.memory.mb=8192;set mapreduce.map.java.opts=-Xmx3482m;set mapreduce.reduce.java.opts=-Xmx6963m;
USE $DB;
alter table kpi_mid_cell_day drop partition(dt="$ANALY_DATE");
alter table kpi_mid_cell_day add partition(dt="$ANALY_DATE");
insert into kpi_mid_cell_day partition(dt="$ANALY_DATE")
(ttime,cellid,dir_state,kpi031,kpi032,kpi033,kpi034,kpi035,kpi037,kpi039,kpi041,
kpi042,kpi043,kpi044,kpi045,kpi046,kpi047,kpi009,kpi010,kpi020,kpi019,kpi022,kpi021,kpi001,
kpi003,kpi004,kpi029,kpi030,kpi023,kpi024,kpi025,kpi026,kpi013,kpi014,kpi017,kpi018,kpi011,kpi012,kpi005,
kpi006,kpi007,kpi008,kpi015,kpi016,kpi027,kpi028, kpi071,kpi072,kpi079,kpi075
)
select "$CAL_DATE",cellid,dir_state,sum(kpi031),sum(kpi032),sum(kpi033),sum(kpi034),sum(kpi035),sum(kpi037),sum(kpi039),sum(kpi041),
sum(kpi042),sum(kpi043),sum(kpi044),sum(kpi045),sum(kpi046),sum(kpi047),sum(kpi009),sum(kpi010),
sum(kpi020),sum(kpi019),sum(kpi022),sum(kpi021),sum(kpi001),sum(kpi003),sum(kpi004),sum(kpi029),
sum(kpi030),sum(kpi023),sum(kpi024),sum(kpi025),sum(kpi026),sum(kpi013),sum(kpi014),sum(kpi017),
sum(kpi018),sum(kpi011),sum(kpi012),sum(kpi005),sum(kpi006),sum(kpi007),sum(kpi008),sum(kpi015),
sum(kpi016),sum(kpi027),sum(kpi028),sum( kpi071),sum(kpi072),sum(kpi079),sum(kpi075)
from kpi_mid_cell_hour
where dt="$ANALY_DATE"
group by cellid,dir_state;
EOF