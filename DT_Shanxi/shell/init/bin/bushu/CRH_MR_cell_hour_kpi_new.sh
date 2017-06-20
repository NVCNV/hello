#!/bin/sh
ANALY_DATE=$1
ANALY_HOUR=$2
DB=$3
DEFAULT=$4
CAL_DATE="${ANALY_DATE:0:(4)}-${ANALY_DATE:4:(2)}-${ANALY_DATE:6:(2)} ${ANALY_HOUR}:00:00"

MRSQL0="
(select cellid,(eupordown)dir_state,xdrid,
sum(case when MRNAME='MR.LteScRSRP' AND VID=1 AND KPI1 is not NULL AND Eventtype='PERIOD' then KPI1-141 else 0 end)avgrsrpx,
sum(case when MRNAME='MR.LteScRSRP' AND VID=1 AND KPI1 is not NULL AND Eventtype='PERIOD' then 1 else 0 end)commy,
sum(case when MRNAME='MR.LteScRSRP' AND VID=1 AND KPI3 is not NULL AND Eventtype='PERIOD' then KPI3*0.5-20 else 0 end)avgrsrqx,
sum(case when MRNAME='MR.LteScRSRP' AND VID=1 AND KPI1 is not NULL AND (KPI1-141)>-110 AND Eventtype='PERIOD' THEN 1 else 0 end)ltecoverratex,
sum(case when MRNAME='MR.LteScRSRP' AND VID=1 AND KPI1 is not NULL AND (KPI1-141)<=-110 AND Eventtype='PERIOD' THEN 1 else 0 end)weakcoverratex,
sum(case when MRNAME='MR.LteScRSRP' AND KPI1 is not NULL AND KPI2 is not NULL and (kpi1-141)>-110 then 1 else 0 end)overlapcoverratex,
sum(case when MRNAME='MR.LteScRSRP' AND VID=1 AND KPI1 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)overlapcoverratey,
MAX(case when MRNAME='MR.LteScRSRP' AND KPI8 is not NULL THEN KPI8-11 else NULL end)upsigrateavgmax,
sum(case when MRNAME='MR.LteScRSRP' AND KPI8 is not NULL THEN KPI8-11 else 0 end)upsigrateavgx,
sum(case when MRNAME='MR.LteScRSRP' AND KPI8 is not NULL THEN 1 else 0 end)upsigrateavgy,
sum(case when MRNAME='MR.LteScRSRP' AND KPI6 is not NULL THEN 46-KPI6 else 0 end)uebootx,
sum(case when MRNAME='MR.LteScRSRP' AND KPI6 is not NULL THEN 1 else 0 end)uebooty,
sum(case when MRNAME='MR.LteScRSRP' AND KPI1 is not null and KPI2 is not null AND (kpi1-141)>-110 AND
abs(KPI1 - KPI2) < 6 and KPI10=KPI12 then 1 else 0 end) model3diststrox,
sum(case when MRNAME='MR.LteScRSRP' AND KPI1 is not null and KPI2 is not null AND (kpi1-141)>-110 AND
abs(KPI1 - KPI2) < 6 then 1 else 0 end)model3diststroy
          from lte_mro_source_new
          WHERE dt="$ANALY_DATE" and h="$ANALY_HOUR"
                 group by
                  CELLID,eupordown,xdrid)t1
"
MRSQL1="
select cellid,0 as dir_state,xdrid,
sum(case when MRNAME='MR.LteScRIP0' AND KPI1 is not NULL AND KPI2 is not NULL THEN KPI1*0.1-126.1 else 0 end)updiststroxvalue1,
sum(case when MRNAME='MR.LteScRIP0' AND KPI2 is not NULL AND KPI2 is not NULL THEN KPI2*0.1-126.1 else 0 end)updiststroxvalue2,
sum(case when MRNAME='MR.LteScRIP0' AND KPI3 is not NULL AND KPI2 is not NULL THEN KPI3*0.1-126.1 else 0 end)updiststroxvalue3,
sum(case when MRNAME='MR.LteScRIP0' AND KPI4 is not NULL AND KPI2 is not NULL THEN KPI4*0.1-126.1 else 0 end)updiststroxvalue4,
sum(case when MRNAME='MR.LteScRIP0' AND KPI5 is not NULL AND KPI2 is not NULL THEN KPI5*0.1-126.1 else 0 end)updiststroxvalue5,
sum(case when MRNAME='MR.LteScRIP0' AND KPI6 is not NULL AND KPI2 is not NULL THEN KPI6*0.1-126.1 else 0 end)updiststroxvalue6,
sum(case when MRNAME='MR.LteScRIP0' AND KPI7 is not NULL AND KPI2 is not NULL THEN KPI7*0.1-126.1 else 0 end)updiststroxvalue7,
sum(case when MRNAME='MR.LteScRIP0' AND KPI8 is not NULL AND KPI2 is not NULL THEN KPI8*0.1-126.1 else 0 end)updiststroxvalue8,
sum(case when MRNAME='MR.LteScRIP0' AND KPI9 is not NULL AND KPI2 is not NULL THEN KPI9*0.1-126.1 else 0 end)updiststroxvalue9,
sum(case when MRNAME='MR.LteScRIP0' AND KPI10 is not NULL AND KPI2 is not NULL THEN KPI10*0.1-126.1 else 0 end)updiststroxvalue10,
sum(case when MRNAME='MR.LteScRIP0' AND KPI1 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox1,
sum(case when MRNAME='MR.LteScRIP0' AND KPI2 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox2,
sum(case when MRNAME='MR.LteScRIP0' AND KPI3 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox3,
sum(case when MRNAME='MR.LteScRIP0' AND KPI4 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox4,
sum(case when MRNAME='MR.LteScRIP0' AND KPI5 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox5,
sum(case when MRNAME='MR.LteScRIP0' AND KPI6 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox6,
sum(case when MRNAME='MR.LteScRIP0' AND KPI7 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox7,
sum(case when MRNAME='MR.LteScRIP0' AND KPI8 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox8,
sum(case when MRNAME='MR.LteScRIP0' AND KPI9 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox9,
sum(case when MRNAME='MR.LteScRIP0' AND KPI10 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox10,
sum(case when MRNAME='MR.LteScRIP0' AND KPI2 is not NULL THEN 1 else 0 end)updiststroy
          from ${DEFAULT}.lte_mro_source
          WHERE dt="$ANALY_DATE" and h="$ANALY_HOUR"
                 group by
                  CELLID,xdrid
"

MRSQL2="
select cellid,1 as dir_state,xdrid,
sum(case when MRNAME='MR.LteScRIP0' AND KPI1 is not NULL AND KPI2 is not NULL THEN KPI1*0.1-126.1 else 0 end)updiststroxvalue1,
sum(case when MRNAME='MR.LteScRIP0' AND KPI2 is not NULL AND KPI2 is not NULL THEN KPI2*0.1-126.1 else 0 end)updiststroxvalue2,
sum(case when MRNAME='MR.LteScRIP0' AND KPI3 is not NULL AND KPI2 is not NULL THEN KPI3*0.1-126.1 else 0 end)updiststroxvalue3,
sum(case when MRNAME='MR.LteScRIP0' AND KPI4 is not NULL AND KPI2 is not NULL THEN KPI4*0.1-126.1 else 0 end)updiststroxvalue4,
sum(case when MRNAME='MR.LteScRIP0' AND KPI5 is not NULL AND KPI2 is not NULL THEN KPI5*0.1-126.1 else 0 end)updiststroxvalue5,
sum(case when MRNAME='MR.LteScRIP0' AND KPI6 is not NULL AND KPI2 is not NULL THEN KPI6*0.1-126.1 else 0 end)updiststroxvalue6,
sum(case when MRNAME='MR.LteScRIP0' AND KPI7 is not NULL AND KPI2 is not NULL THEN KPI7*0.1-126.1 else 0 end)updiststroxvalue7,
sum(case when MRNAME='MR.LteScRIP0' AND KPI8 is not NULL AND KPI2 is not NULL THEN KPI8*0.1-126.1 else 0 end)updiststroxvalue8,
sum(case when MRNAME='MR.LteScRIP0' AND KPI9 is not NULL AND KPI2 is not NULL THEN KPI9*0.1-126.1 else 0 end)updiststroxvalue9,
sum(case when MRNAME='MR.LteScRIP0' AND KPI10 is not NULL AND KPI2 is not NULL THEN KPI10*0.1-126.1 else 0 end)updiststroxvalue10,
sum(case when MRNAME='MR.LteScRIP0' AND KPI1 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox1,
sum(case when MRNAME='MR.LteScRIP0' AND KPI2 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox2,
sum(case when MRNAME='MR.LteScRIP0' AND KPI3 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox3,
sum(case when MRNAME='MR.LteScRIP0' AND KPI4 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox4,
sum(case when MRNAME='MR.LteScRIP0' AND KPI5 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox5,
sum(case when MRNAME='MR.LteScRIP0' AND KPI6 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox6,
sum(case when MRNAME='MR.LteScRIP0' AND KPI7 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox7,
sum(case when MRNAME='MR.LteScRIP0' AND KPI8 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox8,
sum(case when MRNAME='MR.LteScRIP0' AND KPI9 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox9,
sum(case when MRNAME='MR.LteScRIP0' AND KPI10 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)updiststrox10,
sum(case when MRNAME='MR.LteScRIP0' AND KPI2 is not NULL THEN 1 else 0 end)updiststroy
          from ${DEFAULT}.lte_mro_source
          WHERE dt="$ANALY_DATE" and h="$ANALY_HOUR"
                 group by CELLID,xdrid"


ltemr_new="select cellid,dir_state,sum(avgrsrpx)avgrsrpx,sum(commy)commy,sum(avgrsrqx)avgrsrqx,sum(ltecoverratex)ltecoverratex,sum(weakcoverratex)weakcoverratex,
sum(case when overlapcoverratex>3 THEN 1 else 0 end)overlapcoverratex,
sum(overlapcoverratey)overlapcoverratey,
0 as updiststromax,
0 as updiststrox,
0 as updiststroy,
MAX(upsigrateavgmax)upsigrateavgmax,sum(upsigrateavgx)upsigrateavgx,sum(upsigrateavgy)upsigrateavgy,
sum(uebootx)uebootx,sum(uebooty)uebooty,
sum(model3diststrox)model3diststrox,
sum(model3diststroy)model3diststroy
from $MRSQL0 group by cellid,dir_state"


ltemr="select cellid,dir_state,
0 as avgrsrpx,
0 as commy,
0 as avgrsrqx,
0 as ltecoverratex,
0 as weakcoverratex,
0 as overlapcoverratex,
0 as overlapcoverratey,
max(greatest(updiststroxvalue1,updiststroxvalue2,updiststroxvalue3,updiststroxvalue4,updiststroxvalue5,updiststroxvalue6,updiststroxvalue7,updiststroxvalue8,
updiststroxvalue9,updiststroxvalue10))updiststromax,
sum(updiststroxvalue1+updiststroxvalue2+updiststroxvalue3+updiststroxvalue4+updiststroxvalue5+updiststroxvalue6+
updiststroxvalue7+updiststroxvalue8+updiststroxvalue9+updiststroxvalue10)updiststrox,
sum(updiststrox1+updiststrox2+updiststrox3+updiststrox4+updiststrox5+updiststrox6+updiststrox7+updiststrox8+updiststrox9+updiststrox10)updiststroy,
0 as upsigrateavgmax,
0 as upsigrateavgx,
0 as upsigrateavgy,
0 as uebootx,
0 as uebooty,
0 as model3diststrox,
0 as model3diststroy
from 
(
$MRSQL1
union
$MRSQL2
)t2
group by cellid,dir_state"





hive<<EOF
set mapreduce.map.memory.mb=4096;set mapreduce.reduce.memory.mb=8192;set mapreduce.map.java.opts=-Xmx3482m;set mapreduce.reduce.java.opts=-Xmx6963m;
USE ${DB};
alter table mro_kpi_mid_cell_hour drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table mro_kpi_mid_cell_hour add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");

insert into mro_kpi_mid_cell_hour partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
(ttime,cellid,dir_state,kpi001,kpi002,kpi003,kpi004,kpi005,kpi006,kpi007,kpi010,kpi011,kpi012,
kpi013,kpi014,kpi015,kpi016,kpi017,kpi018,kpi019
)
select "$CAL_DATE",cellid,dir_state,sum(avgrsrpx),sum(commy),sum(avgrsrqx),sum(ltecoverratex),sum(weakcoverratex),sum(overlapcoverratex),sum(overlapcoverratey),sum(updiststromax),
sum(updiststrox),sum(updiststroy),sum(upsigrateavgmax),sum(upsigrateavgx),sum(upsigrateavgy),sum(uebootx),sum(uebooty),sum(model3diststrox),sum(model3diststroy)
from 
(
   $ltemr_new
   union
   $ltemr

)t3
group by cellid,dir_state;
EOF