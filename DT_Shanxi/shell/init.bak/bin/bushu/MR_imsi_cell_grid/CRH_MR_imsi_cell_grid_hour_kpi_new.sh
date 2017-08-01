#!/bin/sh
ANALY_DATE=$1
ANALY_HOUR=$2
DB=$3

CAL_DATE="${ANALY_DATE:0:(4)}-${ANALY_DATE:4:(2)}-${ANALY_DATE:6:(2)} ${ANALY_HOUR}:00:00"

addRRUID="
(select t.*,g.rruid from lte_mro_source_new t 
left join grid_rru g on t.gridid=g.gridid where t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR")t1
"

MRSQL="
(select imsi,msisdn,cellid,rruid,gridid,(eupordown)dir_state,xdrid,
sum(case when MRNAME='MR.LteScRSRP' AND VID=1 AND KPI1 is not NULL then KPI1-141 else 0 end)avgrsrpx,
sum(case when MRNAME='MR.LteScRSRP' AND VID=1 AND KPI1 is not NULL then 1 else 0 end)commy,
sum(case when MRNAME='MR.LteScRSRP' AND VID=1 AND KPI3 is not NULL then KPI3*0.5-20 else 0 end)avgrsrqx,
sum(case when MRNAME='MR.LteScRSRP' AND VID=1 AND KPI1 is not NULL AND (KPI1-141)>-110 THEN 1 else 0 end)ltecoverratex,
sum(case when MRNAME='MR.LteScRSRP' AND VID=1 AND KPI1 is not NULL AND (KPI1-141)<=-110 THEN 1 else 0 end)weakcoverratex,
sum(case when MRNAME='MR.LteScRSRP' AND KPI1 is not NULL AND KPI2 is not NULL and (kpi1-141)>-110 then 1 else 0 end)overlapcoverratex,
sum(case when MRNAME='MR.LteScRSRP' AND VID=1 AND KPI1 is not NULL AND KPI2 is not NULL THEN 1 else 0 end)overlapcoverratey,
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
sum(case when MRNAME='MR.LteScRIP0' AND KPI2 is not NULL THEN 1 else 0 end)updiststroy,
MAX(case when MRNAME='MR.LteScRSRP' AND KPI8 is not NULL THEN KPI8-11 else NULL end)upsigrateavgmax,
sum(case when MRNAME='MR.LteScRSRP' AND KPI8 is not NULL THEN KPI8-11 else 0 end)upsigrateavgx,
sum(case when MRNAME='MR.LteScRSRP' AND KPI8 is not NULL THEN 1 else 0 end)upsigrateavgy,
sum(case when MRNAME='MR.LteScRSRP' AND KPI6 is not NULL THEN 46-KPI6 else 0 end)uebootx,
sum(case when MRNAME='MR.LteScRSRP' AND KPI6 is not NULL THEN 1 else 0 end)uebooty,
sum(case when MRNAME='MR.LteScRSRP' AND KPI1 is not null and KPI2 is not null AND (kpi1-141)>-110 AND
abs(KPI1 - KPI2) < 6 and KPI10=KPI12 then 1 else 0 end) model3diststrox,
sum(case when MRNAME='MR.LteScRSRP' AND KPI1 is not null and KPI2 is not null AND (kpi1-141)>-110 AND
abs(KPI1 - KPI2) < 6 then 1 else 0 end)model3diststroy
          from $addRRUID
                 group by
                  imsi,msisdn,CELLID,rruid,gridid,eupordown,xdrid)t
"
hive<<EOF
set mapreduce.map.memory.mb=4096;set mapreduce.reduce.memory.mb=8192;set mapreduce.map.java.opts=-Xmx3482m;set mapreduce.reduce.java.opts=-Xmx6963m;

USE ${DB};
alter table mro_kpi_mid_imsi_cell_grid_hour drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table mro_kpi_mid_imsi_cell_grid_hour add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");

insert into mro_kpi_mid_imsi_cell_grid_hour partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
(ttime,imsi,msisdn,cellid,rruid,gridid,dir_state,kpi001,kpi002,kpi003,kpi004,kpi005,kpi006,kpi007,kpi010,kpi011,kpi012,
kpi013,kpi014,kpi015,kpi016,kpi017,kpi018,kpi019
)
select "$CAL_DATE",imsi,msisdn,cellid,rruid,gridid,dir_state,sum(avgrsrpx),sum(commy),sum(avgrsrqx),sum(ltecoverratex),sum(weakcoverratex),
sum(case when overlapcoverratey>3 THEN 1 else 0 end)overlapcoverratex,
sum(overlapcoverratey),
max(greatest(updiststroxvalue1,updiststroxvalue2,updiststroxvalue3,updiststroxvalue4,updiststroxvalue5,updiststroxvalue6,updiststroxvalue7,updiststroxvalue8,
updiststroxvalue9,updiststroxvalue10))updiststromax,
sum(updiststroxvalue1+updiststroxvalue2+updiststroxvalue3+updiststroxvalue4+updiststroxvalue5+updiststroxvalue6+
updiststroxvalue7+updiststroxvalue8+updiststroxvalue9+updiststroxvalue10)updiststrox,
sum(updiststrox1+updiststrox2+updiststrox3+updiststrox4+updiststrox5+updiststrox6+updiststrox7+updiststrox8+updiststrox9+updiststrox10)updiststroy,
MAX(upsigrateavgmax),sum(upsigrateavgx),sum(upsigrateavgy),
sum(uebootx),sum(uebooty),
sum(model3diststrox)model3diststrox,
sum(model3diststrox)model3diststroy
from $MRSQL group by imsi,msisdn,cellid,rruid,gridid,dir_state;
EOF

