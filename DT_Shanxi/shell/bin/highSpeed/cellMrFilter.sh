#!/usr/bin/env bash


ANALY_DATE=$1
ANALY_HOUR=$2
INITDATABASE=$3
RESULTDATABASE=$4
SOURCEDATABASE=$5

hive<<EOF


alter table ${INITDATABASE}.lte_mro_source add if not exists partition(dt="${ANALY_DATE}",h="${ANALY_HOUR}")
location "/${SOURCEDATABASE}/LTE_MRO_SOURCE/${ANALY_DATE}/${ANALY_HOUR}" ;

alter table  ${RESULTDATABASE}.lte_cellmr_source drop if exists  partition(dt="${ANALY_DATE}",h="${ANALY_HOUR}");
alter table ${RESULTDATABASE}.lte_cellmr_source set serdeproperties('serialization.null.format' = '');

insert into ${RESULTDATABASE}.lte_cellmr_source  partition(dt="${ANALY_DATE}",h="${ANALY_HOUR}")
select
objectid,
vid,
fileformatversion,
starttime,
endtime,
period,
enbid,
userlabel,
mrname,
cellid,
earfcn,
subframenbr,
prbnbr,
mmeues1apid,
mmegroupid,
mmecode,
meatime,
eventtype,
gridcenterlongitude,
gridcenterlatitude,
kpi1,
kpi2,
kpi3,
kpi4,
kpi5,
kpi6,
kpi7,
kpi8,
kpi9,
kpi10,
kpi11,
kpi12,
kpi13,
kpi14,
kpi15,
kpi16,
kpi17,
kpi18,
kpi19,
kpi20,
kpi21,
kpi22,
kpi23,
kpi24,
kpi25,
kpi26,
kpi27,
kpi28,
kpi29,
kpi30,
kpi31,
kpi32,
kpi33,
kpi34,
kpi35,
kpi36,
kpi37,
kpi38,
kpi39,
kpi40,
kpi41,
kpi42,
kpi43,
kpi44,
kpi45,
kpi46,
kpi47,
kpi48,
kpi49,
kpi50,
kpi51,
kpi52,
kpi53,
kpi54,
kpi55,
kpi56,
kpi57,
kpi58,
kpi59,
kpi60,
kpi61,
kpi62,
kpi63,
kpi64,
kpi65,
kpi66,
kpi67,
kpi68,
kpi69,
kpi70,
kpi71,
length,
city,
xdrtype,
interface,
xdrid,
rat,
imsi,
imei,
msisdn,
mrtype,
neighborcellnumber,
gsmneighborcellnumber,
tdsneighborcellnumber,
v_enb,
mrtime
from ${INITDATABASE}.lte_mro_source where mrname='MR.LteScRIP0' and dt="${ANALY_DATE}" and h="${ANALY_HOUR}";


EOF
