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
ObjectID,
startTime,
endTime,
eNBID,
mrName,
cellID,
meaTime,
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
length,
City,
Interface,
XDRID,
RAT
from ${INITDATABASE}.lte_mro_source where mrname='MR.LteScRIP0' and dt="${ANALY_DATE}" and h="${ANALY_HOUR}";


EOF
