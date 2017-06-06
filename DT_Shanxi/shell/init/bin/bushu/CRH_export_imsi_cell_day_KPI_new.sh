#!/bin/bash
ANALY_DATE=$1
DB=$2
hive<<EOF
set mapreduce.map.memory.mb=4096;set mapreduce.reduce.memory.mb=8192;set mapreduce.map.java.opts=-Xmx3482m;set mapreduce.reduce.java.opts=-Xmx6963m;
USE ${DB};
alter table volte_gt_user_ana_baseday drop partition(dt="$ANALY_DATE");
alter table volte_gt_user_ana_baseday add partition(dt="$ANALY_DATE");
insert into volte_gt_user_ana_baseday partition(dt="$ANALY_DATE")
(ttime,imsi,msisdn,cellid,dir_state,voltemcsucc,voltemcatt,voltevdsucc,voltevdatt,voltetime,voltemctime,voltevdtime,voltemchandover,
volteanswer,voltevdhandover,voltevdanswer,srvccsucc,srvccatt,srvcctime,lteswsucc,lteswatt,srqatt,srqsucc,tauatt,tausucc,rrcrebuild,rrcsucc,rrcreq,imsiregatt,imsiregsucc,wirelessdrop,wireless,eabdrop,eab,eabs1swx,eabs1swy,s1tox2swx,s1tox2swy,enbx2swx,enbx2swy,uuenbswx,uuenbswy,uuenbinx,uuenbiny,swx,swy,attachx,attachy,pagereq,pageresp,pageshowtimeall,pageresptimeall,pageshowsucc,httpdownflow,httpdowntime,mediareq,mediasucc,mediadownflow,mediadowntime,
browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness
)
select ttime,imsi,msisdn,cellid,dir_state,kpi031,kpi032,kpi033,kpi034,kpi035,kpi037,kpi039,kpi041,
kpi042,kpi043,kpi044,kpi045,kpi046,kpi047,kpi009,kpi010,kpi020,kpi019,kpi022,kpi021,kpi001,
kpi003,kpi004,kpi029,kpi030,kpi023,kpi024,kpi025,kpi026,kpi013,kpi014,kpi017,kpi018,kpi011,kpi012,kpi005,
kpi006,kpi007,kpi008,kpi015,kpi016,kpi027,kpi028, kpi071,kpi072,kpi079,kpi075,kpi077,kpi083,kpi084,
kpi086,kpi087,kpi090,kpi091,kpi092,kpi093,kpi094,kpi095,kpi096,kpi097,kpi098,kpi099
from kpi_mid_imsi_cell_day
where dt="$ANALY_DATE";
EOF