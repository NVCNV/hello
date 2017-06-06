#!/bin/sh
ANALY_DATE=$1
ANALY_HOUR=$2
CAL_DATE="${ANALY_DATE:0:(4)}-${ANALY_DATE:4:(2)}-${ANALY_DATE:6:(2)} ${ANALY_HOUR}:00:00"
DB=$3
DEFAULTDB=$4

uusql="
select cellid,(eupordown)dir_state,0 as voltemcsucc,0 as voltemcatt,0 as voltevdsucc,0 as voltevdatt,0 as voltetime,0 as voltemctime,0 as voltevdtime,
0 as voltemchandover,0 as volteanswer,0 as voltevdhandover,0 as voltevdanswer,0 as srvccsucc,0 as srvccatt,0 as srvcctime,
sum(case when (proceduretype = 7 or proceduretype = 8) and procedurestatus=0 then 1 else 0 end)lteswsucc,
sum(case when (proceduretype = 7 or proceduretype = 8) then 1 else 0 end)lteswatt,
0 as srqatt ,0 as srqsucc ,0 as tauatt  ,0 as tausucc,
sum(case when ProcedureType=4 then 1 else 0 end)rrcrebuild,
sum(case when ProcedureType=1 and ProcedureStatus=0 then 1 else 0 end)rrcsucc,
sum(case when ProcedureType=1 then 1 else 0 end)rrcreq,
0 as imsiregatt,0 as imsiregsucc,0 as wirelessdrop,0 as wireless,0 as eabdrop,0 as eab,
0 as eabs1swx,0 as eabs1swy,0 as s1tox2swx,0 as s1tox2swy,0 as enbx2swx,0 as enbx2swy,
sum(case when ProcedureType=8 and ProcedureStatus=0 then 1 else 0 end)uuenbswx,
sum(case when ProcedureType=8 then 1 else 0 end)uuenbswy,
sum(case when ProcedureType=7 and ProcedureStatus=0 then 1 else 0 end)uuenbinx,
sum(case when ProcedureType=7 then 1 else 0 end)uuenbiny,
sum(case when ProcedureType=7 and ProcedureStatus=0 then 1 else 0 end)swx,
sum(case when ProcedureType=7 then 1 else 0 end)swy,
0 as attachx,0 as attachy,0 as  pagereq,0 as pageresp,0 as pageshowtimeall,
0 as pageresptimeall,0 as pageshowsucc,0 as httpdownflow,0 as httpdowntime,0 as mediareq,0 as mediasucc,0 as mediadownflow,0 as mediadowntime,
0 as voltesucc,
0 as browse_download_visits,
0 as video_service_visits,
0 as instant_message_visits,
0 as app_visits,
0 as browse_download_business,
0 as video_service_business,
0 as instant_message_business,
0 as app_business
          from tb_xdr_ifc_uu_new
          WHERE dt="$ANALY_DATE" and h="$ANALY_HOUR"
                 group by 
                  CELLID,eupordown
"
x2sql="
select CELLID,(eupordown)dir_state,0 as voltemcsucc,0 as voltemcatt,0 as voltevdsucc,0 as voltevdatt,0 as voltetime,0 as voltemctime,0 as voltevdtime,
0 as voltemchandover,0 as volteanswer,0 as voltevdhandover,0 as voltevdanswer,
0 as srvccsucc,0 as srvccatt,0 as srvcctime,
0 as lteswsucc,0 as lteswatt,0 as srqatt ,0 as srqsucc ,0 as tauatt  ,0 as tausucc,0 as rrcrebuild,0 as rrcsucc,0 as rrcreq,0 as imsiregatt,0 as imsiregsucc,
0 as wirelessdrop,0 as wireless,0 as eabdrop,0 as eab,0 as eabs1swx,0 as eabs1swy,0 as s1tox2swx,0 as s1tox2swy,
sum(case when ProcedureType=1 AND ProcedureStatus=0 then 1 else 0 end)enbx2swx,
sum(case when ProcedureType=1 AND (ProcedureStatus=0 or ((ProcedureStatus between 1 and 255) and (failurecause!=1000 or failurecause is null))) then 1 else 0 end)enbx2swy,
0 as uuenbswx,0 as uuenbswy,0 as uuenbinx,0 as uuenbiny,
sum(case when ProcedureType=1 AND ProcedureStatus=0 then 1 else 0 end)swx,
sum(case when ProcedureType=1 then 1 else 0 end)swy,
0 as attachx,0 as attachy,0 as  pagereq,0 as pageresp,0 as pageshowtimeall,
0 as pageresptimeall,0 as pageshowsucc,0 as httpdownflow,0 as httpdowntime,0 as mediareq,0 as mediasucc,0 as mediadownflow,0 as mediadowntime,
0 as voltesucc,
0 as browse_download_visits,
0 as video_service_visits,
0 as instant_message_visits,
0 as app_visits,
0 as browse_download_business,
0 as video_service_business,
0 as instant_message_business,
0 as app_business
          from tb_xdr_ifc_x2_new
          WHERE dt="$ANALY_DATE" and h="$ANALY_HOUR"
                 group by 
                  CELLID,eupordown
"
svsql="
select (SOURCEECI)cellid,(eupordown)dir_state,0 as voltemcsucc,0 as voltemcatt,0 as voltevdsucc,0 as voltevdatt,0 as voltetime,0 as voltemctime,0 as voltevdtime,
0 as voltemchandover,0 as volteanswer,0 as voltevdhandover,0 as voltevdanswer,
sum(case when ProcedureType=1 and RESULT=0 then 1 else 0 end)srvccsucc,
sum(case when ProcedureType=1 then 1 else 0 end)srvccatt,
sum(case when ProcedureType=1 and RESULT=0 then SVDELAY end)srvcctime,
0 as lteswsucc,0 as lteswatt,0 as srqatt ,0 as srqsucc ,0 as tauatt  ,0 as tausucc,0 as rrcrebuild,0 as rrcsucc,0 as rrcreq,0 as imsiregatt,0 as imsiregsucc,
0 as wirelessdrop,0 as wireless,0 as eabdrop,0 as eab,0 as eabs1swx,0 as eabs1swy,0 as s1tox2swx,0 as s1tox2swy,0 as enbx2swx,0 as enbx2swy,
0 as uuenbswx,0 as uuenbswy,0 as uuenbinx,0 as uuenbiny,0 as swx,0 as swy,0 as attachx,0 as attachy,0 as  pagereq,0 as pageresp,0 as pageshowtimeall,
0 as pageresptimeall,0 as pageshowsucc,0 as httpdownflow,0 as httpdowntime,0 as mediareq,0 as mediasucc,0 as mediadownflow,0 as mediadowntime,
0 as voltesucc,
0 as browse_download_visits,
0 as video_service_visits,
0 as instant_message_visits,
0 as app_visits,
0 as browse_download_business,
0 as video_service_business,
0 as instant_message_business,
0 as app_business
          from tb_xdr_ifc_sv_new
          WHERE dt="$ANALY_DATE" and h="$ANALY_HOUR"
                 group by 
                  SOURCEECI,eupordown
"
voltesipsql="
select (sourceeci)cellid,(eupordown)dir_state,
0 as voltemcsucc,
0 as voltemcatt,
0 as voltevdsucc,
0 as voltevdatt,
0 as voltetime,
0 as voltemctime,
0 as voltevdtime,
0 as voltemchandover,
0 as volteanswer,
0 as voltevdhandover,
0 as voltevdanswer,
0 as srvccsucc,0 as srvccatt,0 as srvcctime,0 as lteswsucc,0 as lteswatt,0 as srqatt ,
0 as srqsucc ,0 as tauatt  ,0 as tausucc,0 as rrcrebuild,0 as rrcsucc,0 as rrcreq,
sum(case when ProcedureType=1 and interface=14 then 1 else 0 end)imsiregatt,
sum(case when ProcedureType=1 and interface=14 and ProcedureStatus=0 then 1 else 0 end)imsiregsucc,
0 as wirelessdrop,0 as wireless,0 as eabdrop,0 as eab,0 as eabs1swx,0 as eabs1swy,0 as s1tox2swx,0 as s1tox2swy,0 as enbx2swx,0 as enbx2swy,
0 as uuenbswx,0 as uuenbswy,0 as uuenbinx,0 as uuenbiny,0 as swx,0 as swy,0 as attachx,0 as attachy,0 as  pagereq,0 as pageresp,0 as pageshowtimeall,
0 as pageresptimeall,0 as pageshowsucc,0 as httpdownflow,0 as httpdowntime,0 as mediareq,0 as mediasucc,0 as mediadownflow,0 as mediadowntime,
0 as voltesucc,
0 as browse_download_visits,
0 as video_service_visits,
0 as instant_message_visits,
0 as app_visits,
0 as browse_download_business,
0 as video_service_business,
0 as instant_message_business,
0 as app_business
          from tb_xdr_ifc_gmmwmgmimjisc_new
          WHERE dt="$ANALY_DATE" and h="$ANALY_HOUR"
                 group by 
               sourceeci,eupordown
"

voltesip0sql="
select (sourceeci)cellid,(eupordown)dir_state,
sum(case when ProcedureType=5 and interface=14 and ServiceType=0 and alertingtime is not null then 1 else 0 end)voltemcsucc,
sum(case when ProcedureType=5 and interface=14 and ServiceType=0 then 1 else 0 end)voltemcatt,
sum(case when ProcedureType=5 and interface=14 and ServiceType=1 and alertingtime is not null then 1 else 0 end)voltevdsucc,
sum(case when ProcedureType=5 and interface=14 and ServiceType=1 then 1 else 0 end)voltevdatt,
sum(case when ProcedureType=5 and interface=14 and alertingtime is not null then alertingtime else 0 end)voltetime,
sum(case when ProcedureType=5 and interface=14 and ServiceType=0 then callduration else 0 end)voltemctime,
sum(case when ProcedureType=5 and interface=14 and ServiceType=1 then callduration else 0 end)voltevdtime,
0 as voltemchandover,
sum(case when ProcedureType=5 and ServiceType=0 and Answertime is not null then 1 else 0 end)volteanswer,
0 as voltevdhandover,
sum(case when ProcedureType=5 and ServiceType=1 and Answertime is not null then 1 else 0 end)voltevdanswer,
0 as srvccsucc,0 as srvccatt,0 as srvcctime,0 as lteswsucc,0 as lteswatt,0 as srqatt ,
0 as srqsucc ,0 as tauatt  ,0 as tausucc,0 as rrcrebuild,0 as rrcsucc,0 as rrcreq,
sum(case when ProcedureType=1 and interface=14 then 1 else 0 end)imsiregatt,
sum(case when ProcedureType=1 and interface=14 and ProcedureStatus=0 then 1 else 0 end)imsiregsucc,
0 as wirelessdrop,0 as wireless,0 as eabdrop,0 as eab,0 as eabs1swx,0 as eabs1swy,0 as s1tox2swx,0 as s1tox2swy,0 as enbx2swx,0 as enbx2swy,
0 as uuenbswx,0 as uuenbswy,0 as uuenbinx,0 as uuenbiny,0 as swx,0 as swy,0 as attachx,0 as attachy,0 as  pagereq,0 as pageresp,0 as pageshowtimeall,
0 as pageresptimeall,0 as pageshowsucc,0 as httpdownflow,0 as httpdowntime,0 as mediareq,0 as mediasucc,0 as mediadownflow,0 as mediadowntime,
sum(case when ProcedureType=5 and alertingtime is not null then 1 else 0 end)voltesucc,
0 as browse_download_visits,
0 as video_service_visits,
0 as instant_message_visits,
0 as app_visits,
0 as browse_download_business,
0 as video_service_business,
0 as instant_message_business,
0 as app_business
          from tb_xdr_ifc_gmmwmgmimjisc_new
         where callside=0 and dt="$ANALY_DATE" and h="$ANALY_HOUR"
                 group by 
               sourceeci,eupordown
"

voltesip1sql="
select (desteci)cellid,(eupordown)dir_state,
sum(case when ProcedureType=5 and interface=14 and ServiceType=0 and alertingtime is not null then 1 else 0 end)voltemcsucc,
sum(case when ProcedureType=5 and interface=14 and ServiceType=0 then 1 else 0 end)voltemcatt,
sum(case when ProcedureType=5 and interface=14 and ServiceType=1 and alertingtime is not null then 1 else 0 end)voltevdsucc,
sum(case when ProcedureType=5 and interface=14 and ServiceType=1 then 1 else 0 end)voltevdatt,
sum(case when ProcedureType=5 and interface=14 and alertingtime is not null then alertingtime else 0 end)voltetime,
sum(case when ProcedureType=5 and interface=14 and ServiceType=0 then callduration else 0 end)voltemctime,
sum(case when ProcedureType=5 and interface=14 and ServiceType=1 then callduration else 0 end)voltevdtime,
0 as voltemchandover,
sum(case when ProcedureType=5 and ServiceType=0 and Answertime is not null then 1 else 0 end)volteanswer,
0 as voltevdhandover,
sum(case when ProcedureType=5 and ServiceType=1 and Answertime is not null then 1 else 0 end)voltevdanswer,
0 as srvccsucc,0 as srvccatt,0 as srvcctime,0 as lteswsucc,0 as lteswatt,0 as srqatt ,
0 as srqsucc ,0 as tauatt  ,0 as tausucc,0 as rrcrebuild,0 as rrcsucc,0 as rrcreq,
sum(case when ProcedureType=1 and interface=14 then 1 else 0 end)imsiregatt,
sum(case when ProcedureType=1 and interface=14 and ProcedureStatus=0 then 1 else 0 end)imsiregsucc,
0 as wirelessdrop,0 as wireless,0 as eabdrop,0 as eab,0 as eabs1swx,0 as eabs1swy,0 as s1tox2swx,0 as s1tox2swy,0 as enbx2swx,0 as enbx2swy,
0 as uuenbswx,0 as uuenbswy,0 as uuenbinx,0 as uuenbiny,0 as swx,0 as swy,0 as attachx,0 as attachy,0 as  pagereq,0 as pageresp,0 as pageshowtimeall,
0 as pageresptimeall,0 as pageshowsucc,0 as httpdownflow,0 as httpdowntime,0 as mediareq,0 as mediasucc,0 as mediadownflow,0 as mediadowntime,
sum(case when ProcedureType=5 and alertingtime is not null then 1 else 0 end)voltesucc,
0 as browse_download_visits,
0 as video_service_visits,
0 as instant_message_visits,
0 as app_visits,
0 as browse_download_business,
0 as video_service_business,
0 as instant_message_business,
0 as app_business
          from tb_xdr_ifc_gmmwmgmimjisc_new
       where callside=1 and dt="$ANALY_DATE" and h="$ANALY_HOUR"
                 group by 
                  desteci,eupordown
"


s1_usql="
select
(ECGI)CELLID,(eupordown)dir_state,
0 as voltemcsucc,
0 as voltemcatt,
0 as voltevdsucc,
0 as voltevdatt,
0 as voltetime,
0 as voltemctime,
0 as voltevdtime,
0 as voltemchandover,
0 as volteanswer,
0 as voltevdhandover,
0 as voltevdanswer,
0 as srvccsucc,
0 as srvccatt,
0 as srvcctime,
0 as lteswsucc,
0 as lteswatt,
0 as srqatt,
0 as srqsucc,
0 as tauatt,
0 as tausucc,
0 as rrcrebuild,
0 as rrcsucc,
0 as rrcreq,
0 as imsiregatt,
0 as imsiregsucc,
0 as wirelessdrop,
0 as wireless,
0 as eabdrop,
0 as eab,
0 as eabs1swx,
0 as eabs1swy,
0 as s1tox2swx,
0 as s1tox2swy,
0 as enbx2swx,
0 as enbx2swy,
0 as uuenbswx,
0 as uuenbswy,
0 as uuenbinx,
0 as uuenbiny,
0 as swx,
0 as swy,
0 as attachx,
0 as attachy,
count(case when Interface = 11 and APPTYPECODE = 103  then 1 else 0 end ) pagereq,
count(case when Interface = 11 and APPTYPECODE = 103 and HTTPFIRSTREDE != 0 and HTTPFIRSTREDE != 'FFFFFFFFFFFFFF1'  then 1 else 0 end)pageresp,
sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0  and HTTPLASTREDE != 'FFFFFFFFFFFFFF1' then   CASE WHEN TCPATTNUM > 0 THEN HTTPLASTREDE + TCPESTABREDE + TCPESTABDECONF + TCPFIRSTREDE WHEN TCPATTNUM = 0 THEN  HTTPLASTREDE  END  end)pageshowtimeall,
sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPFIRSTREDE != 0 and HTTPFIRSTREDE != 'FFFFFFFFFFFFFF1' then  CASE WHEN TCPATTNUM > 0 THEN  HTTPFIRSTREDE + TCPESTABREDE + TCPESTABDECONF + TCPFIRSTREDE  WHEN TCPATTNUM = 0 THEN HTTPFIRSTREDE  END end)pageresptimeall,
count(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0 and HTTPLASTREDE != 'FFFFFFFFFFFFFF1' then 1 else 0 end)pageshowsucc,
sum(case when Interface = 11 and APPTYPECODE = 103 then DLData / 1024 end)httpdownflow,
sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0 and HTTPLASTREDE != 'FFFFFFFFFFFFFF1' then HTTPLASTREDE - HTTPFIRSTREDE end)httpdowntime,
count(case when Interface = 11 and APPTYPECODE = 107 then 1 else 0 end)mediareq,
count(case when Interface = 11 and APPTYPECODE = 107 and APPSTATUS = 0  then 1 else 0 end)mediasucc,
sum(case when Interface = 11 and APPTYPE = 5  then DLData / 1024 end ) mediadownflow,
sum(case when Interface = 11 and APPTYPE = 5  then PROCEDUREENDTIME - PROCEDURESTARTTIME end ) mediadowntime,
0 as voltesucc,
count(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 then 1 else 0 end ) browse_download_visits,
count(case when Interface = 11 and (APPTYPECODE = 103 or APPTYPECODE = 107) and APPTYPE = 5 then 1 else 0 end ) video_service_visits,
count(case when Interface = 11 and APPTYPECODE = 108 and APPTYPE = 1 then 1 else 0 end ) instant_message_visits,
count(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 7 then 1 else 0 end ) app_visits,
sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 then DLDATA end ) browse_download_business,
sum(case when Interface = 11 and (APPTYPECODE = 103 or APPTYPECODE = 107) and APPTYPE = 5 then DLDATA end )video_service_business,
sum(case when Interface = 11 and APPTYPECODE = 108 and APPTYPE = 1 then DLDATA end ) instant_message_business,
count(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 7 then DLDATA end ) app_business
from TB_XDR_IFC_HTTP
WHERE dt="$ANALY_DATE" and h="$ANALY_HOUR"
               group by
                  ECGI,eupordown
"


#excel sheet名：VOLTE_Rx接口小区汇总
#volterxsql="(select ECGI,(eupordown)dir_state,0 as voltemcsucc, 0 as voltemcatt, 0 as voltevdsucc, 0 as voltevdatt, 0 as voltetime, 0 as voltemctime, 0 as voltevdtime, 
#count(case when Interface = 26 and ProcedureType = 3 and MEDIATYPE = 0 and AbortCause in (0, 1, 2, 4) then 1 end) voltemchandover, 0 as volteanswer, 
#count(case when Interface = 26 and ProcedureType = 3 and MEDIATYPE = 1 and AbortCause in (0, 1, 2, 4) then 1 end) voltevdhandover, 0 as voltevdanswer,
#0 as srvccsucc, 0 as srvccatt, 0 as srvcctime, 0 as lteswsucc, 0 as lteswatt, 0 as srqatt , 0 as srqsucc , 0 as tauatt  , 0 as tausucc, 0 as rrcrebuild, 
#0 as rrcsucc, 0 as rrcreq, 0 as imsiregatt, 0 as imsiregsucc, 0 as wirelessdrop, 0 as wireless, 0 as eabdrop, 0 as eab, 0 as eabs1swx, 0 as eabs1swy, 0 as s1tox2swx, 0 as s1tox2swy, 0 as enbx2swx, 
#0 as enbx2swy, 0 as uuenbswx, 0 as uuenbswy, 0 as uuenbinx, 0 as uuenbiny, 0 as swx, 0 as swy, 0 as attachx, 0 as attachy, 0 as  pagereq, 0 as pageresp, 0 as pageshowtimeall, 0 as pageresptimeall, 
#0 as pageshowsucc, 0 as httpdownflow, 0 as httpdowntime, 0 as mediareq, 0 as mediasucc, 0 as mediadownflow, 0 as mediadowntime,
#0 as voltesucc,
#0 as browse_download_visits,0 as video_service_visits,0 as instant_message_visits,0 as app_visits,0 as browse_download_business,0 as video_service_business,
#0 as instant_message_business,0 as app_business
#          from TB_XDR_IFC_GXRX
#           WHERE dt="$ANALY_DATE" and h="$ANALY_HOUR"
#              group by 
#                  ECGI,eupordown)"


s1mmesql="select CELLID,(eupordown)dir_state,0 as voltemcsucc, 0 as voltemcatt, 0 as voltevdsucc, 0 as voltevdatt, 0 as voltetime, 0 as voltemctime, 0 as voltevdtime, 0 as voltemchandover, 0 as volteanswer, 
0 as voltevdhandover, 0 as voltevdanswer, 0 as srvccsucc, 0 as srvccatt, 0 as srvcctime, 0 as lteswsucc, 0 as lteswatt, 
sum(case when INTERFACE = 5 and proceduretype = 2 then 1 else 0 end ) srqatt , 
sum(case when INTERFACE = 5 and proceduretype = 2 and procedurestatus = 0 then 1 else 0 end ) as srqsucc , 
sum(case when INTERFACE = 5 and proceduretype = 5 then 1 else 0 end ) tauatt  , 
sum(case when INTERFACE = 5 and proceduretype = 5 and procedurestatus = 0 then 1 else 0 end ) tausucc,
0 as rrcrebuild, 0 as rrcsucc, 0 as rrcreq, 0 as imsiregatt, 0 as imsiregsucc, 
sum(case when proceduretype = 20 and Keyword1=0 AND RequestCause is not null AND RequestCause not in (2, 20, 23, 24, 28, 512, 514) then 1 else 0 end) wirelessdrop, 
sum(case when proceduretype = 18 AND ProcedureStatus=0 then 1 else 0 end ) wireless, 
sum(case when proceduretype = 21 
and BEARER0REQUESTCAUSE is not null and BEARER1REQUESTCAUSE is not null
and BEARER0REQUESTCAUSE not in(2, 20, 23, 24,28, 512, 514) 
and BEARER1REQUESTCAUSE not in(2, 20, 23, 24,28, 512, 514)
then 1 else 0 end )eabdrop, 
0 as eab, 0 as eabs1swx, 
sum(case when INTERFACE = 5 and proceduretype = 16 and keyword1 = 1 then 1 else 0 end )eabs1swy, 
sum(case when INTERFACE = 5 and proceduretype = 14 and procedurestatus = 0 then 1 else 0 end )s1tox2swx, 
sum(case when INTERFACE = 5 and proceduretype = 14 then 1 else 0 end ) s1tox2swy, 
0 as enbx2swx, 0 as enbx2swy, 0 as uuenbswx, 0 as uuenbswy, 0 as uuenbinx, 0 as uuenbiny, 
0 as swx,
sum(case when INTERFACE = 5 and proceduretype = 16 then 1 else 0 end )swy, 
sum(case when INTERFACE = 5 and proceduretype = 1 and procedurestatus = 0 then 1 else 0 end ) attachx, 
sum(case when INTERFACE = 5 and proceduretype = 1 then 1 else 0 end ) attachy, 
0 as  pagereq,0 as pageresp,0 as pageshowtimeall,0 as pageresptimeall, 0 as pageshowsucc, 0 as httpdownflow,0 as httpdowntime,0 as mediareq,0 as mediasucc,0 as mediadownflow,0 as mediadowntime,
0 as voltesucc,
0 as browse_download_visits,
0 as video_service_visits,
0 as instant_message_visits,
0 as app_visits,
0 as browse_download_business,
0 as video_service_business,
0 as instant_message_business,
0 as app_business
          from tb_xdr_ifc_s1mme_new T
          WHERE dt="$ANALY_DATE" and h="$ANALY_HOUR"
               group by 
                  CELLID,eupordown"

S1MMEHANDOVER="
select CELLID,(eupordown)dir_state,
0 as voltemcsucc, 
0 as voltemcatt, 
0 as voltevdsucc, 
0 as voltevdatt, 
0 as voltetime, 
0 as voltemctime, 
0 as voltevdtime, 
0 as voltemchandover, 
0 as volteanswer, 
0 as voltevdhandover, 
0 as voltevdanswer, 
0 as srvccsucc, 
0 as srvccatt, 
0 as srvcctime, 
0 as lteswsucc,
0 as lteswatt, 
0 as srqatt , 
0 as srqsucc,
0 as tauatt, 
0 as tausucc,
0 as rrcrebuild, 
0 as rrcsucc,
0 as rrcreq, 
0 as imsiregatt, 
0 as imsiregsucc, 
0 as wirelessdrop,
0 as wireless,
0 as eabdrop,
0 as eab,
count(1) as eabs1swx,
0 as eabs1swy,
0 as s1tox2swx,
0 as s1tox2swy,
0 as enbx2swx,
0 as enbx2swy,
0 as uuenbswx,
0 as uuenbswy,
0 as uuenbinx,
0 as uuenbiny,
count(1) as swx,
0 as swy,
0 as attachx,
0 as attachy,
0 as pagereq,
0 as pageresp,
0 as pageshowtimeall,
0 as pageresptimeall,
0 as pageshowsucc,
0 as httpdownflow,
0 as httpdowntime,
0 as mediareq,
0 as mediasucc,
0 as mediadownflow,
0 as mediadowntime,
0 as voltesucc,
0 as browse_download_visits,
0 as video_service_visits,
0 as instant_message_visits,
0 as app_visits,
0 as browse_download_business,
0 as video_service_business,
0 as instant_message_business,
0 as app_business
from
(select distinct S1MME_1.* from
(select * from tb_xdr_ifc_s1mme_new where dt="$ANALY_DATE" and h="$ANALY_HOUR" and PROCEDURETYPE = 16 and keyword1 = 1 AND PROCEDURESTATUS = 0 AND IMSI is not NULL) S1MME_1 left join
(select * from tb_xdr_ifc_s1mme_new where dt="$ANALY_DATE" and h="$ANALY_HOUR" and PROCEDURETYPE = 20  and requestcause = 2 AND IMSI is not NULL) S1MME_2
on S1MME_1.IMSI = S1MME_2.IMSI AND S1MME_1.CELLID = S1MME_2.CELLID where S1MME_2.PROCEDURESTARTTIME between S1MME_1.PROCEDURESTARTTIME and S1MME_1.PROCEDURESTARTTIME + 6*1000) a
group by CELLID,eupordown
"

hive<<EOF
set mapreduce.map.memory.mb=4096;set mapreduce.reduce.memory.mb=8192;set mapreduce.map.java.opts=-Xmx3482m;set mapreduce.reduce.java.opts=-Xmx6963m;
USE $DB;
alter table kpi_mid_cell_hour drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table kpi_mid_cell_hour add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");

insert into table kpi_mid_cell_hour partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
(ttime,cellid,dir_state,kpi031,kpi032,kpi033,kpi034,kpi035,kpi037,kpi039,kpi041,
kpi042,kpi043,kpi044,kpi045,kpi046,kpi047,kpi009,kpi010,kpi020,kpi019,kpi022,kpi021,kpi001,
kpi003,kpi004,kpi029,kpi030,kpi023,kpi024,kpi025,kpi026,kpi013,kpi014,kpi017,kpi018,kpi011,kpi012,kpi005,
kpi006,kpi007,kpi008,kpi015,kpi016,kpi027,kpi028, kpi071,kpi072,kpi079,kpi075,kpi077,kpi083,kpi084,
kpi086,kpi087,kpi090,kpi091,kpi036,kpi092,kpi093,kpi094,kpi095,kpi096,kpi097,kpi098,kpi099
)
select "$CAL_DATE",CELLID,dir_state,sum(voltemcsucc),sum(voltemcatt),sum(voltevdsucc),sum(voltevdatt),sum(voltetime),
sum(voltemctime),sum(voltevdtime),sum(voltemchandover),sum(volteanswer),sum(voltevdhandover),sum(voltevdanswer),sum(srvccsucc),
sum(srvccatt),sum(srvcctime),sum(lteswsucc),sum(lteswatt),sum(srqatt),sum(srqsucc),sum(tauatt),sum(tausucc),sum(rrcrebuild),
sum(rrcsucc),sum(rrcreq),sum(imsiregatt),sum(imsiregsucc),sum(wirelessdrop),sum(wireless),sum(eabdrop),sum(eab),sum(eabs1swx),
sum(eabs1swy),sum(s1tox2swx),sum(s1tox2swy),sum(enbx2swx),sum(enbx2swy),sum(uuenbswx),sum(uuenbswy),sum(uuenbinx),sum(uuenbiny),
sum(swx),sum(swy),sum(attachx),sum(attachy),sum(pagereq),sum(pageresp),sum(pageshowtimeall),sum(pageresptimeall),sum(pageshowsucc),
sum(httpdownflow),sum(httpdowntime),sum(mediareq),sum(mediasucc),sum(mediadownflow),sum(mediadowntime),sum(voltesucc),sum(browse_download_visits),
sum(video_service_visits),sum(instant_message_visits),sum(app_visits),sum(browse_download_business),
sum(video_service_business),sum(instant_message_business),sum(app_business)
from
($svsql
 union
 $uusql
 union
 $x2sql
 union
 $voltesipsql
 union
 $voltesip0sql
 union
 $voltesip1sql
 union
 $s1mmesql
 union
 $S1MMEHANDOVER
) t7
group by cellid,dir_state;
EOF
