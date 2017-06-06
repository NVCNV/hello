#!/bin/sh
#oracle jdbc url
URL=jdbc:oracle:thin:@10.204.215.99:1521/umorpho
#oracle username
USERNAME=scott
#oracle passwd
PASSWD=tiger
#export hdfs dir
HDFS_DIR=$1
#oracel table
TABLE=$2
#table cols
COL_NUM=$3
#map num
MAP_NUM=$4

#columns
KPI_IMSI_COLS=imsi,imei,msisdn,cellid,ttime,voltemcsucc,voltemcatt,voltevdsucc,voltevdatt,voltetime,voltemctime,voltevdtime,voltemchandover,volteanswer,voltevdhandover,voltevdanswer,srvccsucc,srvccatt,srvcctime,lteswsucc,lteswatt,srqatt,srqsucc,tauatt,tausucc,rrcrebuild,rrcsucc,rrcreq,imsiregatt,imsiregsucc,wirelessdrop,wireless,eabdrop,eab,eabs1swx,eabs1swy,s1tox2swx,s1tox2swy,enbx2swx,enbx2swy,uuenbswx,uuenbswy,uuenbinx,uuenbiny,swx,swy,attachx,attachy,pagereq,pageresp,pageshowtimeall,pageresptimeall,pageshowsucc,httpdownflow,httpdowntime,mediareq,mediasucc,mediadownflow,mediadowntime 

KPI_CELL_COLS='ttime,cellid,voltemcsucc,voltemcatt,voltevdsucc,voltevdatt,voltetime,voltemctime,voltevdtime,voltemchandover,volteanswer,voltevdhandover,voltevdanswer,srvccsucc,srvccatt,srvcctime,lteswsucc,lteswatt,srqatt,srqsucc,tauatt,tausucc,rrcrebuild,rrcsucc,rrcreq,imsiregatt,imsiregsucc,wirelessdrop,wireless,eabdrop,eab,eabs1swx,eabs1swy,s1tox2swx,s1tox2swy,enbx2swx,enbx2swy,uuenbswx,uuenbswy,uuenbinx,uuenbiny,swx,swy,attachx,attachy,pagereq,pageresp,pageshowtimeall,pageresptimeall,pageshowsucc,httpdownflow,httpdowntime,mediareq,mediasucc,mediadownflow,mediadowntime'

EVENT_MSG=EVENT_NAME,PROCEDURESTARTTIME,IMSI,PROCEDURETYPE,ETYPE,CELLID,FALURECAUSE,CELLTYPE,CELLREGION,CELLKEY,INTERFACE,PROINTERFACE,RANGETIME,elong,elat,EUPORDOWN

MR_CELL_COLS=cellid,ttime,dir_state,avgrsrpx,commy,avgrsrqx,ltecoverratex,weakcoverratex,overlapcoverratex,overlapcoverratey,upsigrateavgx,upsigrateavgy,updiststrox,updiststroy,model3diststrox,model3diststroy,uebootx,uebooty

MR_IMSI_COLS=imsi,imei,msisdn,cellid,rruid,gridid,ttime,dir_state,elong,elat,avgrsrpx,commy,avgrsrqx,ltecoverratex,weakcoverratex,overlapcoverratex,overlapcoverratey,upsigrateavgx,upsigrateavgy,updiststrox,updiststroy,model3diststrox,model3diststroy,uebootx,uebooty

X2=LENGTH,CITY,INTERFACE,XDRID,RAT,IMSI,IMEI,MSISDN,PROCEDURETYPE,PROCEDURESTARTTIME,PROCEDUREENDTIME,PROCEDURESTATUS,CELLID,TARGETCELLID,ENBID,TARGETENBID,MMEUES1APID,MMEGROUPID,MMECODE,REQUESTCAUSE,FAILURECAUSE,EPSBEARERNUMBER,BEARER0ID,BEARER0STATUS,BEARER1ID,BEARER1STATUS,BEARER2ID,BEARER2STATUS,BEARER3ID,BEARER3STATUS,BEARER4ID,BEARER4STATUS,BEARER5ID,BEARER5STATUS,BEARER6ID,BEARER6STATUS,BEARER7ID,BEARER7STATUS,BEARER8ID,BEARER8STATUS,BEARER9ID,BEARER9STATUS,BEARER10ID,BEARER10STATUS,BEARER11ID,BEARER11STATUS,BEARER12ID,BEARER12STATUS,BEARER13ID,BEARER13STATUS,BEARER14ID,BEARER14STATUS,BEARER15ID,BEARER15STATUS,RANGETIME,etype

UU=LENGTH,CITY,INTERFACE,XDRID,RAT,IMSI,IMEI,MSISDN,PROCEDURETYPE,PROCEDURESTARTTIME,PROCEDUREENDTIME,KEYWORD1,KEYWORD2,PROCEDURESTATUS,PLMNID,ENBID,CELLID,CRNTI,TARGETENBID,TARGETCELLID,TARGETCRNTI,MMEUES1APID,MMEGROUPID,MMECODE,MTMSI,CSFBINDICATION,REDIRECTEDNETWORK,EPSBEARERNUMBER,BEARER0ID,BEARER0STATUS,BEARER1ID,BEARER1STATUS,BEARER2ID,BEARER2STATUS,BEARER3ID,BEARER3STATUS,BEARER4ID,BEARER4STATUS,BEARER5ID,BEARER5STATUS,BEARER6ID,BEARER6STATUS,BEARER7ID,BEARER7STATUS,BEARER8ID,BEARER8STATUS,BEARER9ID,BEARER9STATUS,BEARER10ID,BEARER10STATUS,BEARER11ID,BEARER11STATUS,BEARER12ID,BEARER12STATUS,BEARER13ID,BEARER13STATUS,BEARER14ID,BEARER14STATUS,BEARER15ID,BEARER15STATUS,RANGETIME,etype

S1=LENGTH,CITY,INTERFACE,XDRID,RAT,IMSI,IMEI,MSISDN,PROCEDURETYPE,PROCEDURESTARTTIME,PROCEDUREENDTIME,PROCEDURESTATUS,REQUESTCAUSE,FAILURECAUSE,KEYWORD1,KEYWORD2,KEYWORD3,KEYWORD4,MMEUES1APID,OLDMMEGROUPID,OLDMMECODE,OLDMTMSI,MMEGROUPID,MMECODE,MTMSI,TMSI,USERIPV4,USERIPV6,MMEIPADD,ENBIPADD,MMEPORT,ENBPORT,TAC,CELLID,OTHERTAC,OTHERECI,APN,EPSBEARERNUMBER,BEARER0ID,BEARER0TYPE,BEARER0QCI,BEARER0STATUS,BEARER0REQUESTCAUSE,BEARER0FAILURECAUSE,BEARER0ENBGTPTEID,BEARER0SGWGTPTEID,BEARER1ID,BEARER1TYPE,BEARER1QCI,BEARER1STATUS,BEARER1REQUESTCAUSE,BEARER1FAILURECAUSE,BEARER1ENBGTPTEID,BEARER1SGWGTPTEID,BEARER2ID,BEARER2TYPE,BEARER2QCI,BEARER2STATUS,BEARER2REQUESTCAUSE,BEARER2FAILURECAUSE,BEARER2ENBGTPTEID,BEARER2SGWGTPTEID,BEARER3ID,BEARER3TYPE,BEARER3QCI,BEARER3STATUS,BEARER3REQUESTCAUSE,BEARER3FAILURECAUSE,BEARER3ENBGTPTEID,BEARER3SGWGTPTEID,BEARER4ID,BEARER4TYPE,BEARER4QCI,BEARER4STATUS,BEARER4REQUESTCAUSE,BEARER4FAILURECAUSE,BEARER4ENBGTPTEID,BEARER4SGWGTPTEID,BEARER5ID,BEARER5TYPE,BEARER5QCI,BEARER5STATUS,BEARER5REQUESTCAUSE,BEARER5FAILURECAUSE,BEARER5ENBGTPTEID,BEARER5SGWGTPTEID,BEARER6ID,BEARER6TYPE,BEARER6QCI,BEARER6STATUS,BEARER6REQUESTCAUSE,BEARER6FAILURECAUSE,BEARER6ENBGTPTEID,BEARER6SGWGTPTEID,BEARER7ID,BEARER7TYPE,BEARER7QCI,BEARER7STATUS,BEARER7REQUESTCAUSE,BEARER7FAILURECAUSE,BEARER7ENBGTPTEID,BEARER7SGWGTPTEID,BEARER8ID,BEARER8TYPE,BEARER8QCI,BEARER8STATUS,BEARER8REQUESTCAUSE,BEARER8FAILURECAUSE,BEARER8ENBGTPTEID,BEARER8SGWGTPTEID,BEARER9ID,BEARER9TYPE,BEARER9QCI,BEARER9STATUS,BEARER9REQUESTCAUSE,BEARER9FAILURECAUSE,BEARER9ENBGTPTEID,BEARER9SGWGTPTEID,BEARER10ID,BEARER10TYPE,BEARER10QCI,BEARER10STATUS,BEARER10REQUESTCAUSE,BEARER10FAILURECAUSE,BEARER10ENBGTPTEID,BEARER10SGWGTPTEID,BEARER11ID,BEARER11TYPE,BEARER11QCI,BEARER11STATUS,BEARER11REQUESTCAUSE,BEARER11FAILURECAUSE,BEARER11ENBGTPTEID,BEARER11SGWGTPTEID,BEARER12ID,BEARER12TYPE,BEARER12QCI,BEARER12STATUS,BEARER12REQUESTCAUSE,BEARER12FAILURECAUSE,BEARER12ENBGTPTEID,BEARER12SGWGTPTEID,BEARER13ID,BEARER13TYPE,BEARER13QCI,BEARER13STATUS,BEARER13REQUESTCAUSE,BEARER13FAILURECAUSE,BEARER13ENBGTPTEID,BEARER13SGWGTPTEID,BEARER14ID,BEARER14TYPE,BEARER14QCI,BEARER14STATUS,BEARER14REQUESTCAUSE,BEARER14FAILURECAUSE,BEARER14ENBGTPTEID,BEARER14SGWGTPTEID,BEARER15ID,BEARER15TYPE,BEARER15QCI,BEARER15STATUS,BEARER15REQUESTCAUSE,BEARER15FAILURECAUSE,BEARER15ENBGTPTEID,BEARER15SGWGTPTEID,RANGETIME,etype

LOC_GUSER_MARK=id,imsi,slong,slat,dlong,dlat,avgspeed,speedv0,speedv1,speedv2,timet,timet0,timet1,timet2,timetend,rangetime,speedok,flag,chkflag,eupordown,swp,remark2,remark3,remark4,remark5,switchfpdistance

VOLTE_GT_BUSI_USER_DATA=imsi,cellid,targetcellid,proceduretype,procedurestatus,rangetime,imei,msisdn,procedurestarttime,procedureendtime,enbid,targetenbid,dir_state,seqnum,ispub

VOLTE_GT_FREE_USER_DATA=imsi,cellid,proceduretype,procedurestatus,rangetime,imei,msisdn,procedurestarttime,procedureendtime,dir_state,seqnum,ispub

COMM_USER_DATA=imsi,cellid,targetcellid,proceduretype,procedurestatus,rangetime,imei,msisdn,procedurestarttime,procedureendtime,enbid,targetenbid,dir_state,seqnum,ispub

VOLTE_GT_CELL_ANA_BASE60=ttime,cellid,dir_state,voltemcsucc,voltemcatt,voltevdsucc,voltevdatt,voltetime,voltemctime,voltevdtime,voltemchandover,volteanswer,voltevdhandover,voltevdanswer,srvccsucc,srvccatt,srvcctime,lteswsucc,lteswatt,srqatt,srqsucc,tauatt,tausucc,rrcrebuild,rrcsucc,rrcreq,imsiregatt,imsiregsucc,wirelessdrop,wireless,eabdrop,eab,eabs1swx,eabs1swy,s1tox2swx,s1tox2swy,enbx2swx,enbx2swy,uuenbswx,uuenbswy,uuenbinx,uuenbiny,swx,swy,attachx,attachy,pagereq,pageresp,pageshowtimeall,pageresptimeall,pageshowsucc,httpdownflow,httpdowntime,mediareq,mediasucc,mediadownflow,mediadowntime,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness

volte_gt_cell_ana_baseday=ttime,cellid,dir_state,voltemcsucc,voltemcatt,voltevdsucc,voltevdatt,voltetime,voltemctime,voltevdtime,voltemchandover,volteanswer,voltevdhandover,voltevdanswer,srvccsucc,srvccatt,srvcctime,lteswsucc,lteswatt,srqatt,srqsucc,tauatt,tausucc,rrcrebuild,rrcsucc,rrcreq,imsiregatt,imsiregsucc,wirelessdrop,wireless,eabdrop,eab,eabs1swx,eabs1swy,s1tox2swx,s1tox2swy,enbx2swx,enbx2swy,uuenbswx,uuenbswy,uuenbinx,uuenbiny,swx,swy,attachx,attachy,pagereq,pageresp,pageshowtimeall,pageresptimeall,pageshowsucc,httpdownflow,httpdowntime,mediareq,mediasucc,mediadownflow,mediadowntime,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness

volte_gt_user_ana_base60=imsi,imei,msisdn,cellid,ttime,dir_state,voltemcsucc,voltemcatt,voltevdsucc,voltevdatt,voltetime,voltemctime,voltevdtime,voltemchandover,volteanswer,voltevdhandover,voltevdanswer,srvccsucc,srvccatt,srvcctime,lteswsucc,lteswatt,srqatt,srqsucc,tauatt,tausucc,rrcrebuild,rrcsucc,rrcreq,imsiregatt,imsiregsucc,wirelessdrop,wireless,eabdrop,eab,eabs1swx,eabs1swy,s1tox2swx,s1tox2swy,enbx2swx,enbx2swy,uuenbswx,uuenbswy,uuenbinx,uuenbiny,swx,swy,attachx,attachy,pagereq,pageresp,pageshowtimeall,pageresptimeall,pageshowsucc,httpdownflow,httpdowntime,mediareq,mediasucc,mediadownflow,mediadowntime,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness

volte_gt_user_ana_baseday=imsi,imei,msisdn,cellid,ttime,dir_state,voltemcsucc,voltemcatt,voltevdsucc,voltevdatt,voltetime,voltemctime,voltevdtime,voltemchandover,volteanswer,voltevdhandover,voltevdanswer,srvccsucc,srvccatt,srvcctime,lteswsucc,lteswatt,srqatt,srqsucc,tauatt,tausucc,rrcrebuild,rrcsucc,rrcreq,imsiregatt,imsiregsucc,wirelessdrop,wireless,eabdrop,eab,eabs1swx,eabs1swy,s1tox2swx,s1tox2swy,enbx2swx,enbx2swy,uuenbswx,uuenbswy,uuenbinx,uuenbiny,swx,swy,attachx,attachy,pagereq,pageresp,pageshowtimeall,pageresptimeall,pageshowsucc,httpdownflow,httpdowntime,mediareq,mediasucc,mediadownflow,mediadowntime,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness

mr_gt_cell_ana_base60=cellid,ttime,dir_state,avgrsrpx,commy,avgrsrqx,ltecoverratex,weakcoverratex,overlapcoverratex,overlapcoverratey,upsigrateavgx,upsigrateavgy,updiststrox,updiststroy,model3diststrox,model3diststroy,uebootx,uebooty

mr_gt_cell_ana_baseday=cellid,ttime,dir_state,avgrsrpx,commy,avgrsrqx,ltecoverratex,weakcoverratex,overlapcoverratex,overlapcoverratey,upsigrateavgx,upsigrateavgy,updiststrox,updiststroy,model3diststrox,model3diststroy,uebootx,uebooty

mr_gt_user_ana_base60=imsi,imei,msisdn,cellid,rruid,gridid,ttime,dir_state,elong,elat,avgrsrpx,commy,avgrsrqx,ltecoverratex,weakcoverratex,overlapcoverratex,overlapcoverratey,upsigrateavgx,upsigrateavgy,updiststrox,updiststroy,model3diststrox,model3diststroy,uebootx,uebooty

mr_gt_user_ana_baseday=imsi,imei,msisdn,cellid,rruid,gridid,ttime,dir_state,elong,elat,avgrsrpx,commy,avgrsrqx,ltecoverratex,weakcoverratex,overlapcoverratex,overlapcoverratey,upsigrateavgx,upsigrateavgy,updiststrox,updiststroy,model3diststrox,model3diststroy,uebootx,uebooty

mr_gt_grid_ana_base60=gridid,ttime,dir_state,cellid,rruid,avgrsrpx,commy,avgrsrqx,ltecoverratex,weakcoverratex,overlapcoverratex,overlapcoverratey,upsigrateavgx,upsigrateavgy,updiststrox,updiststroy,model3diststrox,model3diststroy,uebootx,uebooty

mr_gt_grid_ana_baseday=gridid,ttime,dir_state,cellid,rruid,avgrsrpx,commy,avgrsrqx,ltecoverratex,weakcoverratex,overlapcoverratex,overlapcoverratey,upsigrateavgx,upsigrateavgy,updiststrox,updiststroy,model3diststrox,model3diststroy,uebootx,uebooty

mw_new=length,city,interface,xdrid,rat,imsi,imei,msisdn,proceduretype,procedurestarttime,procedureendtime,servicetype,procedurestatus,callingnumber,callednumber,callingpartyuri,requesturi,userip,callid,icid,sourceneip,sourceneport,destneip,destneport,callside,sourceaccesstype,sourceeci,sourcetac,sourceimsi,sourceimei,destaccesstype,desteci,desttac,destimsi,destimei,authtype,expirestimereq,expirestimersp,callingsdpipaddr,callingaudiosdpport,callingvideosdpport,calledsdpipaddr,calledaudiosdpport,calledvideoport,audiocodec,videocodec,redirectingpartyaddress,originalpartyaddress,redirectreason,responsecode,finishwarningcode,finishreasonprotocol,finishreasoncause,firfailtime,firstfailneip,alertingtime,answertime,releasetime,callduration,authreqtime,authrsptime,stnsr,atcfmgmt,atusti,cmsisdn,ssi,rangetime,etype,gridid,slong,slat,dlong,dlat,distance,espeed,elong,elat,falurecause,flag,beforeflag,eupordown

s1mme_new=length,city,interface,xdrid,rat,imsi,imei,msisdn,proceduretype,procedurestarttime,procedureendtime,procedurestatus,requestcause,failurecause,keyword1,keyword2,keyword3,keyword4,mmeues1apid,oldmmegroupid,oldmmecode,oldmtmsi,mmegroupid,mmecode,mtmsi,tmsi,useripv4,useripv6,mmeipadd,enbipadd,mmeport,enbport,tac,cellid,othertac,othereci,apn,EPSBEARERNUMBER,BEARER0ID,BEARER0TYPE,BEARER0QCI,BEARER0STATUS,BEARER0REQUESTCAUSE,BEARER0FAILURECAUSE,BEARER0ENBGTPTEID,BEARER0SGWGTPTEID,BEARER1ID,BEARER1TYPE,BEARER1QCI,BEARER1STATUS,BEARER1REQUESTCAUSE,BEARER1FAILURECAUSE,BEARER1ENBGTPTEID,BEARER1SGWGTPTEID,BEARER2ID,BEARER2TYPE,BEARER2QCI,BEARER2STATUS,BEARER2REQUESTCAUSE,BEARER2FAILURECAUSE,BEARER2ENBGTPTEID,BEARER2SGWGTPTEID,BEARER3ID,BEARER3TYPE,BEARER3QCI,BEARER3STATUS,BEARER3REQUESTCAUSE,BEARER3FAILURECAUSE,BEARER3ENBGTPTEID,BEARER3SGWGTPTEID,BEARER4ID,BEARER4TYPE,BEARER4QCI,BEARER4STATUS,BEARER4REQUESTCAUSE,BEARER4FAILURECAUSE,BEARER4ENBGTPTEID,BEARER4SGWGTPTEID,BEARER5ID,BEARER5TYPE,BEARER5QCI,BEARER5STATUS,BEARER5REQUESTCAUSE,BEARER5FAILURECAUSE,BEARER5ENBGTPTEID,BEARER5SGWGTPTEID,rangetime,etype,gridid,slong,slat,dlong,dlat,distance,espeed,elong,elat,falurecause,flag,beforeflag,eupordown,echk4gtype,echk4g23type

uu_new=length,city,interface,xdrid,rat,imsi,imei,msisdn,proceduretype,procedurestarttime,procedureendtime,keyword1,keyword2,procedurestatus,plmnid,enbid,cellid,crnti,targetenbid,targetcellid,targetcrnti,mmeues1apid,mmegroupid,mmecode,mtmsi,csfbindication,EPSBEARERNUMBER,BEARER0ID,BEARER0STATUS,BEARER1ID,BEARER1STATUS,BEARER2ID,BEARER2STATUS,BEARER3ID,BEARER3STATUS,BEARER4ID,BEARER4STATUS,BEARER5ID,BEARER5STATUS,rangetime,etype,gridid,slong,slat,dlong,dlat,distance,espeed,elong,elat,falurecause,flag,beforeflag,eupordown

x2_new=length,city,interface,xdrid,rat,imsi,imei,msisdn,proceduretype,procedurestarttime,procedureendtime,procedurestatus,cellid,targetcellid,enbid,targetenbid,mmeues1apid,mmegroupid,mmecode,requestcause,failurecause,EPSBEARERNUMBER,BEARER0ID,BEARER0STATUS,BEARER1ID,BEARER1STATUS,BEARER2ID,BEARER2STATUS,BEARER3ID,BEARER3STATUS,BEARER4ID,BEARER4STATUS,BEARER5ID,BEARER5STATUS,rangetime,etype,gridid,slong,slat,dlong,dlat,distance,espeed,elong,elat,falurecause,flag,beforeflag,eupordown

gxrx_new=length,city,interface,xdrid,rat,imsi,imei,msisdn,proceduretype,procedurestarttime,procedureendtime,icid,originrealm,destinationrealm,originhost,destinationhost,sgsnsgwsigip,afappid,ccrequesttype,rxrequesttype,mediatype,abortcause,resultcode,experimentalresultcode,sessionreleasecause,rangetime,etype,gridid,slong,slat,dlong,dlat,distance,espeed,elong,elat,falurecause,flag,beforeflag,eupordown

sgs_new=length,city,interface,xdrid,rat,imsi,imei,msisdn,proceduretype,procedurestarttime,procedureendtime,procedurestatus,sgscause,rejectcause,cpcause,rpcause,useripv4,useripv6,mmeipadd,mscserveripadd,mmeport,mscserverport,serviceindicator,mmename,tmsi,newlac,oldlac,tac,cellid,callingid,vlrnamelength,vlrname,rangetime,etype,gridid,slong,slat,dlong,dlat,distance,espeed,elong,elat,falurecause,flag,beforeflag,eupordown

sv_new=length,city,interface,xdrid,rat,imsi,imei,msisdn,proceduretype,procedurestarttime,procedureendtime,sourceneip,sourceneport,destneip,destneport,roamdirection,homemcc,homemnc,mcc,mnc,targetlac,sourcetac,sourceeci,svflags,ulcmscip,dlcmmeip,ulcmscteid,dlcmmeteid,stnsr,targetrncid,targetcellid,arp,requestresult,result,svcause,postfailurecause,respdelay,svdelay,rangetime,etype,gridid,slong,slat,dlong,dlat,distance,espeed,elong,elat,falurecause,flag,beforeflag,eupordown,echksvtype

mr_new=objectid,vid,fileformatversion,starttime,endtime,period,enbid,userlabel,mrname,cellid,earfcn,subframenbr,prbnbr,mmeues1apid,mmegroupid,mmecode,meatime,eventtype,gridcenterlongitude,gridcenterlatitude,kpi1,kpi2,kpi3,kpi4,kpi5,kpi6,kpi7,kpi8,kpi9,kpi10,kpi11,kpi12,kpi13,kpi14,kpi15,kpi16,kpi17,kpi18,kpi19,kpi20,kpi21,kpi22,kpi23,kpi24,kpi25,kpi26,kpi27,kpi28,kpi29,kpi30,kpi31,kpi32,kpi33,kpi34,kpi35,kpi36,kpi37,kpi38,kpi39,kpi40,kpi41,kpi42,kpi43,kpi44,kpi45,kpi46,kpi47,kpi48,kpi49,kpi50,kpi51,kpi52,kpi53,kpi54,kpi55,kpi56,kpi57,kpi58,kpi59,kpi60,kpi61,kpi62,kpi63,kpi64,kpi65,kpi66,kpi67,kpi68,kpi69,kpi70,kpi71,length,city,xdrtype,interface,xdrid,rat,imsi,imei,msisdn,mrtype,neighborcellnumber,gsmneighborcellnumber,tdsneighborcellnumber,v_enb,mrtime,etype,gridid,slong,slat,dlong,dlat,distance,espeed,elong,elat,falurecause,flag,beforeflag,eupordown

u1=imsi,xdrid,cellid,targetcellid,upordown,procedurestarttime,rangetime,seqnum,groupname,hour

volte_gtuser_data=imsi,cellid,ttime

UpOrDown=imsi,xdrid,upordown,groupmap,intime,in_time,instation,outtime,out_time,outstation

user_home=imsi,pnumber,home

VOLTE_CELLMR=cellid,meatime,xdrid,rip

numberiden=imsi,xdrid,cellid,upordown,groupname,intime,outtime,instation,outstation,sorttime

if [ $COL_NUM = 1 ] 
then
   COLS=$KPI_IMSI_COLS
elif [ $COL_NUM = 2 ]; then
   COLS=$KPI_CELL_COLS
elif [ $COL_NUM = 3 ]; then 
   COLS=$EVENT_MSG
elif [ $COL_NUM = 4 ]; then 
   COLS=$MR_CELL_COLS
elif [ $COL_NUM = 5 ]; then
   COLS=$X2
elif [ $COL_NUM = 6 ]; then
   COLS=$UU
elif [ $COL_NUM = 7 ]; then
   COLS=$S1
elif [ $COL_NUM = 8 ]; then
   COLS=$LOC_GUSER_MARK
elif [ $COL_NUM = 9 ]; then
   COLS=$VOLTE_GT_BUSI_USER_DATA
elif [ $COL_NUM = 10 ]; then
   COLS=$COMM_USER_DATA
elif [ $COL_NUM = 11 ]; then
   COLS=$VOLTE_GT_CELL_ANA_BASE60
elif [ $COL_NUM = 12 ]; then
   COLS=$volte_gt_cell_ana_baseday
elif [ $COL_NUM = 13 ]; then
   COLS=$volte_gt_user_ana_base60
elif [ $COL_NUM = 14 ]; then
   COLS=$volte_gt_user_ana_baseday
elif [ $COL_NUM = 15 ]; then
   COLS=$mr_gt_cell_ana_base60   
elif [ $COL_NUM = 16 ]; then
   COLS=$mr_gt_cell_ana_baseday
elif [ $COL_NUM = 17 ]; then
   COLS=$mr_gt_user_ana_base60
elif [ $COL_NUM = 18 ]; then
   COLS=$mr_gt_user_ana_baseday
elif [ $COL_NUM = 19 ]; then
   COLS=$mw_new
elif [ $COL_NUM = 20 ]; then
   COLS=$s1mme_new
elif [ $COL_NUM = 21 ]; then
   COLS=$uu_new
elif [ $COL_NUM = 22 ]; then
   COLS=$x2_new
elif [ $COL_NUM = 23 ]; then
   COLS=$gxrx_new
elif [ $COL_NUM = 24 ]; then
   COLS=$sgs_new
elif [ $COL_NUM = 25 ]; then
   COLS=$sv_new
elif [ $COL_NUM = 26 ]; then
   COLS=$mr_new
elif [ $COL_NUM = 27 ]; then
   COLS=$u1
elif [ $COL_NUM = 28 ]; then
   COLS=$volte_gtuser_data
elif [ $COL_NUM = 29 ]; then
   COLS=$UpOrDown
elif [ $COL_NUM = 30 ]; then
   COLS=$VOLTE_GT_FREE_USER_DATA
elif [ $COL_NUM = 31 ]; then
   COLS=$user_home
elif [ $COL_NUM = 32 ]; then
   COLS=$VOLTE_CELLMR
elif [ $COL_NUM = 33 ]; then
   COLS=$mr_gt_grid_ana_base60
elif [ $COL_NUM = 34 ]; then
   COLS=$mr_gt_grid_ana_baseday
elif [ $COL_NUM = 35 ]; then
   COLS=$numberiden
else 
   COLS=$MR_IMSI_COLS
fi
/opt/app/sqoop/bin/sqoop export --connect $URL \
--username $USERNAME --password tiger \
--table $TABLE \
--columns $COLS \
--export-dir $HDFS_DIR \
--input-fields-terminated-by ',' \
--m $MAP_NUM \
--input-null-string '\\N' --input-null-non-string '\\N' 

