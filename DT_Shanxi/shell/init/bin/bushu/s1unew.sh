#!/bin/sh
ANALY_DATE=$1
ANALY_HOUR=$2
#用户名原始表数据库
DB=$3
#高铁用户信息表数据库
DEFAULT=$4
hive<<EOF
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;

USE ${DB};

alter table tb_xdr_ifc_s1u_common_new drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_common_new add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
insert into tb_xdr_ifc_s1u_common_new partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
select
t.length
,t.city
,t.interface
,t.xdrid
,t.rat
,t.imsi
,t.imei
,t.msisdn
,t.machineipaddtype
,t.sgwipaddr
,t.enbipaddr
,t.sgwport
,t.enbport
,t.enbgtpteid
,t.sgwgtpteid
,t.tac
,t.ecgi
,t.apn
,t.apptypecode
,t.procedurestarttime
,t.procedureendtime
,t.protocoltype
,t.apptype
,t.appsubtype
,t.appcontent
,t.appstatus
,t.useripv4
,t.useripv6
,t.userport
,t.l4protocal
,t.appserveripipv4
,t.appserveripipv6
,t.appserverport
,t.uldata
,t.dldata
,t.ulippacket
,t.dlippacket
,t.ultcppacketor
,t.dltcppacketor
,t.ultcppacketre
,t.dltcppacketre
,t.tcpestabrede
,t.tcpestabdeconf
,t.ulipfragpackets
,t.dlipfragpackets
,t.tcpfirstrede
,t.tcpfirstconf
,t.winsize
,t.msssize
,t.tcpattnum
,t.tcplinkstatus
,t.sessionflag
,g.dir_state from ${DEFAULT}.tb_xdr_ifc_s1u_common t
inner join volte_gt_busi_user_data g on t.imsi=g.imsi WHERE t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR" and g.dt="$ANALY_DATE" and g.h="$ANALY_HOUR" ;

alter table tb_xdr_ifc_s1u_HTTP_NEW drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_HTTP_NEW add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
insert into tb_xdr_ifc_s1u_HTTP_NEW partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
select
t.length
,t.city
,t.interface
,t.xdrid
,t.rat
,t.imsi
,t.imei
,t.msisdn
,t.machineipaddtype
,t.sgwipaddr
,t.enbipaddr
,t.sgwport
,t.enbport
,t.enbgtpteid
,t.sgwgtpteid
,t.tac
,t.ecgi
,t.apn
,t.apptypecode
,t.procedurestarttime
,t.procedureendtime
,t.protocoltype
,t.apptype
,t.appsubtype
,t.appcontent
,t.appstatus
,t.useripv4
,t.useripv6
,t.userport
,t.l4protocal
,t.appserveripipv4
,t.appserveripipv6
,t.appserverport
,t.uldata
,t.dldata
,t.ulippacket
,t.dlippacket
,t.ultcppacketor
,t.dltcppacketor
,t.ultcppacketre
,t.dltcppacketre
,t.tcpestabrede
,t.tcpestabdeconf
,t.ulipfragpackets
,t.dlipfragpackets
,t.tcpfirstrede
,t.tcpfirstconf
,t.winsize
,t.msssize
,t.tcpattnum
,t.tcplinkstatus
,t.sessionflag
,t.httpversion
,t.transactiontype
,t.httpstate
,t.httpfirstrede
,t.httplastrede
,t.acklastconf
,t.host
,t.uri
,t.xonlinehost
,t.useragent
,t.httpcontenttype
,t.referuri
,t.cookie
,t.contentlength
,t.targetbehavior
,t.wtpinterrupttype
,t.wtpinterruptcause
,t.title
,t.keyword
,t.busconductlogo
,t.buscompletionflag
,t.busrede
,t.browsingtool
,t.portalapp
,g.dir_state from ${DEFAULT}.tb_xdr_ifc_s1u_http t inner join volte_gt_busi_user_data g on t.imsi=g.imsi WHERE t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR" and g.dt="$ANALY_DATE" and g.h="$ANALY_HOUR" ;

alter table tb_xdr_ifc_s1u_DNS_NEW drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_DNS_NEW add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
insert into tb_xdr_ifc_s1u_DNS_NEW partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
select
t.LENGTH
,t.CITY
,t.INTERFACE
,t.XDRID
,t.RAT
,t.IMSI
,t.IMEI
,t.MSISDN
,t.MACHINEIPADDTYPE
,t.SGWIPADDR
,t.ENBIPADDR
,t.SGWPORT
,t.ENBPORT
,t.ENBGTPTEID
,t.SGWGTPTEID
,t.TAC
,t.ecgi
,t.APN
,t.APPTYPECODE
,t.PROCEDURESTARTTIME
,t.PROCEDUREENDTIME
,t.PROTOCOLTYPE
,t.APPTYPE
,t.APPSUBTYPE
,t.APPCONTENT
,t.APPSTATUS
,t.USERIPV4
,t.USERIPV6
,t.USERPORT
,t.L4PROTOCAL
,t.APPSERVERIPIPV4
,t.APPSERVERIPIPV6
,t.APPSERVERPORT
,t.ULDATA
,t.DLDATA
,t.ULIPPACKET
,t.DLIPPACKET
,t.ULTCPPACKETOR
,t.DLTCPPACKETOR
,t.ULTCPPACKETRE
,t.DLTCPPACKETRE
,t.TCPESTABREDE
,t.TCPESTABDECONF
,t.ULIPFRAGPACKETS
,t.DLIPFRAGPACKETS
,t.TCPFIRSTREDE
,t.TCPFIRSTCONF
,t.WINSIZE
,t.MSSSIZE
,t.TCPATTNUM
,t.TCPLINKSTATUS
,t.SESSIONFLAG
,t.REQDNS
,t.RESULTIP
,t.DNSRECODE
,t.DNSATTNUM
,t.DNSRENUM
,t.LICENSEDCONNUM
,t.ADDITIONALCONNUM
,g.dir_state from ${DEFAULT}.tb_xdr_ifc_s1u_dns t inner join volte_gt_busi_user_data g on t.imsi=g.imsi WHERE t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR" and g.dt="$ANALY_DATE" and g.h="$ANALY_HOUR" ;

alter table tb_xdr_ifc_s1u_MMS_NEW drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_MMS_NEW add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
insert into tb_xdr_ifc_s1u_MMS_NEW partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
select
t.LENGTH
,t.CITY
,t.INTERFACE
,t.XDRID
,t.RAT
,t.IMSI
,t.IMEI
,t.MSISDN
,t.MACHINEIPADDTYPE
,t.SGWIPADDR
,t.ENBIPADDR
,t.SGWPORT
,t.ENBPORT
,t.ENBGTPTEID
,t.SGWGTPTEID
,t.TAC
,t.ecgi
,t.APN
,t.APPTYPECODE
,t.PROCEDURESTARTTIME
,t.PROCEDUREENDTIME
,t.PROTOCOLTYPE
,t.APPTYPE
,t.APPSUBTYPE
,t.APPCONTENT
,t.APPSTATUS
,t.USERIPV4
,t.USERIPV6
,t.USERPORT
,t.L4PROTOCAL
,t.APPSERVERIPIPV4
,t.APPSERVERIPIPV6
,t.APPSERVERPORT
,t.ULDATA
,t.DLDATA
,t.ULIPPACKET
,t.DLIPPACKET
,t.ULTCPPACKETOR
,t.DLTCPPACKETOR
,t.ULTCPPACKETRE
,t.DLTCPPACKETRE
,t.TCPESTABREDE
,t.TCPESTABDECONF
,t.ULIPFRAGPACKETS
,t.DLIPFRAGPACKETS
,t.TCPFIRSTREDE
,t.TCPFIRSTCONF
,t.WINSIZE
,t.MSSSIZE
,t.TCPATTNUM
,t.TCPLINKSTATUS
,t.SESSIONFLAG
,t.TRANSTYPE
,t.SUCCESSFLAG
,t.HTTPORWAP1X
,t.HTTPWAPCODE
,t.MMSERSPSTATUS
,t.MMSSENDADDR
,t.MMSMSGID
,t.MMSTRANSACTIONID
,t.MMSRETRIVEADDR
,t.MMSRETRIVEADDRNUM
,t.MMSCCBCCADDR
,t.MMSCCBCCADDRNUM
,t.MMSSUBJECT
,t.MMSDATASIZE
,t.MMSCIPADDR
,t.HOST
,t.URI
,t.XONLINEHOST
,g.dir_state from ${DEFAULT}.tb_xdr_ifc_s1u_mms t inner join volte_gt_busi_user_data g on t.imsi=g.imsi WHERE t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR" and g.dt="$ANALY_DATE" and g.h="$ANALY_HOUR" ;

alter table tb_xdr_ifc_s1u_FTP_NEW drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_FTP_NEW add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
insert into tb_xdr_ifc_s1u_FTP_NEW partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
select
t.LENGTH
,t.CITY
,t.INTERFACE
,t.XDRID
,t.RAT
,t.IMSI
,t.IMEI
,t.MSISDN
,t.MACHINEIPADDTYPE
,t.SGWIPADDR
,t.ENBIPADDR
,t.SGWPORT
,t.ENBPORT
,t.ENBGTPTEID
,t.SGWGTPTEID
,t.TAC
,t.ecgi
,t.APN
,t.APPTYPECODE
,t.PROCEDURESTARTTIME
,t.PROCEDUREENDTIME
,t.PROTOCOLTYPE
,t.APPTYPE
,t.APPSUBTYPE
,t.APPCONTENT
,t.APPSTATUS
,t.USERIPV4
,t.USERIPV6
,t.USERPORT
,t.L4PROTOCAL
,t.APPSERVERIPIPV4
,t.APPSERVERIPIPV6
,t.APPSERVERPORT
,t.ULDATA
,t.DLDATA
,t.ULIPPACKET
,t.DLIPPACKET
,t.ULTCPPACKETOR
,t.DLTCPPACKETOR
,t.ULTCPPACKETRE
,t.DLTCPPACKETRE
,t.TCPESTABREDE
,t.TCPESTABDECONF
,t.ULIPFRAGPACKETS
,t.DLIPFRAGPACKETS
,t.TCPFIRSTREDE
,t.TCPFIRSTCONF
,t.WINSIZE
,t.MSSSIZE
,t.TCPATTNUM
,t.TCPLINKSTATUS
,t.SESSIONFLAG
,t.FTPSTATUS
,t.LOGINUSER
,t.CURRENTDIRECTORY
,t.FILETRANSFERMODE
,t.TRANSDIRECTFLAG
,t.FILENAME
,t.FTPLOCDATAPORT
,t.FTPREMDATAPORT
,t.TOTALFILESIZE
,t.FTPREDE
,t.TRANSDURA
,g.dir_state from ${DEFAULT}.tb_xdr_ifc_s1u_ftp t inner join volte_gt_busi_user_data g on t.imsi=g.imsi WHERE t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR" and g.dt="$ANALY_DATE" and g.h="$ANALY_HOUR" ;


alter table tb_xdr_ifc_s1u_EMAIL_NEW drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_EMAIL_NEW add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
insert into tb_xdr_ifc_s1u_EMAIL_NEW partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
select
t.LENGTH
,t.CITY
,t.INTERFACE
,t.XDRID
,t.RAT
,t.IMSI
,t.IMEI
,t.MSISDN
,t.MACHINEIPADDTYPE
,t.SGWIPADDR
,t.ENBIPADDR
,t.SGWPORT
,t.ENBPORT
,t.ENBGTPTEID
,t.SGWGTPTEID
,t.TAC
,t.ecgi
,t.APN
,t.APPTYPECODE
,t.PROCEDURESTARTTIME
,t.PROCEDUREENDTIME
,t.PROTOCOLTYPE
,t.APPTYPE
,t.APPSUBTYPE
,t.APPCONTENT
,t.APPSTATUS
,t.USERIPV4
,t.USERIPV6
,t.USERPORT
,t.L4PROTOCAL
,t.APPSERVERIPIPV4
,t.APPSERVERIPIPV6
,t.APPSERVERPORT
,t.ULDATA
,t.DLDATA
,t.ULIPPACKET
,t.DLIPPACKET
,t.ULTCPPACKETOR
,t.DLTCPPACKETOR
,t.ULTCPPACKETRE
,t.DLTCPPACKETRE
,t.TCPESTABREDE
,t.TCPESTABDECONF
,t.ULIPFRAGPACKETS
,t.DLIPFRAGPACKETS
,t.TCPFIRSTREDE
,t.TCPFIRSTCONF
,t.WINSIZE
,t.MSSSIZE
,t.TCPATTNUM
,t.TCPLINKSTATUS
,t.SESSIONFLAG
,t.EMAILTRANSTYPE
,t.RESTATUSCODE
,t.USERNAME
,t.SENDER
,t.EMAIL
,t.SMTPADDR
,t.RECIPACC
,t.MESSHEADINFOR
,t.ACCESSTYPE
,g.dir_state from ${DEFAULT}.tb_xdr_ifc_s1u_email t inner join volte_gt_busi_user_data g on t.imsi=g.imsi WHERE t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR" and g.dt="$ANALY_DATE" and g.h="$ANALY_HOUR" ;


alter table tb_xdr_ifc_s1u_VOIP_NEW drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_VOIP_NEW add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
insert into tb_xdr_ifc_s1u_VOIP_NEW partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
select
t.LENGTH
,t.CITY
,t.INTERFACE
,t.XDRID
,t.RAT
,t.IMSI
,t.IMEI
,t.MSISDN
,t.MACHINEIPADDTYPE
,t.SGWIPADDR
,t.ENBIPADDR
,t.SGWPORT
,t.ENBPORT
,t.ENBGTPTEID
,t.SGWGTPTEID
,t.TAC
,t.ecgi
,t.APN
,t.APPTYPECODE
,t.PROCEDURESTARTTIME
,t.PROCEDUREENDTIME
,t.PROTOCOLTYPE
,t.APPTYPE
,t.APPSUBTYPE
,t.APPCONTENT
,t.APPSTATUS
,t.USERIPV4
,t.USERIPV6
,t.USERPORT
,t.L4PROTOCAL
,t.APPSERVERIPIPV4
,t.APPSERVERIPIPV6
,t.APPSERVERPORT
,t.ULDATA
,t.DLDATA
,t.ULIPPACKET
,t.DLIPPACKET
,t.ULTCPPACKETOR
,t.DLTCPPACKETOR
,t.ULTCPPACKETRE
,t.DLTCPPACKETRE
,t.TCPESTABREDE
,t.TCPESTABDECONF
,t.ULIPFRAGPACKETS
,t.DLIPFRAGPACKETS
,t.TCPFIRSTREDE
,t.TCPFIRSTCONF
,t.WINSIZE
,t.MSSSIZE
,t.TCPATTNUM
,t.TCPLINKSTATUS
,t.SESSIONFLAG
,t.CALLDIRECTION
,t.CALLERNUM
,t.CALLEDNUM
,t.CALLTYPE
,t.VOIPSTREAMSNUM
,t.REASONHOOK
,t.SIGNPROTYPE
,g.dir_state from ${DEFAULT}.tb_xdr_ifc_s1u_voip t inner join volte_gt_busi_user_data g on t.imsi=g.imsi WHERE t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR" and g.dt="$ANALY_DATE" and g.h="$ANALY_HOUR" ;

alter table tb_xdr_ifc_s1u_RTSP_NEW drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_RTSP_NEW add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
insert into tb_xdr_ifc_s1u_RTSP_NEW partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
select
t.LENGTH
,t.CITY
,t.INTERFACE
,t.XDRID
,t.RAT
,t.IMSI
,t.IMEI
,t.MSISDN
,t.MACHINEIPADDTYPE
,t.SGWIPADDR
,t.ENBIPADDR
,t.SGWPORT
,t.ENBPORT
,t.ENBGTPTEID
,t.SGWGTPTEID
,t.TAC
,t.ecgi
,t.APN
,t.APPTYPECODE
,t.PROCEDURESTARTTIME
,t.PROCEDUREENDTIME
,t.PROTOCOLTYPE
,t.APPTYPE
,t.APPSUBTYPE
,t.APPCONTENT
,t.APPSTATUS
,t.USERIPV4
,t.USERIPV6
,t.USERPORT
,t.L4PROTOCAL
,t.APPSERVERIPIPV4
,t.APPSERVERIPIPV6
,t.APPSERVERPORT
,t.ULDATA
,t.DLDATA
,t.ULIPPACKET
,t.DLIPPACKET
,t.ULTCPPACKETOR
,t.DLTCPPACKETOR
,t.ULTCPPACKETRE
,t.DLTCPPACKETRE
,t.TCPESTABREDE
,t.TCPESTABDECONF
,t.ULIPFRAGPACKETS
,t.DLIPFRAGPACKETS
,t.TCPFIRSTREDE
,t.TCPFIRSTCONF
,t.WINSIZE
,t.MSSSIZE
,t.TCPATTNUM
,t.TCPLINKSTATUS
,t.SESSIONFLAG
,t.RTSPURL
,t.USERAGENT
,t.RTPIP
,t.RTPSTARTCLPORT
,t.RTPENDCLPORT
,t.RTPSTARTSERPORT
,t.RTPENDSERPORT
,t.RTSPVIDSTREAM
,t.RTSPAUDSTREAM
,t.RTSPREDE
,g.dir_state from ${DEFAULT}.tb_xdr_ifc_s1u_rtsp t inner join volte_gt_busi_user_data g on t.imsi=g.imsi WHERE t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR" and g.dt="$ANALY_DATE" and g.h="$ANALY_HOUR" ;

alter table tb_xdr_ifc_s1u_RTCOMM_NEW drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_RTCOMM_NEW add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
insert into tb_xdr_ifc_s1u_RTCOMM_NEW partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
select
,t.LENGTH
,t.CITY
,t.INTERFACE
,t.XDRID
,t.RAT
,t.IMSI
,t.IMEI
,t.MSISDN
,t.MACHINEIPADDTYPE
,t.SGWIPADDR
,t.ENBIPADDR
,t.SGWPORT
,t.ENBPORT
,t.ENBGTPTEID
,t.SGWGTPTEID
,t.TAC
,t.ecgi
,t.APN
,t.APPTYPECODE
,t.PROCEDURESTARTTIME
,t.PROCEDUREENDTIME
,t.PROTOCOLTYPE
,t.APPTYPE
,t.APPSUBTYPE
,t.APPCONTENT
,t.APPSTATUS
,t.USERIPV4
,t.USERIPV6
,t.USERPORT
,t.L4PROTOCAL
,t.APPSERVERIPIPV4
,t.APPSERVERIPIPV6
,t.APPSERVERPORT
,t.ULDATA
,t.DLDATA
,t.ULIPPACKET
,t.DLIPPACKET
,t.ULTCPPACKETOR
,t.DLTCPPACKETOR
,t.ULTCPPACKETRE
,t.DLTCPPACKETRE
,t.TCPESTABREDE
,t.TCPESTABDECONF
,t.ULIPFRAGPACKETS
,t.DLIPFRAGPACKETS
,t.TCPFIRSTREDE
,t.TCPFIRSTCONF
,t.WINSIZE
,t.MSSSIZE
,t.TCPATTNUM
,t.TCPLINKSTATUS
,t.SESSIONFLAG
,t.RTCOMM
,t.APPVERSION
,t.CLVERSION
,t.OPERTYPE
,g.dir_state from ${DEFAULT}.tb_xdr_ifc_s1u_rtcomm t WHERE t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR"
inner join volte_gt_busi_user_data g on t.imsi=g.imsi WHERE t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR" and g.dt="$ANALY_DATE" and g.h="$ANALY_HOUR" ;


alter table tb_xdr_ifc_s1u_P2P_NEW drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_P2P_NEW add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
insert into tb_xdr_ifc_s1u_P2P_NEW partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
select
t.LENGTH
,t.CITY
,t.INTERFACE
,t.XDRID
,t.RAT
,t.IMSI
,t.IMEI
,t.MSISDN
,t.MACHINEIPADDTYPE
,t.SGWIPADDR
,t.ENBIPADDR
,t.SGWPORT
,t.ENBPORT
,t.ENBGTPTEID
,t.SGWGTPTEID
,t.TAC
,t.ecgi
,t.APN
,t.APPTYPECODE
,t.PROCEDURESTARTTIME
,t.PROCEDUREENDTIME
,t.PROTOCOLTYPE
,t.APPTYPE
,t.APPSUBTYPE
,t.APPCONTENT
,t.APPSTATUS
,t.USERIPV4
,t.USERIPV6
,t.USERPORT
,t.L4PROTOCAL
,t.APPSERVERIPIPV4
,t.APPSERVERIPIPV6
,t.APPSERVERPORT
,t.ULDATA
,t.DLDATA
,t.ULIPPACKET
,t.DLIPPACKET
,t.ULTCPPACKETOR
,t.DLTCPPACKETOR
,t.ULTCPPACKETRE
,t.DLTCPPACKETRE
,t.TCPESTABREDE
,t.TCPESTABDECONF
,t.ULIPFRAGPACKETS
,t.DLIPFRAGPACKETS
,t.TCPFIRSTREDE
,t.TCPFIRSTCONF
,t.WINSIZE
,t.MSSSIZE
,t.TCPATTNUM
,t.TCPLINKSTATUS
,t.SESSIONFLAG
,t.FILESIZE
,t.P2PSIGN
,t.TRACKER
,g.dir_state from ${DEFAULT}.tb_xdr_ifc_s1u_p2p t inner join volte_gt_busi_user_data g on t.imsi=g.imsi WHERE t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR" and g.dt="$ANALY_DATE" and g.h="$ANALY_HOUR" ;


EOF