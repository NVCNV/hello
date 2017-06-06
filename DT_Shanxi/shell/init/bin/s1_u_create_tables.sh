#!/bin/bash
#明细数据库
DDLDB=$1
DB_PATH=$2
#缓存数据库
DCLDB=$3
export NLS_LANG="AMERICAN_AMERICA.UTF8"
#HQL
hive<<EOF
CREATE DATABASE IF NOT EXISTS ${DDLDB};
USE ${DDLDB};

DROP TABLE IF EXISTS  tb_xdr_ifc_s1u_common;
create EXTERNAL table tb_xdr_ifc_s1u_common
(
 LENGTH              BIGINT,          
  CITY               BIGINT,        
  INTERFACE          BIGINT,        
  XDRID              STRING,     
  RAT                BIGINT,        
  IMSI               STRING,     
  IMEI               STRING,   
  MSISDN             STRING,     
  MACHINEIPADDTYPE   BIGINT,        
  SGWIPADDR          STRING,     
  ENBIPADDR          STRING,     
  SGWPORT            BIGINT,        
  ENBPORT            BIGINT,        
  ENBGTPTEID         BIGINT,     
  SGWGTPTEID         BIGINT,     
  TAC                BIGINT,     
  ecgi               BIGINT,     
  APN                STRING,     
  APPTYPECODE        BIGINT,     
  PROCEDURESTARTTIME BIGINT,     
  PROCEDUREENDTIME   BIGINT,     
  PROTOCOLTYPE       BIGINT,     
  APPTYPE            BIGINT,     
  APPSUBTYPE         BIGINT,     
  APPCONTENT         BIGINT,        
  APPSTATUS          BIGINT,        
  USERIPV4           STRING,     
  USERIPV6           STRING,     
  USERPORT           STRING,     
  L4PROTOCAL         BIGINT,        
  APPSERVERIPIPV4    STRING,     
  APPSERVERIPIPV6    STRING,     
  APPSERVERPORT      STRING,     
  ULDATA             BIGINT,     
  DLDATA             BIGINT,     
  ULIPPACKET         BIGINT,     
  DLIPPACKET         BIGINT,     
  ULTCPPACKETOR      BIGINT,     
  DLTCPPACKETOR      BIGINT,     
  ULTCPPACKETRE      BIGINT,     
  DLTCPPACKETRE      BIGINT,     
  TCPESTABREDE       BIGINT,     
  TCPESTABDECONF     BIGINT,     
  ULIPFRAGPACKETS    BIGINT,     
  DLIPFRAGPACKETS    BIGINT,     
  TCPFIRSTREDE       BIGINT,     
  TCPFIRSTCONF       BIGINT,     
  WINSIZE            BIGINT,     
  MSSSIZE            BIGINT,     
  TCPATTNUM          BIGINT,     
  TCPLINKSTATUS      BIGINT,     
  SESSIONFLAG        BIGINT
  )PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS  TEXTFILE
location '/${DB_PATH}/tb_ifc_common';


DROP TABLE IF EXISTS  tb_xdr_ifc_s1u_http;
create EXTERNAL table tb_xdr_ifc_s1u_http
(
 LENGTH              BIGINT,          
  CITY               BIGINT,        
  INTERFACE          BIGINT,        
  XDRID              STRING,     
  RAT                BIGINT,        
  IMSI               STRING,     
  IMEI               STRING,   
  MSISDN             STRING,     
  MACHINEIPADDTYPE   BIGINT,        
  SGWIPADDR          STRING,     
  ENBIPADDR          STRING,     
  SGWPORT            BIGINT,        
  ENBPORT            BIGINT,        
  ENBGTPTEID         BIGINT,     
  SGWGTPTEID         BIGINT,     
  TAC                BIGINT,     
  ecgi               BIGINT,     
  APN                STRING,     
  APPTYPECODE        BIGINT,     
  PROCEDURESTARTTIME BIGINT,     
  PROCEDUREENDTIME   BIGINT,     
  PROTOCOLTYPE       BIGINT,     
  APPTYPE            BIGINT,     
  APPSUBTYPE         BIGINT,     
  APPCONTENT         BIGINT,        
  APPSTATUS          BIGINT,        
  USERIPV4           STRING,     
  USERIPV6           STRING,     
  USERPORT           STRING,     
  L4PROTOCAL         BIGINT,        
  APPSERVERIPIPV4    STRING,     
  APPSERVERIPIPV6    STRING,     
  APPSERVERPORT      STRING,     
  ULDATA             BIGINT,     
  DLDATA             BIGINT,     
  ULIPPACKET         BIGINT,     
  DLIPPACKET         BIGINT,     
  ULTCPPACKETOR      BIGINT,     
  DLTCPPACKETOR      BIGINT,     
  ULTCPPACKETRE      BIGINT,     
  DLTCPPACKETRE      BIGINT,     
  TCPESTABREDE       BIGINT,     
  TCPESTABDECONF     BIGINT,     
  ULIPFRAGPACKETS    BIGINT,     
  DLIPFRAGPACKETS    BIGINT,     
  TCPFIRSTREDE       BIGINT,     
  TCPFIRSTCONF       BIGINT,     
  WINSIZE            BIGINT,     
  MSSSIZE            BIGINT,     
  TCPATTNUM          BIGINT,     
  TCPLINKSTATUS      BIGINT,     
  SESSIONFLAG        BIGINT,        
  HTTPVERSION        STRING,     
  TRANSACTIONTYPE    STRING,     
  HTTPSTATE          STRING,     
  HTTPFIRSTREDE      BIGINT,     
  HTTPLASTREDE       BIGINT,     
  ACKLASTCONF        BIGINT,     
  HOST               STRING,     
  URI                STRING,     
  XONLINEHOST        STRING,     
  USERAGENT          STRING,     
  HTTPCONTENTTYPE    STRING,     
  REFERURI           STRING,     
  COOKIE             STRING,     
  CONTENTLENGTH      BIGINT,     
  TARGETBEHAVIOR     BIGINT,        
  WTPINTERRUPTTYPE   BIGINT,        
  WTPINTERRUPTCAUSE  BIGINT,        
  TITLE              STRING,     
  KEYWORD            STRING,     
  BUSCONDUCTLOGO     BIGINT,        
  BUSCOMPLETIONFLAG  BIGINT,        
  BUSREDE            BIGINT,        
  BROWSINGTOOL       BIGINT ,       
  PORTALAPP          BIGINT     
)PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS  TEXTFILE
location '/${DB_PATH}/tb_xdr_ifc_http';



DROP TABLE IF EXISTS tb_xdr_ifc_s1u_dns;
create EXTERNAL table tb_xdr_ifc_s1u_dns
(
  LENGTH             BIGINT,       
  CITY               BIGINT,     
  INTERFACE          BIGINT,     
  XDRID              STRING,  
  RAT                BIGINT,     
  IMSI               STRING,  
  IMEI               STRING,
  MSISDN             STRING,  
  MACHINEIPADDTYPE   BIGINT,    
  SGWIPADDR          STRING,  
  ENBIPADDR          STRING,  
  SGWPORT            BIGINT,     
  ENBPORT            BIGINT,     
  ENBGTPTEID         STRING,  
  SGWGTPTEID         BIGINT,  
  TAC                BIGINT,  
  ecgi               BIGINT,  
  APN                STRING,  
  APPTYPECODE        BIGINT,  
  PROCEDURESTARTTIME BIGINT,  
  PROCEDUREENDTIME   BIGINT,  
  PROTOCOLTYPE       BIGINT,  
  APPTYPE            BIGINT,  
  APPSUBTYPE         BIGINT,  
  APPCONTENT         BIGINT,     
  APPSTATUS          BIGINT,     
  USERIPV4           STRING,  
  USERIPV6           STRING,  
  USERPORT           STRING,  
  L4PROTOCAL         BIGINT,     
  APPSERVERIPIPV4    STRING,  
  APPSERVERIPIPV6    STRING,  
  APPSERVERPORT      STRING,  
  ULDATA             BIGINT, 
  DLDATA             BIGINT, 
  ULIPPACKET         BIGINT, 
  DLIPPACKET         BIGINT, 
  ULTCPPACKETOR      BIGINT, 
  DLTCPPACKETOR      BIGINT, 
  ULTCPPACKETRE      BIGINT, 
  DLTCPPACKETRE      BIGINT, 
  TCPESTABREDE       BIGINT, 
  TCPESTABDECONF     BIGINT, 
  ULIPFRAGPACKETS    BIGINT, 
  DLIPFRAGPACKETS    BIGINT, 
  TCPFIRSTREDE       BIGINT, 
  TCPFIRSTCONF       BIGINT, 
  WINSIZE            BIGINT, 
  MSSSIZE            BIGINT, 
  TCPATTNUM          BIGINT,    
  TCPLINKSTATUS      BIGINT,     
  SESSIONFLAG        BIGINT,     
  REQDNS             STRING, 
  RESULTIP           STRING,  
  DNSRECODE          BIGINT, 
  DNSATTNUM          BIGINT, 
  DNSRENUM           BIGINT, 
  LICENSEDCONNUM     BIGINT, 
  ADDITIONALCONNUM   BIGINT
)PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS  TEXTFILE
location '/${DB_PATH}/tb_xdr_ifc_dns';



DROP TABLE IF EXISTS tb_xdr_ifc_s1u_mms;
create  EXTERNAL table tb_xdr_ifc_s1u_mms
(
  LENGTH             BIGINT,    
  CITY               BIGINT,  
  INTERFACE          BIGINT,  
  XDRID              STRING,
  RAT                BIGINT,  
  IMSI               STRING,
  IMEI               BIGINT,
  MSISDN             STRING,
  MACHINEIPADDTYPE   BIGINT,  
  SGWIPADDR          STRING,
  ENBIPADDR          STRING,
  SGWPORT            BIGINT,  
  ENBPORT            BIGINT,  
  ENBGTPTEID         BIGINT,
  SGWGTPTEID         BIGINT,
  TAC                BIGINT,
 ecgi                BIGINT,  
 APN                 STRING,
 APPTYPECODE         BIGINT,
 PROCEDURESTARTTIME  BIGINT,
 PROCEDUREENDTIME    BIGINT,
 PROTOCOLTYPE        BIGINT,
 APPTYPE             BIGINT,
 APPSUBTYPE          BIGINT,
 APPCONTENT          BIGINT,
 APPSTATUS           BIGINT,
 USERIPV4            STRING,
 USERIPV6            STRING,
 USERPORT            STRING,
 L4PROTOCAL          BIGINT,   
 APPSERVERIPIPV4     STRING,
 APPSERVERIPIPV6     STRING,
 APPSERVERPORT       STRING,
 ULDATA              BIGINT,
 DLDATA              BIGINT,
 ULIPPACKET          BIGINT,
 DLIPPACKET          BIGINT,
 ULTCPPACKETOR       BIGINT,
 DLTCPPACKETOR       BIGINT,
 ULTCPPACKETRE       BIGINT,
 DLTCPPACKETRE       BIGINT,
 TCPESTABREDE        BIGINT,
 TCPESTABDECONF      BIGINT,
 ULIPFRAGPACKETS     BIGINT,
 DLIPFRAGPACKETS     BIGINT,
 TCPFIRSTREDE        BIGINT,
 TCPFIRSTCONF        BIGINT,
 WINSIZE             BIGINT,
 MSSSIZE             BIGINT,
 TCPATTNUM           BIGINT,
 TCPLINKSTATUS       BIGINT,
 SESSIONFLAG         BIGINT,   
 TRANSTYPE           BIGINT,   
 SUCCESSFLAG         BIGINT,   
 HTTPORWAP1X         BIGINT,   
 HTTPWAPCODE         BIGINT,
 MMSERSPSTATUS       BIGINT,
 MMSSENDADDR         STRING,
 MMSMSGID            STRING,
 MMSTRANSACTIONID    STRING,
 MMSRETRIVEADDR      STRING,
 MMSRETRIVEADDRNUM   BIGINT,
 MMSCCBCCADDR        STRING,
 MMSCCBCCADDRNUM     BIGINT,
 MMSSUBJECT          STRING,
 MMSDATASIZE         BIGINT,
 MMSCIPADDR          STRING,
 HOST                STRING,
 URI                 STRING,
 XONLINEHOST         STRING
)
PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS  TEXTFILE
location '/${DB_PATH}/tb_xdr_ifc_mms';



DROP TABLE IF EXISTS tb_xdr_ifc_s1u_ftp;
create EXTERNAL  table tb_xdr_ifc_s1u_ftp
(
LENGTH             BIGINT,    
CITY               BIGINT,    
INTERFACE          BIGINT,    
XDRID              STRING, 
RAT                BIGINT,    
IMSI               STRING, 
IMEI               BIGINT, 
MSISDN             STRING, 
MACHINEIPADDTYPE   BIGINT,    
SGWIPADDR          STRING, 
ENBIPADDR          STRING, 
SGWPORT            BIGINT,    
ENBPORT            BIGINT,    
ENBGTPTEID         BIGINT, 
SGWGTPTEID         BIGINT, 
TAC                BIGINT, 
ecgi               BIGINT,   
APN                STRING, 
APPTYPECODE        BIGINT, 
PROCEDURESTARTTIME BIGINT, 
PROCEDUREENDTIME   BIGINT, 
PROTOCOLTYPE       BIGINT, 
APPTYPE            BIGINT, 
APPSUBTYPE         BIGINT, 
APPCONTENT         BIGINT,    
APPSTATUS          BIGINT,    
USERIPV4           STRING, 
USERIPV6           STRING, 
USERPORT           STRING, 
L4PROTOCAL         BIGINT,    
APPSERVERIPIPV4    STRING, 
APPSERVERIPIPV6    STRING, 
APPSERVERPORT      STRING, 
ULDATA             BIGINT, 
DLDATA             BIGINT, 
ULIPPACKET         BIGINT, 
DLIPPACKET         BIGINT, 
ULTCPPACKETOR      BIGINT, 
DLTCPPACKETOR      BIGINT, 
ULTCPPACKETRE      BIGINT, 
DLTCPPACKETRE      BIGINT, 
TCPESTABREDE       BIGINT, 
TCPESTABDECONF     BIGINT, 
ULIPFRAGPACKETS    BIGINT, 
DLIPFRAGPACKETS    BIGINT, 
TCPFIRSTREDE       BIGINT, 
TCPFIRSTCONF       BIGINT, 
WINSIZE            BIGINT, 
MSSSIZE            BIGINT, 
TCPATTNUM          BIGINT, 
TCPLINKSTATUS      BIGINT, 
SESSIONFLAG        BIGINT, 
FTPSTATUS          BIGINT,    
LOGINUSER          STRING, 
CURRENTDIRECTORY   STRING, 
FILETRANSFERMODE   BIGINT,    
TRANSDIRECTFLAG    BIGINT,    
FILENAME           STRING, 
FTPLOCDATAPORT     BIGINT,    
FTPREMDATAPORT     BIGINT,    
TOTALFILESIZE      BIGINT, 
FTPREDE            BIGINT, 
TRANSDURA          BIGINT

  
)
PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS  TEXTFILE
location '/${DB_PATH}/tb_xdr_ifc_ftp';



DROP TABLE IF EXISTS tb_xdr_ifc_s1u_email;
create EXTERNAL table tb_xdr_ifc_s1u_email
(
   LENGTH             BIGINT,        
   CITY               BIGINT,      
   INTERFACE          BIGINT,      
   XDRID              STRING,   
   RAT                BIGINT,      
   IMSI               STRING,   
   IMEI               STRING, 
   MSISDN             STRING,   
   MACHINEIPADDTYPE   BIGINT,     
   SGWIPADDR          STRING,   
   ENBIPADDR          STRING,   
   SGWPORT            BIGINT,      
   ENBPORT            BIGINT,      
   ENBGTPTEID         BIGINT,   
   SGWGTPTEID         BIGINT,   
   TAC                BIGINT,   
   ecgi               BIGINT,     
   APN                STRING,   
   APPTYPECODE        BIGINT,   
   PROCEDURESTARTTIME BIGINT,   
   PROCEDUREENDTIME   BIGINT,   
   PROTOCOLTYPE       BIGINT,   
   APPTYPE            BIGINT,   
   APPSUBTYPE         BIGINT,   
   APPCONTENT         BIGINT,      
   APPSTATUS          BIGINT,      
   USERIPV4           STRING,   
   USERIPV6           STRING,   
   USERPORT           STRING,   
   L4PROTOCAL         BIGINT,      
   APPSERVERIPIPV4    STRING,   
   APPSERVERIPIPV6    STRING,   
   APPSERVERPORT      STRING,   
   ULDATA             BIGINT,   
   DLDATA             BIGINT,   
   ULIPPACKET         BIGINT,   
   DLIPPACKET         BIGINT,   
   ULTCPPACKETOR      BIGINT,   
   DLTCPPACKETOR      BIGINT,   
   ULTCPPACKETRE      BIGINT,   
   DLTCPPACKETRE      BIGINT,   
   TCPESTABREDE       BIGINT,   
   TCPESTABDECONF     BIGINT,   
   ULIPFRAGPACKETS    BIGINT,   
   DLIPFRAGPACKETS    BIGINT,   
   TCPFIRSTREDE       BIGINT,   
   TCPFIRSTCONF       BIGINT,   
   WINSIZE            BIGINT,   
   MSSSIZE            BIGINT,   
   TCPATTNUM          BIGINT,  
   TCPLINKSTATUS      BIGINT,   
   SESSIONFLAG        BIGINT,   
   EMAILTRANSTYPE     BIGINT,   
   RESTATUSCODE       BIGINT,   
   USERNAME           STRING,   
   SENDER             STRING,   
   EMAIL              STRING,   
   SMTPADDR           STRING,   
   RECIPACC           STRING,   
   MESSHEADINFOR      STRING,   
   ACCESSTYPE         BIGINT 
)PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS  TEXTFILE
location '/${DB_PATH}/tb_xdr_ifc_email';




DROP TABLE IF EXISTS tb_xdr_ifc_s1u_voip;
create EXTERNAL table tb_xdr_ifc_s1u_voip
(
 LENGTH             BIGINT,     
 CITY               BIGINT,    
 INTERFACE          BIGINT,    
 XDRID              STRING, 
 RAT                BIGINT,    
 IMSI               STRING, 
 IMEI               BIGINT, 
 MSISDN             STRING, 
 MACHINEIPADDTYPE   BIGINT,   
 SGWIPADDR          STRING, 
 ENBIPADDR          STRING, 
 SGWPORT            BIGINT,    
 ENBPORT            BIGINT,    
 ENBGTPTEID         BIGINT, 
 SGWGTPTEID         BIGINT, 
 TAC                BIGINT, 
 ecgi               BIGINT,   
 APN                STRING, 
 APPTYPECODE        BIGINT, 
 PROCEDURESTARTTIME BIGINT, 
 PROCEDUREENDTIME   BIGINT, 
 PROTOCOLTYPE       BIGINT, 
 APPTYPE            BIGINT, 
 APPSUBTYPE         BIGINT, 
 APPCONTENT         BIGINT,    
 APPSTATUS          BIGINT,    
 USERIPV4           STRING, 
 USERIPV6           STRING, 
 USERPORT           STRING, 
 L4PROTOCAL         BIGINT,    
 APPSERVERIPIPV4    STRING, 
 APPSERVERIPIPV6    STRING, 
 APPSERVERPORT      STRING, 
 ULDATA             BIGINT, 
 DLDATA             BIGINT, 
 ULIPPACKET         BIGINT, 
 DLIPPACKET         BIGINT, 
 ULTCPPACKETOR      BIGINT, 
 DLTCPPACKETOR      BIGINT, 
 ULTCPPACKETRE      BIGINT, 
 DLTCPPACKETRE      BIGINT, 
 TCPESTABREDE       BIGINT, 
 TCPESTABDECONF     BIGINT, 
 ULIPFRAGPACKETS    BIGINT, 
 DLIPFRAGPACKETS    BIGINT, 
 TCPFIRSTREDE       BIGINT, 
 TCPFIRSTCONF       BIGINT, 
 WINSIZE            BIGINT, 
 MSSSIZE            BIGINT, 
 TCPATTNUM          BIGINT,
 TCPLINKSTATUS      BIGINT, 
 SESSIONFLAG        BIGINT,    
 CALLDIRECTION      BIGINT,    
 CALLERNUM          STRING, 
 CALLEDNUM          STRING, 
 CALLTYPE           BIGINT,    
 VOIPSTREAMSNUM     BIGINT, 
 REASONHOOK         BIGINT,    
 SIGNPROTYPE        BIGINT
)PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS  TEXTFILE
location '/${DB_PATH}/tb_xdr_ifc_voip';


DROP TABLE if exists tb_xdr_ifc_s1u_rtsp;
create EXTERNAL   table  tb_xdr_ifc_s1u_rtsp
(
  LENGTH             BIGINT,
  CITY               BIGINT,
  INTERFACE          BIGINT,
  XDRID              STRING,
  RAT                BIGINT,
  IMSI               STRING,
  IMEI               BIGINT,
  MSISDN             STRING,
  MACHINEIPADDTYPE   BIGINT,
  SGWIPADDR          STRING,
  ENBIPADDR          STRING,
  SGWPORT            BIGINT,
  ENBPORT            BIGINT,
  ENBGTPTEID         STRING,
  SGWGTPTEID         STRING,
  TAC                STRING,
  ecgi               BIGINT,
  APN                STRING,
  APPTYPECODE        BIGINT,
  PROCEDURESTARTTIME BIGINT,
  PROCEDUREENDTIME   BIGINT,
  PROTOCOLTYPE       BIGINT,
  APPTYPE            BIGINT,
  APPSUBTYPE         BIGINT,
  APPCONTENT         BIGINT,
  APPSTATUS          BIGINT,
  USERIPV4           STRING,
  USERIPV6           STRING,
  USERPORT           STRING,
  L4PROTOCAL         BIGINT,
  APPSERVERIPIPV4    STRING,
  APPSERVERIPIPV6    STRING,
  APPSERVERPORT      STRING,
  ULDATA             BIGINT,
  DLDATA             BIGINT,
  ULIPPACKET         BIGINT,
  DLIPPACKET         BIGINT,
  ULTCPPACKETOR      BIGINT,
  DLTCPPACKETOR      BIGINT,
  ULTCPPACKETRE      BIGINT,
  DLTCPPACKETRE      BIGINT,
  TCPESTABREDE       BIGINT,
  TCPESTABDECONF     BIGINT,
  ULIPFRAGPACKETS    BIGINT,
  DLIPFRAGPACKETS    BIGINT,
  TCPFIRSTREDE       BIGINT,
  TCPFIRSTCONF       BIGINT,
  WINSIZE            BIGINT,
  MSSSIZE            BIGINT,
  TCPATTNUM          BIGINT,
  TCPLINKSTATUS      BIGINT,
  SESSIONFLAG        BIGINT,
  RTSPURL            STRING,
  USERAGENT          STRING,
  RTPIP              STRING,
  RTPSTARTCLPORT     BIGINT,
  RTPENDCLPORT       BIGINT,
  RTPSTARTSERPORT    BIGINT,
  RTPENDSERPORT      BIGINT,
  RTSPVIDSTREAM      BIGINT,
  RTSPAUDSTREAM      BIGINT,
  RTSPREDE           BIGINT
  
)
PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS  TEXTFILE
location '/${DB_PATH}/tb_xdr_ifc_rtsp';



DROP TABLE if exists tb_xdr_ifc_s1u_rtcomm;
create EXTERNAL table  tb_xdr_ifc_s1u_rtcomm
(
  LENGTH             BIGINT,        
  CITY               BIGINT,        
  INTERFACE          BIGINT,        
  XDRID              STRING,     
  RAT                BIGINT,        
  IMSI               STRING,     
  IMEI               STRING,   
  MSISDN             STRING,     
  MACHINEIPADDTYPE   BIGINT ,       
  SGWIPADDR          STRING,     
  ENBIPADDR          STRING,     
  SGWPORT            BIGINT,     
  ENBPORT            BIGINT,     
  ENBGTPTEID         STRING,     
  SGWGTPTEID         STRING,     
  TAC                STRING,     
  ecgi               BIGINT,     
  APN                STRING,     
  APPTYPECODE        BIGINT,     
  PROCEDURESTARTTIME BIGINT,     
  PROCEDUREENDTIME   BIGINT,     
  PROTOCOLTYPE       BIGINT,     
  APPTYPE            BIGINT,     
  APPSUBTYPE         BIGINT,     
  APPCONTENT         BIGINT,
  APPSTATUS          BIGINT,
  USERIPV4           STRING,
  USERIPV6           STRING,
  USERPORT           STRING,
  L4PROTOCAL         BIGINT,
  APPSERVERIPIPV4    STRING,
  APPSERVERIPIPV6    STRING,
  APPSERVERPORT      STRING,
  ULDATA             BIGINT,
  DLDATA             BIGINT,
  ULIPPACKET         BIGINT,
  DLIPPACKET         BIGINT,
  ULTCPPACKETOR      BIGINT,
  DLTCPPACKETOR      BIGINT,
  ULTCPPACKETRE      BIGINT,
  DLTCPPACKETRE      BIGINT,
  TCPESTABREDE       BIGINT,
  TCPESTABDECONF     BIGINT,
  ULIPFRAGPACKETS    BIGINT,
  DLIPFRAGPACKETS    BIGINT,
  TCPFIRSTREDE       BIGINT,
  TCPFIRSTCONF       BIGINT,
  WINSIZE            BIGINT,
  MSSSIZE            BIGINT,
  TCPATTNUM          BIGINT,
  TCPLINKSTATUS      BIGINT,
  SESSIONFLAG        BIGINT,
  RTCOMM             STRING,
  APPVERSION         STRING,
  CLVERSION          STRING,
  OPERTYPE           BIGINT
  
)
PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS  TEXTFILE
location '/${DB_PATH}/tb_xdr_ifc_rtcomm';


DROP TABLE if exists tb_xdr_ifc_s1u_p2p;
create EXTERNAL  table  tb_xdr_ifc_s1u_p2p
(
  LENGTH             BIGINT,        
  CITY               BIGINT,        
  INTERFACE          BIGINT,        
  XDRID              STRING,     
  RAT                BIGINT,        
  IMSI               STRING,     
  IMEI               STRING,   
  MSISDN             STRING,     
  MACHINEIPADDTYPE   BIGINT,       
  SGWIPADDR          STRING,     
  ENBIPADDR          STRING,     
  SGWPORT            BIGINT,     
  ENBPORT            BIGINT,     
  ENBGTPTEID         STRING,     
  SGWGTPTEID         STRING,     
  TAC                STRING,     
  ecgi               BIGINT,     
  APN                STRING,     
  APPTYPECODE        BIGINT,     
  PROCEDURESTARTTIME BIGINT,     
  PROCEDUREENDTIME   BIGINT,     
  PROTOCOLTYPE       BIGINT,     
  APPTYPE            BIGINT,     
  APPSUBTYPE         BIGINT,     
  APPCONTENT         BIGINT,
  APPSTATUS          BIGINT,
  USERIPV4           STRING,
  USERIPV6           STRING,
  USERPORT           STRING,
  L4PROTOCAL         BIGINT,
  APPSERVERIPIPV4    STRING,
  APPSERVERIPIPV6    STRING,
  APPSERVERPORT      STRING,
  ULDATA             BIGINT,
  DLDATA             BIGINT,
  ULIPPACKET         BIGINT,
  DLIPPACKET         BIGINT,
  ULTCPPACKETOR      BIGINT,
  DLTCPPACKETOR      BIGINT,
  ULTCPPACKETRE      BIGINT,
  DLTCPPACKETRE      BIGINT,
  TCPESTABREDE       BIGINT,
  TCPESTABDECONF     BIGINT,
  ULIPFRAGPACKETS    BIGINT,
  DLIPFRAGPACKETS    BIGINT,
  TCPFIRSTREDE       BIGINT,
  TCPFIRSTCONF       BIGINT,
  WINSIZE            BIGINT,
  MSSSIZE            BIGINT,
  TCPATTNUM          BIGINT,
  TCPLINKSTATUS      BIGINT,
  SESSIONFLAG        BIGINT,
  FILESIZE           BIGINT,
  P2PSIGN            STRING,
  TRACKER            STRING
)
PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS  TEXTFILE
location '/${DB_PATH}/tb_xdr_ifc_p2p';



EOF

exit 0

