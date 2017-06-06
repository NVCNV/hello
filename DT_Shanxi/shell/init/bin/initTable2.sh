#!/bin/bash
DDLDB=$1
DB_PATH=$2
#DCLDB=$3
export NLS_LANG="AMERICAN_AMERICA.UTF8"
#HQL
hive<<EOF
CREATE DATABASE IF NOT EXISTS ${DDLDB};
USE ${DDLDB};

DROP TABLE IF EXISTS TB_XDR_IFC_UU ;

CREATE EXTERNAL TABLE   IF NOT EXISTS  TB_XDR_IFC_UU (
      PARENTXDRID                  STRING,
      LENGTH                       BIGINT,
      CITY                         STRING,
      INTERFACE                     INT,
      XDRID                         STRING,
      RAT                           BIGINT,      
      IMSI                          STRING,
      IMEI                          STRING,
      MSISDN                        STRING,
      PROCEDURETYPE                 INT,
      PROCEDURESTARTTIME            BIGINT, -- STRING,
      PROCEDUREENDTIME              BIGINT, -- STRING,
      KEYWORD1                      INT,
      KEYWORD2                      INT,
      PROCEDURESTATUS               INT,
      PLMNID                        STRING,
      ENBID                         BIGINT,
      CELLID                        BIGINT,
      CRNTI                         BIGINT,
      TARGETENBID                   BIGINT,
      TARGETCELLID                  BIGINT,
      TARGETCRNTI                   BIGINT,
      MMEUES1APID                   BIGINT,
      MMEGROUPID                    BIGINT,
      MMECODE                       BIGINT,
      MTMSI                         BIGINT,
      CSFBINDICATION                BIGINT,
      REDIRECTEDNETWORK             BIGINT,
      EPSBEARERNUMBER               INT,
      BEARER0ID                     BIGINT,
      BEARER0STATUS                 BIGINT,
      BEARER1ID                     BIGINT,
      BEARER1STATUS                 BIGINT,
      BEARER2ID                     BIGINT,
      BEARER2STATUS                 BIGINT,
      BEARER3ID                     BIGINT,
      BEARER3STATUS                 BIGINT,
      BEARER4ID                     BIGINT,
      BEARER4STATUS                 BIGINT,
      BEARER5ID                     BIGINT,
      BEARER5STATUS                 BIGINT,
      BEARER6ID                     BIGINT,
      BEARER6STATUS                 BIGINT,
      BEARER7ID                     BIGINT,
      BEARER7STATUS                 BIGINT,
      BEARER8ID                     BIGINT,
      BEARER8STATUS                 BIGINT,
      BEARER9ID                     BIGINT,
      BEARER9STATUS                 BIGINT,
      BEARER10ID                     STRING,
      BEARER10STATUS                 STRING,
      BEARER11ID                     STRING,
      BEARER11STATUS                 STRING,
      BEARER12ID                     STRING,
      BEARER12STATUS                 STRING,
      BEARER13ID                     STRING,
      BEARER13STATUS                 STRING,
      BEARER14ID                     STRING,
      BEARER14STATUS                 STRING,
      BEARER15ID                     STRING,
      BEARER15STATUS                 STRING,
      RANGETIME                     STRING
)PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS  TEXTFILE
location '/${DB_PATH}/TB_XDR_IFC_UU';


DROP TABLE  IF EXISTS TB_XDR_IFC_X2;   
CREATE EXTERNAL TABLE  IF NOT EXISTS  TB_XDR_IFC_X2 (
      PARENTXDRID                   STRING,
      LENGTH                         BIGINT,
      CITY                         STRING,
      INTERFACE                     BIGINT,
      XDRID                         STRING,
      RAT                            BIGINT,      
      IMSI                          STRING,
      IMEI                          STRING,
      MSISDN                        STRING,
      PROCEDURETYPE                  BIGINT,
      PROCEDURESTARTTIME             BIGINT, -- STRING,
      PROCEDUREENDTIME               BIGINT, -- STRING,
      PROCEDURESTATUS                BIGINT,
      CELLID                        BIGINT,  --SOURCECELLID
      TARGETCELLID                  BIGINT,
      ENBID                         BIGINT,  --SOURCEENBID
      TARGETENBID                   BIGINT,
      MMEUES1APID                   BIGINT,
      MMEGROUPID                    BIGINT,
      MMECODE                       BIGINT,
      REQUESTCAUSE                  BIGINT,
      FAILURECAUSE                  BIGINT,
      EPSBEARERNUMBER               BIGINT,
      BEARER0ID                     BIGINT,
      BEARER0STATUS                 BIGINT,
      BEARER1ID                     BIGINT,
      BEARER1STATUS                 BIGINT,
      BEARER2ID                     BIGINT,
      BEARER2STATUS                 BIGINT,
      BEARER3ID                     BIGINT,
      BEARER3STATUS                 BIGINT,
      BEARER4ID                     BIGINT,
      BEARER4STATUS                 BIGINT,
      BEARER5ID                     BIGINT,
      BEARER5STATUS                 BIGINT,
      BEARER6ID                     BIGINT,
      BEARER6STATUS                 BIGINT,
      BEARER7ID                     BIGINT,
      BEARER7STATUS                 BIGINT,
      BEARER8ID                     BIGINT,
      BEARER8STATUS                 BIGINT,
      BEARER9ID                     BIGINT,
      BEARER9STATUS                 BIGINT,
      BEARER10ID                     BIGINT,
      BEARER10STATUS                 BIGINT,
      BEARER11ID                     BIGINT,
      BEARER11STATUS                 BIGINT,
      BEARER12ID                     BIGINT,
      BEARER12STATUS                 BIGINT,
      BEARER13ID                     BIGINT,
      BEARER13STATUS                 BIGINT,
      BEARER14ID                     BIGINT,
      BEARER14STATUS                 BIGINT,
      BEARER15ID                     BIGINT,
      BEARER15STATUS                 BIGINT,
      RANGETIME                     STRING
)PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS  TEXTFILE
location '/${DB_PATH}/TB_XDR_IFC_X2';



DROP TABLE  IF EXISTS  TB_XDR_IFC_S1MME;   
CREATE EXTERNAL TABLE  IF NOT EXISTS   TB_XDR_IFC_S1MME (
      PARENTXDRID                   STRING,
      LENGTH                        BIGINT,
      CITY                          STRING,
      INTERFACE                     BIGINT,
      XDRID                         STRING,
      RAT                           BIGINT,      
      IMSI                          STRING,
      IMEI                          STRING,
      MSISDN                        STRING,   
      PROCEDURETYPE                 INT,
      PROCEDURESTARTTIME            BIGINT,
      PROCEDUREENDTIME              BIGINT,
      PROCEDURESTATUS               BIGINT,
      REQUESTCAUSE                  INT,
      FAILURECAUSE                  INT,
      KEYWORD1                      INT,
      KEYWORD2                      INT,
      KEYWORD3                      INT,
      KEYWORD4                      INT,
      MMEUES1APID                   BIGINT,
      OLDMMEGROUPID                 BIGINT,
      OLDMMECODE                    BIGINT,
      OLDMTMSI                      BIGINT,
      MMEGROUPID                    BIGINT,
      MMECODE                       BIGINT,
      MTMSI                         BIGINT,
      TMSI                          BIGINT,
      USERIPV4                      STRING,
      USERIPV6                      STRING,
      MMEIPADD                      STRING,
      ENBIPADD                      STRING,
      MMEPORT                       BIGINT,
      ENBPORT                       BIGINT,
      TAC                           BIGINT,
      CELLID                        BIGINT,
      OTHERTAC                      BIGINT,
      OTHERECI                      BIGINT,
      APN                           STRING,
      EPSBEARERNUMBER               BIGINT,
      BEARER0ID                     string,  
      BEARER0TYPE                   string,
      BEARER0QCI                    string,
      BEARER0STATUS                 string,
      BEARER0REQUESTCAUSE           string,
      BEARER0FAILURECAUSE           string,
      BEARER0ENBGTPTEID             string,
      BEARER0SGWGTPTEID             string,
      BEARER1ID                     string,
      BEARER1TYPE                   string,
      BEARER1QCI                    string,
      BEARER1STATUS                 string,
      BEARER1REQUESTCAUSE           string,
      BEARER1FAILURECAUSE           string,
      BEARER1ENBGTPTEID             string,
      BEARER1SGWGTPTEID             string,
      BEARER2ID                     string,  
      BEARER2TYPE                   string,
      BEARER2QCI                    string,
      BEARER2STATUS                 string,
      BEARER2REQUESTCAUSE           string,
      BEARER2FAILURECAUSE           string,
      BEARER2ENBGTPTEID             string,
      BEARER2SGWGTPTEID             string,
      BEARER3ID                     string,
      BEARER3TYPE                   string,
      BEARER3QCI                    string,
      BEARER3STATUS                 string,
      BEARER3REQUESTCAUSE           string,
      BEARER3FAILURECAUSE           string,
      BEARER3ENBGTPTEID             string,
      BEARER3SGWGTPTEID             string,
      BEARER4ID                    string,  
      BEARER4TYPE                   string,
      BEARER4QCI                    string,
      BEARER4STATUS                 string,
      BEARER4REQUESTCAUSE           string,
      BEARER4FAILURECAUSE           string,
      BEARER4ENBGTPTEID             string,
      BEARER4SGWGTPTEID             string,
      BEARER5ID                     string,
      BEARER5TYPE                   string,
      BEARER5QCI                    string,
      BEARER5STATUS                 string,
      BEARER5REQUESTCAUSE           string,
      BEARER5FAILURECAUSE           string,
      BEARER5ENBGTPTEID             string,
      BEARER5SGWGTPTEID             string,
      BEARER6ID                     string,  
      BEARER6TYPE                   string,
      BEARER6QCI                    string,
      BEARER6STATUS                 string,
      BEARER6REQUESTCAUSE           string,
      BEARER6FAILURECAUSE           string,
      BEARER6ENBGTPTEID             string,
      BEARER6SGWGTPTEID             string,
      BEARER7ID                     string,
      BEARER7TYPE                   string,
      BEARER7QCI                    string,
      BEARER7STATUS                 string,
      BEARER7REQUESTCAUSE           string,
      BEARER7FAILURECAUSE           string,
      BEARER7ENBGTPTEID             string,
      BEARER7SGWGTPTEID             string,
      BEARER8ID                     string,  
      BEARER8TYPE                   string,
      BEARER8QCI                    string,
      BEARER8STATUS                string,
      BEARER8REQUESTCAUSE           string,
      BEARER8FAILURECAUSE           string,
      BEARER8ENBGTPTEID             string,
      BEARER8SGWGTPTEID             string,
      BEARER9ID                     string,
      BEARER9TYPE                   string,
      BEARER9QCI                    string,
      BEARER9STATUS                 string,
      BEARER9REQUESTCAUSE           string,
      BEARER9FAILURECAUSE           string,
      BEARER9ENBGTPTEID             string,
      BEARER9SGWGTPTEID             string,
      BEARER10ID                     STRING,  
      BEARER10TYPE                   STRING,
      BEARER10QCI                    STRING,
      BEARER10STATUS                 STRING,
      BEARER10REQUESTCAUSE           STRING,
      BEARER10FAILURECAUSE           STRING,
      BEARER10ENBGTPTEID             STRING,
      BEARER10SGWGTPTEID             STRING,
      BEARER11ID                     STRING,
      BEARER11TYPE                   STRING,
      BEARER11QCI                    STRING,
      BEARER11STATUS                 STRING,
      BEARER11REQUESTCAUSE           STRING,
      BEARER11FAILURECAUSE           STRING,
      BEARER11ENBGTPTEID             STRING,
      BEARER11SGWGTPTEID             STRING,
      BEARER12ID                     STRING,  
      BEARER12TYPE                   STRING,
      BEARER12QCI                    STRING,
      BEARER12STATUS                 STRING,
      BEARER12REQUESTCAUSE           STRING,
      BEARER12FAILURECAUSE           STRING,
      BEARER12ENBGTPTEID             STRING,
      BEARER12SGWGTPTEID             STRING,
      BEARER13ID                     STRING,
      BEARER13TYPE                   STRING,
      BEARER13QCI                    STRING,
      BEARER13STATUS                 STRING,
      BEARER13REQUESTCAUSE           STRING,
      BEARER13FAILURECAUSE           STRING,
      BEARER13ENBGTPTEID             STRING,
      BEARER13SGWGTPTEID             STRING,                        
      BEARER14ID                     STRING,  
      BEARER14TYPE                   STRING,
      BEARER14QCI                    STRING,
      BEARER14STATUS                 STRING,
      BEARER14REQUESTCAUSE           STRING,
      BEARER14FAILURECAUSE           STRING,
      BEARER14ENBGTPTEID             STRING,
      BEARER14SGWGTPTEID             STRING,
      BEARER15ID                     STRING,
      BEARER15TYPE                   STRING,
      BEARER15QCI                    STRING,
      BEARER15STATUS                 STRING,
      BEARER15REQUESTCAUSE           STRING,
      BEARER15FAILURECAUSE           STRING,
      BEARER15ENBGTPTEID             STRING,
      BEARER15SGWGTPTEID             STRING,
      RANGETIME                     STRING
)PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED    
FIELDS TERMINATED BY ','
STORED AS  TEXTFILE
location '/${DB_PATH}/TB_XDR_IFC_S1MME'; 



DROP TABLE   IF EXISTS TB_XDR_IFC_SGS;   
CREATE EXTERNAL TABLE  IF NOT EXISTS TB_XDR_IFC_SGS (
      PARENTXDRID                   STRING,
      LENGTH                       BIGINT,
      CITY                          STRING,
      INTERFACE                     BIGINT,
      XDRID                         STRING,
      RAT                           BIGINT,      
      IMSI                          STRING,
      IMEI                          STRING,
      MSISDN                        STRING,
      PROCEDURETYPE                 BIGINT,
      PROCEDURESTARTTIME            BIGINT,
      PROCEDUREENDTIME              BIGINT,
      PROCEDURESTATUS               BIGINT,
      SGSCAUSE                      BIGINT,
      REJECTCAUSE                   BIGINT,
      CPCAUSE                       BIGINT,
      RPCAUSE                       BIGINT,
      USERIPV4                      STRING,
      USERIPV6                      STRING,
      MMEIPADD                      STRING,
      MSCSERVERIPADD                STRING,
      MMEPORT                       BIGINT,
      MSCSERVERPORT                 BIGINT,
      SERVICEINDICATOR              BIGINT,
      MMENAME                       STRING,
      TMSI                          BIGINT,
      NEWLAC                        BIGINT,
      OLDLAC                        BIGINT,
      TAC                           BIGINT,
      CELLID                        BIGINT,
      CALLINGID                    STRING,
      VLRNAMELENGTH                 BIGINT,
      VLRNAME                       STRING,
      RANGETIME                     STRING
)PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED    
FIELDS TERMINATED BY ','
STORED AS  TEXTFILE
location '/${DB_PATH}/TB_XDR_IFC_SGS';



/**********************************************/
/*1.12 单接口XDR数据结构:SV接口               */
/**********************************************/ 

DROP TABLE  IF EXISTS  TB_XDR_IFC_SV;   
CREATE EXTERNAL TABLE  IF NOT EXISTS  TB_XDR_IFC_SV (
      PARENTXDRID                  STRING,
      LENGTH                        BIGINT,
      CITY                         STRING,
      INTERFACE                     BIGINT,
      XDRID                        STRING,
      RAT                           BIGINT,      
      IMSI                         STRING,
      IMEI                         STRING,
      MSISDN                       STRING,
      PROCEDURETYPE                 BIGINT,
      PROCEDURESTARTTIME           BIGINT,
      PROCEDUREENDTIME             BIGINT,
      SOURCENEIP                   STRING,
      SOURCENEPORT                  BIGINT,
      DESTNEIP                     STRING,
      DESTNEPORT                    BIGINT,
      ROAMDIRECTION                 BIGINT,
      HOMEMCC                       BIGINT,
      HOMEMNC                       BIGINT,
      MCC                           BIGINT,
      MNC                           BIGINT,
      TARGETLAC                     BIGINT,
      SOURCETAC                     BIGINT,
      SOURCEECI                     BIGINT,
      SVFLAGS                       BIGINT,
      ULCMSCIP                     STRING,
      DLCMMEIP                     STRING,
      ULCMSCTEID                    BIGINT,
      DLCMMETEID                    BIGINT,
      STNSR                        STRING,
      TARGETRNCID                   BIGINT,
      TARGETCELLID                  BIGINT,
      ARP                           BIGINT,
      REQUESTRESULT                 BIGINT,
      RESULT                        BIGINT,
      SVCAUSE                       BIGINT,
      POSTFAILURECAUSE              BIGINT,
      RESPDELAY                     BIGINT,
      SVDELAY                       BIGINT,
      RANGETIME                     STRING
)PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED    
FIELDS TERMINATED BY ','
STORED AS  TEXTFILE
location '/${DB_PATH}/TB_XDR_IFC_SV';



/****************************************************/
/*1.13 单接口XDR数据结构:Gm/Mw/Mg/Mi/Mj/ISC接口     */
/****************************************************/ 

DROP TABLE  IF EXISTS TB_XDR_IFC_GMMWMGMIMJISC;   
CREATE EXTERNAL TABLE   IF NOT EXISTS TB_XDR_IFC_GMMWMGMIMJISC (
      LENGTH                        BIGINT,
      CITY                         STRING,
      INTERFACE                     BIGINT,
      XDRID                        STRING,
      RAT                           BIGINT,      
      IMSI                         STRING,
      IMEI                         STRING,
      MSISDN                       STRING,   
      PROCEDURETYPE                 BIGINT,
      PROCEDURESTARTTIME           BIGINT,
      PROCEDUREENDTIME             BIGINT,
      SERVICETYPE                   BIGINT,
      PROCEDURESTATUS               BIGINT,
      CALLINGNUMBER                STRING,
      CALLEDNUMBER                 STRING,
      CALLINGPARTYURI              STRING,
      REQUESTURI                   STRING,
      USERIP                       STRING,
      CALLID                       STRING,
      ICID                         STRING,
      SOURCENEIP                   STRING,
      SOURCENEPORT                  BIGINT,
      DESTNEIP                     STRING,
      DESTNEPORT                    BIGINT,
      CALLSIDE                      BIGINT,
      SOURCEACCESSTYPE              BIGINT,
      SOURCEECI                     BIGINT,
      SOURCETAC                     BIGINT,
      SOURCEIMSI                   STRING,
      SOURCEIMEI                   STRING,
      DESTACCESSTYPE                BIGINT,
      DESTECI                       BIGINT,
      DESTTAC                       BIGINT,
      DESTIMSI                     STRING,
      DESTIMEI                     STRING,
      AUTHTYPE                      BIGINT,
      EXPIRESTIMEREQ               BIGINT,
      EXPIRESTIMERSP               BIGINT,
      CALLINGSDPIPADDR             STRING,
      CALLINGAUDIOSDPPORT           BIGINT,
      CALLINGVIDEOSDPPORT           BIGINT,
      CALLEDSDPIPADDR              STRING,
      CALLEDAUDIOSDPPORT            BIGINT,
      CALLEDVIDEOPORT               BIGINT,
      AUDIOCODEC                    BIGINT,
      VIDEOCODEC                    BIGINT,
      REDIRECTINGPARTYADDRESS      STRING,
      ORIGINALPARTYADDRESS         STRING,
      REDIRECTREASON                BIGINT,
      RESPONSECODE                  BIGINT,
      FINISHWARNINGCODE             BIGINT,
      FINISHREASONPROTOCOL          BIGINT,
      FINISHREASONCAUSE             BIGINT,
      FIRFAILTIME                  BIGINT,
      FIRSTFAILNEIP                STRING,
      ALERTINGTIME                 BIGINT,
      ANSWERTIME                   BIGINT,
      RELEASETIME                  BIGINT,
      CALLDURATION                  BIGINT,
      AUTHREQTIME                  BIGINT,
      AUTHRSPTIME                  BIGINT,
      STNSR                        STRING,
      ATCFMGMT                     STRING,
      ATUSTI                       STRING,
      CMSISDN                      STRING,
      SSI                          STRING,
      RANGETIME                     STRING
)PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED    
FIELDS TERMINATED BY ','
STORED AS  TEXTFILE
location '/${DB_PATH}/TB_XDR_IFC_GMMWMGMIMJISC';


DROP TABLE  IF EXISTS  TB_XDR_IFC_GXRX;   
CREATE EXTERNAL TABLE   IF  NOT  EXISTS  TB_XDR_IFC_GXRX (
      LENGTH                        BIGINT,
      CITY                         STRING,
      INTERFACE                     BIGINT,
      XDRID                        STRING,
      RAT                           BIGINT,      
      IMSI                         STRING,
      IMEI                         STRING,
      MSISDN                       STRING,
      PROCEDURETYPE                 BIGINT,
      PROCEDURESTARTTIME           BIGINT,
      PROCEDUREENDTIME             BIGINT,
      ICID                         STRING,
      ORIGINREALM                  STRING,
      DESTINATIONREALM             STRING,
      ORIGINHOST                   STRING,
      DESTINATIONHOST              STRING,
      SGSNSGWSIGIP                 STRING,
      AFAPPID                      STRING,
      CCREQUESTTYPE                 BIGINT,
      RXREQUESTTYPE                 BIGINT,
      MEDIATYPE                     BIGINT,
      ABORTCAUSE                    BIGINT,
      RESULTCODE                    BIGINT,
      EXPERIMENTALRESULTCODE        BIGINT,
      SESSIONRELEASECAUSE           BIGINT,
      RANGETIME                     STRING      
)PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED    
FIELDS TERMINATED BY ','
STORED AS  TEXTFILE
location '/${DB_PATH}/TB_XDR_IFC_GXRX';



DROP TABLE   IF EXISTS lte_mro_source;  
create EXTERNAL table   IF NOT EXISTS lte_mro_source
(
       objectID       STRING ,
       VID             BIGINT  ,
       fileFormatVersion STRING , 
       startTime       STRING  ,
       endTime         STRING ,
       period          BIGINT ,
       enbID           BIGINT,
       userLabel      STRING ,
       mrName         STRING ,
       cellID          BIGINT ,
       Earfcn          BIGINT ,
       SubFrameNbr     BIGINT ,
       PRBNbr          BIGINT ,
       MmeUeS1apId     BIGINT ,
       MmeGroupId      BIGINT ,
       MmeCode         BIGINT ,
       meaTime         STRING ,
       EventType      STRING ,
       gridcenterLongitude   STRING,
       gridcenterLatitude    STRING,
       kpi1            BIGINT ,
       kpi2            BIGINT ,
       kpi3            BIGINT ,
       kpi4            BIGINT ,
       kpi5            BIGINT ,
       kpi6            BIGINT ,
       kpi7            BIGINT ,
       kpi8            BIGINT ,
       kpi9            BIGINT ,
       kpi10           BIGINT ,
       kpi11           BIGINT ,
       kpi12           BIGINT ,
       kpi13           BIGINT ,
       kpi14           BIGINT ,
       kpi15           BIGINT ,
       kpi16           BIGINT ,
       kpi17           BIGINT ,
       kpi18           BIGINT ,
       kpi19           BIGINT ,
       kpi20           BIGINT ,
       kpi21           BIGINT ,
       kpi22           BIGINT ,
       kpi23           BIGINT ,
       kpi24           BIGINT ,
       kpi25           BIGINT ,
       kpi26           BIGINT ,
       kpi27           BIGINT ,
       kpi28           BIGINT ,
       kpi29           BIGINT ,
       kpi30           BIGINT ,
       kpi31           BIGINT ,
       kpi32           BIGINT ,
       kpi33           BIGINT ,
       kpi34           BIGINT ,
       kpi35           BIGINT ,
       kpi36           BIGINT ,
       kpi37           BIGINT ,
       kpi38           BIGINT ,
       kpi39           BIGINT ,
       kpi40           BIGINT ,
       kpi41           BIGINT ,
       kpi42           BIGINT ,
       kpi43           BIGINT ,
       kpi44           BIGINT ,
       kpi45           BIGINT ,
       kpi46           BIGINT ,
       kpi47           BIGINT ,
       kpi48           BIGINT ,
       kpi49           BIGINT ,
       kpi50           BIGINT ,
       kpi51           BIGINT ,
       kpi52           BIGINT ,
       kpi53           BIGINT ,
       kpi54           BIGINT ,
       kpi55           BIGINT ,
       kpi56           BIGINT ,
       kpi57           BIGINT ,
       kpi58           BIGINT ,
       kpi59           BIGINT ,
       kpi60           BIGINT ,
       kpi61           BIGINT ,
       kpi62           BIGINT ,
       kpi63           BIGINT ,
       kpi64           BIGINT ,
       kpi65           BIGINT ,
       kpi66           BIGINT ,
       kpi67           BIGINT ,
       kpi68           BIGINT ,
       kpi69           BIGINT ,
       kpi70           BIGINT ,
       kpi71           BIGINT ,
       length          BIGINT ,
       City            STRING ,
       XDRType         BIGINT ,
       Interface       BIGINT ,  
       XDRID          STRING ,
       RAT             BIGINT ,
       IMSI           STRING ,
       IMEI           STRING ,
       MSISDN         STRING ,
       MRType          BIGINT ,
       NeighborCellNumber BIGINT ,
       gsmNeighborCellNumber   BIGINT ,
       tdsNeighborCellNumber   BIGINT ,
       v_enb BIGINT,
     mrtime   BIGINT
)PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED    
FIELDS TERMINATED BY ','
STORED AS  TEXTFILE
location '/${DB_PATH}/LTE_MRO_SOURCE';


drop table IF EXISTS t_pub_option_failcause;
create EXTERNAL table IF NOT EXISTS  t_pub_option_failcause
(
  OPTION_DEFINITION_ID BIGINT,
  OPTION_CODE          STRING,
  OPTION_DESC          STRING
)PARTITIONED BY (
dt STRING,
h STRING)
ROW FORMAT DELIMITED    
FIELDS TERMINATED BY ','
STORED AS  TEXTFILE
location '/${DB_PATH}/t_pub_option_failcause';


EOF

exit 0







