#!/bin/bash

DDLDB=$1
DB_PATH=$2

#sh VolumeAnalyseInitTable.sh shanxi  'datang'

hive<<EOF

CREATE DATABASE IF NOT EXISTS ${DDLDB};
USE ${DDLDB};

--volte用户表
drop table if exists volte_user_data;
CREATE TABLE volte_user_data(
  ttime string,
  hours int,
  imsi string, 
  volte_start int, 
  volte_end int)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;

--高铁用户识别表
drop table volte_gtuser_data;
create table volte_gtuser_data(
imsi string,
hours string,
ttime string
)
PARTITIONED BY (
  dt string,
  h string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ',' ;


--小区统计表（分钟级）
drop table if exists gt_pulse_detail;
create table gt_pulse_detail(
ttime string,
hours int,
minutes int,
cellid bigint,
imsi string,
imei string,
gtuser_flag int,
volteuser_flag int,
sub_pulse_mark int)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;



--子脉冲统计表(分钟级)
drop table if exists  gt_pulse_cell_min;
create table gt_pulse_cell_min(
ttime string,
hours int,
minutes int,
cellid bigint,
sub_pulse_mark int,
sub_pulse_type int,
users bigint,
gt_users bigint,
comm_users bigint,
volte_users bigint)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;

--脉冲统计小时表
drop table if exists gt_pulse_cell_base60;
create table gt_pulse_cell_base60(
     ttime string,
     hours int,
     cellid bigint,
     pulse_mark bigint,
     pulse_type bigint,
     pulse_timelen bigint,
     sub_users_peak bigint,
     sub_gtusers_peak bigint,
     sub_volteusers_peak bigint,
     users bigint,
     gt_users bigint,
     volte_users bigint)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


--脉冲明细小时表
drop table  if exists gt_pulse_detail_base60;
create table gt_pulse_detail_base60(
ttime string,
hours int,
cellid bigint,
imsi string,
pulse_mark bigint,
pulse_type bigint,
pulse_timelen bigint,
first_pulse_mark bigint,
gtuser_flag bigint,
volteuser_flag bigint)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;



--线路高铁用户占用频段表
drop table  if exists gt_line_freq_baseday;
create table gt_line_freq_baseday(
line string,
ttime string,
cell_feq string,
cell_num bigint,
gtusers bigint,
commusers bigint,
cellavguses bigint)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


--城市高铁用户频段表
drop table  if exists gt_city_freq_baseday;
create table gt_city_freq_baseday(
     city string,
     ttime string,
     cell_feq string,
     cell_num bigint,
     gtusers bigint,
     commusers bigint,
     cellavguses bigint)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;

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


EOF
exit 0

