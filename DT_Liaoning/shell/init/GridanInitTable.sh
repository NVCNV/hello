#!/bin/bash

DATABASE=$1

INITDATABSES=$2
SOURCEDIR=$3


usage="Please input ResultDatabase,InitDatabase,SourceDir
       Example [ sh GridanInitTable.sh morpho liaoning datang ]"

# if no args specified, show usage
if [ $# -le 2 ]; then
  echo $usage
  exit 1
fi

hive<<EOF

create database if not exists $DATABASE;
use $DATABASE ;



drop table lte_mro_disturb_pretreate60 ;
CREATE TABLE lte_mro_disturb_pretreate60(
  starttime string,
  endtime string,
  timeseq bigint,
  mmegroupid bigint,
  mmeid bigint,
  enodebid bigint,
  cellid bigint,
  cellname string,
  cellpci bigint,
  cellfreq bigint,
  tenodebid bigint,
  tcellid bigint,
  tcellname string,
  tcellpci bigint,
  tcellfreq bigint,
  cellrsrpsum double,
  cellrsrpcount bigint,
  tcellrsrpsum double,
  tcellrsrpcount bigint,
  rsrpdifseqls12 bigint,
  rsrpdifseqn12 bigint,
  rsrpdifseqn11 bigint,
  rsrpdifseqn10 bigint,
  rsrpdifseqn9 bigint,
  rsrpdifseqn8 bigint,
  rsrpdifseqn7 bigint,
  rsrpdifseqn6 bigint,
  rsrpdifseqn5 bigint,
  rsrpdifseqn4 bigint,
  rsrpdifseqn3 bigint,
  rsrpdifseqn2 bigint,
  rsrpdifseqn1 bigint,
  rsrpdifseqn0 bigint,
  rsrpdifseqp1 bigint,
  rsrpdifseqp2 bigint,
  rsrpdifseqp3 bigint,
  rsrpdifseqp4 bigint,
  rsrpdifseqp5 bigint,
  rsrpdifseqp6 bigint,
  rsrpdifseqp7 bigint,
  rsrpdifseqp8 bigint,
  rsrpdifseqp9 bigint,
  rsrpdifseqp10 bigint,
  rsrpdifseqp11 bigint,
  rsrpdifseqp12 bigint,
  rsrpdifseqp13 bigint,
  rsrpdifseqp14 bigint,
  rsrpdifseqp15 bigint,
  rsrpdifseqp16 bigint,
  rsrpdifseqp17 bigint,
  rsrpdifseqp18 bigint,
  rsrpdifseqp19 bigint,
  rsrpdifseqp20 bigint,
  rsrpdifseqp21 bigint,
  rsrpdifseqp22 bigint,
  rsrpdifseqp23 bigint,
  rsrpdifseqp24 bigint,
  rsrpdifseqp25 bigint,
  rsrpdifseqmo25 bigint)
PARTITIONED BY (
  dt string,
  h string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ',' ;



drop table lte_mro_disturb_pretreate60 ;
CREATE TABLE lte_mro_disturb_pretreate60(
  starttime string,
  endtime string,
  timeseq bigint,
  mmegroupid bigint,
  mmeid bigint,
  enodebid bigint,
  cellid bigint,
  cellname string,
  cellpci bigint,
  cellfreq bigint,
  tenodebid bigint,
  tcellid bigint,
  tcellname string,
  tcellpci bigint,
  tcellfreq bigint,
  cellrsrpsum double,
  cellrsrpcount bigint,
  tcellrsrpsum double,
  tcellrsrpcount bigint,
  rsrpdifseqls12 bigint,
  rsrpdifseqn12 bigint,
  rsrpdifseqn11 bigint,
  rsrpdifseqn10 bigint,
  rsrpdifseqn9 bigint,
  rsrpdifseqn8 bigint,
  rsrpdifseqn7 bigint,
  rsrpdifseqn6 bigint,
  rsrpdifseqn5 bigint,
  rsrpdifseqn4 bigint,
  rsrpdifseqn3 bigint,
  rsrpdifseqn2 bigint,
  rsrpdifseqn1 bigint,
  rsrpdifseqn0 bigint,
  rsrpdifseqp1 bigint,
  rsrpdifseqp2 bigint,
  rsrpdifseqp3 bigint,
  rsrpdifseqp4 bigint,
  rsrpdifseqp5 bigint,
  rsrpdifseqp6 bigint,
  rsrpdifseqp7 bigint,
  rsrpdifseqp8 bigint,
  rsrpdifseqp9 bigint,
  rsrpdifseqp10 bigint,
  rsrpdifseqp11 bigint,
  rsrpdifseqp12 bigint,
  rsrpdifseqp13 bigint,
  rsrpdifseqp14 bigint,
  rsrpdifseqp15 bigint,
  rsrpdifseqp16 bigint,
  rsrpdifseqp17 bigint,
  rsrpdifseqp18 bigint,
  rsrpdifseqp19 bigint,
  rsrpdifseqp20 bigint,
  rsrpdifseqp21 bigint,
  rsrpdifseqp22 bigint,
  rsrpdifseqp23 bigint,
  rsrpdifseqp24 bigint,
  rsrpdifseqp25 bigint,
  rsrpdifseqmo25 bigint)
PARTITIONED BY (
  dt string,
  h string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ',' ;


drop table lte_mrs_dlbestrow_ana60;
CREATE TABLE lte_mrs_dlbestrow_ana60(
  starttime string, 
  endtime string, 
  timeseq bigint, 
  mmegroupid bigint, 
  mmeid bigint, 
  enodebid bigint, 
  sitename string, 
  cellid bigint, 
  cellname string, 
  usercount bigint, 
  idrusercount bigint, 
  rsrpsum double, 
  idrrsrpsum double, 
  rsrpcount bigint, 
  idrrsrpcount bigint, 
  rsrpavg double, 
  weakbestrowmrcount bigint, 
  idrweakbestrowmrcount bigint, 
  weakbestrowrate double, 
  goodbestrowmrcount bigint, 
  goodbestrowrate double, 
  powerheadroomlowmrcount bigint, 
  powerheadroomtotalcount bigint, 
  powerheadroomlowrate double, 
  powerheadroomsum bigint, 
  powerheadroomavg double, 
  rxtxtimediffbigmrcount bigint, 
  rxtxtimedifftotalcount bigint, 
  rxtxtimediffbigrate double, 
  rxtxtimediffsum bigint, 
  rxtxtimediffavg double, 
  aoacount bigint, 
  aoasum double, 
  aoadeviates double)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


drop table LTE_MRO_JOINUSER_ANA60;
create table LTE_MRO_JOINUSER_ANA60
(
  STARTTIME          string,
  ENDTIME            string,
  TIMESEQ            bigint,
  MMEGROUPID         bigint,
  MMEID              bigint,
  ENODEBID           bigint,
  SITENAME           string,
  CELLID             bigint,
  CELLNAME           string,
  MMES1APUEID        string,
  CELLMRCOUNT        bigint,
  USERRSRPCOUNT      bigint,
  USERRSRPSUM        double,
  WEAKBESTROWMRCOUNT bigint,
  GOODBESTROWMRCOUNT bigint,
  LASTRSRPCOUNT      double
)PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;



drop table LTE_MRS_OVERCOVER_ANA60;
create table LTE_MRS_OVERCOVER_ANA60
(
  STARTTIME                      string,
  ENDTIME                        string,
  TIMESEQ                        bigint,
  MMEGROUPID                     bigint,
  MMEID                          bigint,
  ENODEBID                       bigint,
  CELLID                         bigint,
  TCELLID                        bigint,
  TCELLPCI                       bigint,
  TCELLFREQ                      bigint,
  RSRPDIFABS                     bigint,
  RSRPDIFCOUNT                   bigint,
  MRCOUNT                        bigint,
  CELLRSRPSUM                    double,
  CELLRSRPCOUNT                  bigint,
  TCELLRSRPSUM                   double,
  TCELLRSRPCOUNT                 bigint,
  ADJACENTAREAINTERFERENCEINTENS double,
  OVERLAPDISTURBRSRPDIFCOUNT     bigint,
  ADJEFFECTRSRPCOUNT             bigint
)PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


drop table LTE2LTEADJ_PCI;
create table LTE2LTEADJ_PCI
(
  MMEGROUPID    bigint,
  MMEID         bigint,
  ENODEBID      bigint,
  CELLNAME      string,
  CELLID        bigint,
  PCI           bigint,
  FREQ1         bigint,
  ADJMMEGROUPID bigint,
  ADJMMEID      bigint,
  ADJENODEBID   bigint,
  ADJCELLNAME   string,
  ADJCELLID     bigint,
  ADJPCI        bigint,
  ADJFREQ1      bigint
)PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;




drop table grid_view;
CREATE TABLE grid_view(
  objectid string, 
  shapeentity double, 
  shapenumpts double, 
  shapeminx double, 
  shapeminy double, 
  shapemaxx double, 
  shapemaxy double, 
  shapeminz double, 
  shapemaxz double, 
  shapeminm double, 
  shapemaxm double, 
  shapearea double, 
  shapelen double, 
  shapesrid double, 
  shapepoints double, 
  x double, 
  y double, 
  x1 double, 
  y1 double)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


drop table LTE_MRS_DLBESTROW_GRID_ANA60;
create table LTE_MRS_DLBESTROW_GRID_ANA60(
   OID   bigint,
   STARTTIME   string,
   ENDTIME   string,
   TIMESEQ   bigint,
   ENODEBID   bigint,
   CELLID   bigint,
   GRIDCENTERLONGITUDE   double,
   GRIDCENTERLATITUDE   double,
   USERCOUNT   bigint,
   IDRUSERCOUNT   bigint,
   RSRPSUM   double,
   IDRRSRPSUM   double,
   RSRPCOUNT   bigint,
   IDRRSRPCOUNT   bigint,
   WEAKBESTROWMRCOUNT   bigint,
   IDRWEAKBESTROWMRCOUNT   bigint,
   POWERHEADROOMTOTALCOUNT   bigint,
   POWERHEADROOMLOWMRCOUNT   bigint
  )PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


drop table LTE_MRO_OVERLAP_GRID_ANA60;
create table LTE_MRO_OVERLAP_GRID_ANA60(
   OID   bigint,
   STARTTIME   string,
   ENDTIME   string,
   TIMESEQ   bigint,
   ENODEBID   bigint,
   CELLID   bigint,
   GRIDCENTERLONGITUDE   double,
   GRIDCENTERLATITUDE   double,
   USERCOUNT   bigint,
   OVERLAPBESTROWCELLCOUNT   bigint,
   ADJACENTAREAINTERFERENCEINTENS   double,
   RSRQCOUNT   bigint,
   RSRQSUM   double,
   CELLOVERLAPBESTROWMRCOUNT   bigint,
   RSRPCOUNT   bigint,
   RSRPSUM   double
  )PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


drop table GRID_LTEMRKPI60;
create table GRID_LTEMRKPI60(
   BEGINTIME   string,
   ENDTIME   string,
   TIMESEQ   bigint,
   ENODEBID   bigint,
   CELLID   bigint,
   GRIDCENTERLONGITUDE   double,
   GRIDCENTERLATITUDE   double,
   OID   bigint,
   KPI1049   bigint,
   KPI1239   bigint,
   KPI1011   bigint,
   KPI1012   bigint,
   KPI1241   bigint,
   KPI1243   bigint,
   KPI1245   bigint,
   KPI1247   bigint
  )PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


drop table CELL_LTEMRKPITEMP;
create table CELL_LTEMRKPITEMP(
   BEGINTIME   string,
   ENDTIME   string,
   TIMESEQ   bigint,
   ENODEBID   bigint,
   CELLID   bigint,
   KPI1001   bigint,
   KPI1002   double,
   KPI1003   bigint,
   KPI1004   double,
   KPI1005   bigint,
   KPI1006   bigint,
   KPI1011   bigint,
   KPI1012   bigint,
   KPI1009   bigint,
   KPI1010   bigint,
   KPI1049   bigint,
   KPI1050   bigint,
   KPI1119   bigint,
   KPI1120   bigint,
   KPI1123   bigint,
   KPI1124   bigint,
   KPI1127   bigint,
   KPI1128   bigint,
   KPI1129   bigint,
   KPI1130   bigint,
   KPI1131   bigint,
   KPI1132   bigint,
   KPI1133   bigint,
   KPI1134   bigint,
   KPI1135   bigint,
   KPI1136   bigint,
   KPI1137   bigint,
   KPI1138   bigint,
   KPI1239   bigint,
   KPI1249   bigint,
   KPI1250   bigint,
   KPI1251   bigint,
   KPI1252   bigint,
   KPI1253   bigint,
   KPI1254   bigint,
   KPI1121   bigint,
   KPI1122   double,
   KPI1125   bigint,
   KPI1126   double,
   KPI1183   bigint,
   KPI1184   bigint,
   KPI1189   bigint,
   KPI1190   bigint,
   KPI1195   bigint,
   KPI1196   double,
   KPI1197   bigint,
   KPI1198   double,
   KPI1199   bigint,
   KPI1200   double,
   KPI1201   bigint,
   KPI1202   double,
   KPI1203   bigint,
   KPI1204   double,
   KPI1205   bigint,
   KPI1206   double,
   KPI1207   bigint,
   KPI1208   double,
   KPI1209   bigint,
   KPI1210   double,
   KPI1211   bigint,
   KPI1212   double,
   KPI1213   bigint,
   KPI1214   double,
   KPI1013   bigint,
   KPI1014   bigint,
   KPI1015   bigint,
   KPI1016   bigint,
   KPI1017   bigint,
   KPI1018   bigint,
   KPI1019   bigint,
   KPI1020   bigint,
   KPI1021   bigint,
   KPI1022   bigint,
   KPI1023   bigint,
   KPI1024   bigint,
   KPI1025   bigint,
   KPI1026   bigint,
   KPI1027   bigint,
   KPI1028   bigint,
   KPI1029   bigint,
   KPI1030   bigint,
   KPI1031   bigint,
   KPI1032   bigint,
   KPI1033   bigint,
   KPI1034   bigint,
   KPI1035   bigint,
   KPI1036   bigint,
   KPI1037   bigint,
   KPI1038   bigint,
   KPI1039   bigint,
   KPI1040   bigint,
   KPI1041   bigint,
   KPI1042   bigint,
   KPI1043   bigint,
   KPI1044   bigint,
   KPI1045   bigint,
   KPI1046   bigint,
   KPI1047   bigint,
   KPI1048   bigint,
   KPI1007   bigint,
   KPI1008   bigint,
   KPI1241   bigint,
   KPI1242   bigint,
   KPI1245   bigint,
   KPI1246   bigint,
   KPI1237   bigint,
   KPI1243   bigint,
   KPI1247   bigint
  )PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;

drop table LTE_MRO_OVERLAP_B_ANA60;
create table LTE_MRO_OVERLAP_B_ANA60(
   STARTTIME   string,
   ENDTIME   string,
   TIMESEQ   bigint,
   ENODEBID   bigint,
   CELLID   bigint,
   USERCOUNT   bigint,
   RSRPSUM   double,
   RSRPCOUNT   bigint,
   RSRPAVG   double,
   OVERLAPBESTROWCELLCOUNT   bigint,
   ADJACENTAREAINTERFERENCEINTENS   double,
   ADJACENTAREAINTERFERENCEINDEX   double,
   CELLOVERLAPBESTROWRATIO   double,
   CELLOVERLAPBESTROWMRCOUNT   bigint,
   RSRQSUM   bigint,
   RSRQCOUNT   bigint,
   RSRQAVG   double
  )PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


drop table CELL_LTENEWMRKPI60;
create table CELL_LTENEWMRKPI60(
   STARTTIME   string,
   ENDTIME   string,
   TIMESEQ   bigint,
   MMEGROUPID   bigint,
   MMEID   bigint,
   ENODEBID   bigint,
   CELLID   bigint,
   MROVERLAYCOUNT   bigint,
   MROVERCOVERCOUNT   bigint,
   MRLOSENEIBCOUNT   bigint,
   MREDGEWEAKCOVERCOUNT   bigint
  )PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;



drop table if exists LTE_MRO_DISTURB_SEC;
 create table LTE_MRO_DISTURB_SEC
(
  STARTTIME  string ,
  ENDTIME    string ,
  PERIOD     int ,
  TIMESEQ    int ,
  MMEGROUPID int,
  MMEID      int,
  ENODEBID   int,
  CELLID     int,
  CELLNAME   string,
  PCI        int,
  SFN        int,
  KPINAME    string,
  SEQ0       int,
  SEQ1       int,
  SEQ2       int,
  SEQ3       int,
  SEQ4       int,
  SEQ5       int,
  SEQ6       int,
  SEQ7       int,
  SEQ8       int,
  SEQ9       int,
  SEQ10      int,
  SEQ11      int,
  SEQ12      int,
  SEQ13      int,
  SEQ14      int,
  SEQ15      int,
  SEQ16      int,
  SEQ17      int,
  SEQ18      int,
  SEQ19      int,
  SEQ20      int,
  SEQ21      int,
  SEQ22      int,
  SEQ23      int,
  SEQ24      int,
  SEQ25      int,
  SEQ26      int,
  SEQ27      int,
  SEQ28      int,
  SEQ29      int,
  SEQ30      int,
  SEQ31      int,
  SEQ32      int,
  SEQ33      int,
  SEQ34      int,
  SEQ35      int,
  SEQ36      int,
  SEQ37      int,
  SEQ38      int,
  SEQ39      int,
  SEQ40      int,
  SEQ41      int,
  SEQ42      int,
  SEQ43      int,
  SEQ44      int,
  SEQ45      int,
  SEQ46      int,
  SEQ47      int,
  SEQ48      int,
  SEQ49      int,
  SEQ50      int,
  SEQ51      int,
  SEQ52      int,
  SEQ53      int,
  SEQ54      int,
  SEQ55      int,
  SEQ56      int,
  SEQ57      int,
  SEQ58      int,
  SEQ59      int,
  SEQ60      int,
  SEQ61      int,
  SEQ62      int,
  SEQ63      int,
  SEQ64      int,
  SEQ65      int,
  SEQ66      int,
  SEQ67      int,
  SEQ68      int,
  SEQ69      int,
  SEQ70      int,
  SEQ71      int
)PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ','  ;

drop table if exists lte_mro_disturb_ana;
CREATE TABLE lte_mro_disturb_ana(
  starttime string, 
  endtime string, 
  period int, 
  timeseq int, 
  mmegroupid int, 
  mmeid int, 
  enodebid int, 
  cellid int, 
  cellname string, 
  pci int, 
  sfn int, 
  adjdisturbtotalnum bigint, 
  adjavailablenum bigint, 
  celldisturbrate string, 
  isstrongdisturbcell int, 
  asadjcellrsrptotalvalue string, 
  asadjcellrsrptotalnum bigint, 
  asadjcellavgrsrp string, 
  srvcellrsrptotalvalue string, 
  srvcellrsrptotalnum bigint, 
  srvcellavgrsrp string)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;

  drop table if exists lte_mro_disturb_mix;
  CREATE TABLE lte_mro_disturb_mix(
  starttime string, 
  endtime string, 
  period int, 
  timeseq int, 
  mmegroupid int, 
  mmeid int, 
  enodebid int, 
  cellid int, 
  cellname string, 
  pci int, 
  sfn int, 
  disturbmmegroupid int, 
  disturbmmeid int, 
  disturbenodebid int, 
  disturbcellid int, 
  disturbcellname string, 
  disturbpci int, 
  disturbsfn int, 
  disturbnum int, 
  adjdisturbtotalnum bigint, 
  disturbrate string, 
  asadjcellrsrptotalvalue string, 
  asadjcellrsrptotalnum bigint, 
  asadjcellavgrsrp string)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;

drop table if exists lte_mro_adjcover_ana60;
CREATE TABLE lte_mro_adjcover_ana60(
  starttime string, 
  endtime string, 
  timeseq int, 
  mmegroupid int, 
  mmeid int, 
  enodebid int, 
  cellid int, 
  nclackpoorcovercount int, 
  poorcoversumval int, 
  ncovercovercount int, 
  overcoversumval int)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


  drop table if exists LTE_MRO_DISTURB_SEC;
    create table LTE_MRO_DISTURB_SEC
(
  STARTTIME  string ,
  ENDTIME    string ,
  PERIOD     int ,
  TIMESEQ    int ,
  MMEGROUPID int,
  MMEID      int,
  ENODEBID   int,
  CELLID     int,
  CELLNAME   string,
  PCI        int,
  SFN        int,
  KPINAME    string,
  SEQ0       int,
  SEQ1       int,
  SEQ2       int,
  SEQ3       int,
  SEQ4       int,
  SEQ5       int,
  SEQ6       int,
  SEQ7       int,
  SEQ8       int,
  SEQ9       int,
  SEQ10      int,
  SEQ11      int,
  SEQ12      int,
  SEQ13      int,
  SEQ14      int,
  SEQ15      int,
  SEQ16      int,
  SEQ17      int,
  SEQ18      int,
  SEQ19      int,
  SEQ20      int,
  SEQ21      int,
  SEQ22      int,
  SEQ23      int,
  SEQ24      int,
  SEQ25      int,
  SEQ26      int,
  SEQ27      int,
  SEQ28      int,
  SEQ29      int,
  SEQ30      int,
  SEQ31      int,
  SEQ32      int,
  SEQ33      int,
  SEQ34      int,
  SEQ35      int,
  SEQ36      int,
  SEQ37      int,
  SEQ38      int,
  SEQ39      int,
  SEQ40      int,
  SEQ41      int,
  SEQ42      int,
  SEQ43      int,
  SEQ44      int,
  SEQ45      int,
  SEQ46      int,
  SEQ47      int,
  SEQ48      int,
  SEQ49      int,
  SEQ50      int,
  SEQ51      int,
  SEQ52      int,
  SEQ53      int,
  SEQ54      int,
  SEQ55      int,
  SEQ56      int,
  SEQ57      int,
  SEQ58      int,
  SEQ59      int,
  SEQ60      int,
  SEQ61      int,
  SEQ62      int,
  SEQ63      int,
  SEQ64      int,
  SEQ65      int,
  SEQ66      int,
  SEQ67      int,
  SEQ68      int,
  SEQ69      int,
  SEQ70      int,
  SEQ71      int
)PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;




CREATE DATABASE IF NOT EXISTS  ${INITDATABSES}  ;

USE ${INITDATABSES} ;



-- 原始表
drop table if exists lte_mro_source;
CREATE EXTERNAL TABLE lte_mro_source(
  objectid string,
  vid bigint,
  fileformatversion string,
  starttime string,
  endtime string,
  period bigint,
  enbid bigint,
  userlabel string,
  mrname string,
  cellid bigint,
  earfcn bigint,
  subframenbr bigint,
  prbnbr bigint,
  mmeues1apid bigint,
  mmegroupid bigint,
  mmecode bigint,
  meatime string,
  eventtype string,
  gridcenterlongitude string,
  gridcenterlatitude string,
  kpi1 bigint,
  kpi2 bigint,
  kpi3 bigint,
  kpi4 bigint,
  kpi5 bigint,
  kpi6 bigint,
  kpi7 bigint,
  kpi8 bigint,
  kpi9 bigint,
  kpi10 bigint,
  kpi11 bigint,
  kpi12 bigint,
  kpi13 bigint,
  kpi14 bigint,
  kpi15 bigint,
  kpi16 bigint,
  kpi17 bigint,
  kpi18 bigint,
  kpi19 bigint,
  kpi20 bigint,
  kpi21 bigint,
  kpi22 bigint,
  kpi23 bigint,
  kpi24 bigint,
  kpi25 bigint,
  kpi26 bigint,
  kpi27 bigint,
  kpi28 bigint,
  kpi29 bigint,
  kpi30 bigint,
  kpi31 bigint,
  kpi32 bigint,
  kpi33 bigint,
  kpi34 bigint,
  kpi35 bigint,
  kpi36 bigint,
  kpi37 bigint,
  kpi38 bigint,
  kpi39 bigint,
  kpi40 bigint,
  kpi41 bigint,
  kpi42 bigint,
  kpi43 bigint,
  kpi44 bigint,
  kpi45 bigint,
  kpi46 bigint,
  kpi47 bigint,
  kpi48 bigint,
  kpi49 bigint,
  kpi50 bigint,
  kpi51 bigint,
  kpi52 bigint,
  kpi53 bigint,
  kpi54 bigint,
  kpi55 bigint,
  kpi56 bigint,
  kpi57 bigint,
  kpi58 bigint,
  kpi59 bigint,
  kpi60 bigint,
  kpi61 bigint,
  kpi62 bigint,
  kpi63 bigint,
  kpi64 bigint,
  kpi65 bigint,
  kpi66 bigint,
  kpi67 bigint,
  kpi68 bigint,
  kpi69 bigint,
  kpi70 bigint,
  kpi71 bigint,
  length bigint,
  city string,
  xdrtype bigint,
  interface bigint,
  xdrid string,
  rat bigint,
  imsi string,
  imei string,
  msisdn string,
  mrtype bigint,
  neighborcellnumber bigint,
  gsmneighborcellnumber bigint,
  tdsneighborcellnumber bigint,
  v_enb bigint,
  mrtime bigint)
PARTITIONED BY (
  dt string,
  h string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
LOCATION
  'hdfs://dtcluster/${SOURCEDIR}/LTE_MRO_SOURCE' ;

EOF

exit 0