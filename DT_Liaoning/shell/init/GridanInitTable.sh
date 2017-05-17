#!/bin/bash

DATABASE=$1


hive<<EOF

create database $DATABASE;
use $DATABASE ;

drop table LTE_MRO_SOURCE_ANA_TMP ;
create table LTE_MRO_SOURCE_ANA_TMP(
  OBJECTID               int,
  VID                    int,
  STARTTIME              string,
  ENDTIME                string,
  TIMESEQ                int,
  ENBID                  int,
  MRNAME                 string,
  CELLID                 int,
  MMEUES1APID            bigint,
  MMEGROUPID             bigint,
  MMECODE                bigint,
  MEATIME                string,
  GRIDCENTERLONGITUDE    double,
  GRIDCENTERLATITUDE     double,
  OLDGRIDCENTERLONGITUDE double,
  OLDGRIDCENTERLATITUDE  double,
  KPI1                   bigint,
  KPI2                   bigint,
  KPI3                   bigint,
  KPI4                   bigint,
  KPI5                   bigint,
  KPI6                   bigint,
  KPI7                   bigint,
  KPI8                   bigint,
  KPI9                   bigint,
  KPI10                  bigint,
  KPI11                  bigint,
  KPI12                  bigint,
  KPI13                  bigint,
  KPI14                  bigint,
  KPI15                  bigint,
  KPI16                  bigint,
  KPI17                  bigint,
  KPI18                  bigint,
  KPI19                  bigint,
  KPI20                  bigint,
  KPI21                  bigint,
  KPI22                  bigint,
  KPI23                  bigint,
  KPI24                  bigint,
  KPI25                  bigint,
  KPI26                  bigint,
  KPI27                  bigint,
  KPI28                  bigint,
  KPI29                  bigint,
  OID                    bigint
  )PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


drop table lte_mro_disturb_pretreate60 ;
CREATE TABLE lte_mro_disturb_pretreate60(
  id bigint, 
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






drop table ltecell ;
create table ltecell(
  MCC         bigint,
  MNC         bigint,
  MMEGROUPID  bigint,
  MMEID       bigint,
  ENODEBID    bigint,
  SITENAME    string,
  CELLNAME    string,
  LOCALCELLID bigint,
  CELLID      bigint,
  TAC         bigint,
  PCI         bigint,
  FREQ1       bigint,
  FREQ2       bigint,
  BANDWIDTH1  double,
  BANDWIDTH2  double,
  FREQCOUNT   bigint,
  LONGITUDE   double,
  LATITUDE    double,
  SECTORTYPE  int,
  DOORTYPE    bigint,
  TILTTOTAL   double,
  TILTM       double,
  TILTE       double,
  AZIMUTH     double,
  BEAMWIDTH   double,
  VBEAMWIDTH  double,
  AHEIGHT     double,
  CITY        string,
  COMPANY     string,
  REGION      string,
  ENBTYPE     string,
  ENBVERSION  string,
  ISVIP       bigint,
  ISDTGARDEN  bigint,
  COVERAGE    string,
  RADIUS      bigint,
  ISBOUND     int,
  CGS         int
)ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


drop table lte_mrs_dlbestrow_ana60;
CREATE TABLE lte_mrs_dlbestrow_ana60(
  id bigint, 
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


drop table MR_INDOORANA_TEMP;
create table MR_INDOORANA_TEMP
(
  MMEUES1APID bigint,
  ENBID       bigint,
  CELLID      bigint,
  MRCOUNT     bigint,
  VARI_TA     double,
  VARI_AOA    double,
  AVG_SRCRSRP bigint,
  INDOORFLAG  int
)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


drop table LTE_MRO_JOINUSER_ANA60;
create table LTE_MRO_JOINUSER_ANA60
(
  ID                 bigint,
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
  ID                             bigint,
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

drop table LTE2LTEADJ;
create table LTE2LTEADJ
(
  MMEGROUPID    bigint,
  MMEID         bigint,
  ENODEBID      bigint,
  CELLNAME      string,
  CELLID        bigint,
  ADJMMEGROUPID bigint,
  ADJMMEID      bigint,
  ADJENODEBID   bigint,
  ADJCELLNAME   string,
  ADJCELLID     bigint
)PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


drop table LTE_MRO_DISTURB_PRETREATE60tmp;
CREATE TABLE LTE_MRO_DISTURB_PRETREATE60tmp(
  starttime date, 
  endtime date, 
  timeseq string, 
  mmeid string, 
  enodebid string, 
  cellid string, 
  cellname string, 
  cellpci string, 
  cellfreq string, 
  tenodebid string, 
  tcellid string, 
  tcellname string, 
  tcellpci string, 
  tcellfreq string, 
  cellrsrpsum double, 
  cellrsrpcount string, 
  tcellrsrpsum double, 
  tcellrsrpcount string, 
  rsrpdifseqls12 string, 
  rsrpdifseqn12 string, 
  rsrpdifseqn11 string, 
  rsrpdifseqn10 string, 
  rsrpdifseqn9 string, 
  rsrpdifseqn8 string, 
  rsrpdifseqn7 string, 
  rsrpdifseqn6 string, 
  rsrpdifseqn5 string, 
  rsrpdifseqn4 string, 
  rsrpdifseqn3 string, 
  rsrpdifseqn2 string, 
  rsrpdifseqn1 string, 
  rsrpdifseqn0 string, 
  rsrpdifseqp1 string, 
  rsrpdifseqp2 string, 
  rsrpdifseqp3 string, 
  rsrpdifseqp4 string, 
  rsrpdifseqp5 string, 
  rsrpdifseqp6 string, 
  rsrpdifseqp7 string, 
  rsrpdifseqp8 string, 
  rsrpdifseqp9 string, 
  rsrpdifseqp10 string, 
  rsrpdifseqp11 string, 
  rsrpdifseqp12 string, 
  rsrpdifseqp13 string, 
  rsrpdifseqp14 string, 
  rsrpdifseqp15 string, 
  rsrpdifseqp16 string, 
  rsrpdifseqp17 string, 
  rsrpdifseqp18 string, 
  rsrpdifseqp19 string, 
  rsrpdifseqp20 string, 
  rsrpdifseqp21 string, 
  rsrpdifseqp22 string, 
  rsrpdifseqp23 string, 
  rsrpdifseqp24 string, 
  rsrpdifseqp25 string, 
  rsrpdifseqmo25 string)
PARTITIONED BY ( 
  dt string, 
  h string)
  ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


drop table lte2lteadj_pci;
CREATE TABLE lte2lteadj_pci(
  mmegroupid bigint, 
  mmeid bigint, 
  enodebid bigint, 
  cellname string, 
  cellid bigint, 
  pci bigint, 
  freq1 bigint, 
  adjmmegroupid bigint, 
  adjmmeid bigint, 
  adjenodebid bigint, 
  adjcellname string, 
  adjcellid bigint, 
  adjpci bigint, 
  adjfreq1 bigint)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;

drop table LTE_MRO_DISTURB_PRETREATE60;
create table LTE_MRO_DISTURB_PRETREATE60(
  id             int,
  starttime      DATE,
  endtime        DATE,
  timeseq        int,
  mmegroupid     int,
  mmeid          int,
  enodebid       int,
  cellid         int,
  cellname      string,
  cellpci        int,
  cellfreq       int,
  tenodebid      int,
  tcellid        int,
  tcellname     string,
  tcellpci       int,
  tcellfreq      int,
  cellrsrpsum    double,
  cellrsrpcount  int,
  tcellrsrpsum   double,
  tcellrsrpcount int,
  rsrpdifseqls12 int,
  rsrpdifseqn12  int,
  rsrpdifseqn11  int,
  rsrpdifseqn10  int,
  rsrpdifseqn9   int,
  rsrpdifseqn8   int,
  rsrpdifseqn7   int,
  rsrpdifseqn6   int,
  rsrpdifseqn5   int,
  rsrpdifseqn4   int,
  rsrpdifseqn3   int,
  rsrpdifseqn2   int,
  rsrpdifseqn1   int,
  rsrpdifseqn0   int,
  rsrpdifseqp1   int,
  rsrpdifseqp2   int,
  rsrpdifseqp3   int,
  rsrpdifseqp4   int,
  rsrpdifseqp5   int,
  rsrpdifseqp6   int,
  rsrpdifseqp7   int,
  rsrpdifseqp8   int,
  rsrpdifseqp9   int,
  rsrpdifseqp10  int,
  rsrpdifseqp11  int,
  rsrpdifseqp12  int,
  rsrpdifseqp13  int,
  rsrpdifseqp14  int,
  rsrpdifseqp15  int,
  rsrpdifseqp16  int,
  rsrpdifseqp17  int,
  rsrpdifseqp18  int,
  rsrpdifseqp19  int,
  rsrpdifseqp20  int,
  rsrpdifseqp21  int,
  rsrpdifseqp22  int,
  rsrpdifseqp23  int,
  rsrpdifseqp24  int,
  rsrpdifseqp25  int,
  rsrpdifseqmo25 int
)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;


drop table grid_view;
CREATE TABLE grid_view(
  objectid_1 string, 
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
PARTITIONED BY ( 
  dt string, 
  h string)
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

drop tbale LTE_MRO_OVERLAP_B_ANA60;
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

drop table CELL_LTEMRKPI60;
create table CELL_LTEMRKPI60(
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

 create table LTE_MRO_DISTURB_SEC
(
  ID         int ,
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


CREATE TABLE lte_mro_disturb_ana(
  id int, 
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
  
  CREATE TABLE lte_mro_disturb_mix(
  id int, 
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
  
CREATE TABLE lte_mro_adjcover_ana60(
  id int, 
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

create table LTE_MRO_SOURCE_TMP
(
  OBJECTID               int ,
  VID                    int ,
  STARTTIME              string ,
  ENDTIME                string ,
  TIMESEQ                string,
  ENBID                  int ,
  MRNAME                 string,
  CELLID                 int ,
  MMEUES1APID            int,
  MMEGROUPID             int,
  MMECODE                int,
  MEATIME                string,
  GRIDCENTERLONGITUDE    string,
  GRIDCENTERLATITUDE     string,
  OLDGRIDCENTERLONGITUDE string,
  OLDGRIDCENTERLATITUDE  string,
  KPI1                   int,
  KPI2                   int,
  KPI3                   int,
  KPI4                   int,
  KPI5                   int,
  KPI6                   int,
  KPI7                   int,
  KPI8                   int,
  KPI9                   int,
  KPI10                  int,
  KPI11                  int,
  KPI12                  int,
  KPI13                  int,
  KPI14                  int,
  KPI15                  int,
  KPI16                  int,
  KPI17                  int,
  KPI18                  int,
  KPI19                  int,
  KPI20                  int,
  KPI21                  int,
  KPI22                  int,
  KPI23                  int,
  KPI24                  int,
  KPI25                  int,
  KPI26                  int,
  KPI27                  int,
  KPI28                  int,
  KPI29                  int,
  OID                    int
)PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ','  ;
  
    create table LTE_MRO_DISTURB_SEC
(
  ID         int ,
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



  CREATE TABLE lte_mro_disturb_mix(
  id int, 
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
  FIELDS TERMINATED BY ','  ;


EOF

exit 0