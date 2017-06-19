#!/bin/bash

export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:/opt/app/sqoop/bin

#oracle jdbc url @172.30.4.159:1521:hadoop
URL=jdbc:oracle:thin:$1
#oracle username
USERNAME=scott
#oracle passwd
PASSWD=tiger
#export hdfs dir
HDFS_DIR=$2
#table cols
COL_NUM=$3
#map num
MAP_NUM=$4


#columns
LTE_MRO_SOURCE_ANA_TMP='OBJECTID,VID,STARTTIME,ENDTIME,TIMESEQ,ENBID,MRNAME,CELLID,MMEUES1APID,MMEGROUPID,MMECODE,MEATIME,GRIDCENTERLONGITUDE,GRIDCENTERLATITUDE,OLDGRIDCENTERLONGITUDE,OLDGRIDCENTERLATITUDE,KPI1,KPI2,KPI3,KPI4,KPI5,KPI6,KPI7,KPI8,KPI9,KPI10,KPI11,KPI12,KPI13,KPI14,KPI15,KPI16,KPI17,KPI18,KPI19,KPI20,KPI21,KPI22,KPI23,KPI24,KPI25,KPI26,KPI27,KPI28,KPI29,OID'

lte_mro_disturb_pretreate60='id,starttime,endtime,timeseq,mmegroupid,mmeid,enodebid,cellid,cellname,cellpci,cellfreq,tenodebid,tcellid,tcellname,tcellpci,tcellfreq,cellrsrpsum,cellrsrpcount,tcellrsrpsum,tcellrsrpcount,rsrpdifseqls12,rsrpdifseqn12,rsrpdifseqn11,rsrpdifseqn10,rsrpdifseqn9,rsrpdifseqn8,rsrpdifseqn7,rsrpdifseqn6,rsrpdifseqn5,rsrpdifseqn4,rsrpdifseqn3,rsrpdifseqn2,rsrpdifseqn1,rsrpdifseqn0,rsrpdifseqp1,rsrpdifseqp2,rsrpdifseqp3,rsrpdifseqp4,rsrpdifseqp5,rsrpdifseqp6,rsrpdifseqp7,rsrpdifseqp8,rsrpdifseqp9,rsrpdifseqp10,rsrpdifseqp11,rsrpdifseqp12,rsrpdifseqp13,rsrpdifseqp14,rsrpdifseqp15,rsrpdifseqp16,rsrpdifseqp17,rsrpdifseqp18,rsrpdifseqp19,rsrpdifseqp20,rsrpdifseqp21,rsrpdifseqp22,rsrpdifseqp23,rsrpdifseqp24,rsrpdifseqp25,rsrpdifseqmo25'

ltecell='MCC,MNC,MMEGROUPID,MMEID,ENODEBID,SITENAME,CELLNAME,LOCALCELLID,CELLID,TAC,PCI,FREQ1,FREQ2,BANDWIDTH1,BANDWIDTH2,FREQCOUNT,LONGITUDE,LATITUDE,SECTORTYPE,DOORTYPE,TILTTOTAL,TILTM,TILTE,AZIMUTH,BEAMWIDTH,VBEAMWIDTH,AHEIGHT,CITY,COMPANY,REGION,ENBTYPE,ENBVERSION,ISVIP,ISDTGARDEN,COVERAGE,RADIUS,ISBOUND,CGS'

lte_mrs_dlbestrow_ana60='id,starttime,endtime,timeseq,mmegroupid,mmeid,enodebid,sitename,cellid,cellname,usercount,idrusercount,rsrpsum,idrrsrpsum,rsrpcount,idrrsrpcount,rsrpavg,weakbestrowmrcount,idrweakbestrowmrcount,weakbestrowrate,goodbestrowmrcount,goodbestrowrate,powerheadroomlowmrcount,powerheadroomtotalcount,powerheadroomlowrate,powerheadroomsum,powerheadroomavg,rxtxtimediffbigmrcount,rxtxtimedifftotalcount,rxtxtimediffbigrate,rxtxtimediffsum,rxtxtimediffavg,aoacount,aoasum,aoadeviates'

MR_INDOORANA_TEMP='MMEUES1APID,ENBID,CELLID,MRCOUNT,VARI_TA,VARI_AOA,AVG_SRCRSRP,INDOORFLAG'

LTE_MRO_JOINUSER_ANA60='ID,STARTTIME,ENDTIME,TIMESEQ,MMEGROUPID,MMEID,ENODEBID,SITENAME,CELLID,CELLNAME,MMES1APUEID,CELLMRCOUNT,USERRSRPCOUNT,USERRSRPSUM,WEAKBESTROWMRCOUNT,GOODBESTROWMRCOUNT,LASTRSRPCOUNT'

LTE_MRS_OVERCOVER_ANA60='ID,STARTTIME,ENDTIME,TIMESEQ,MMEGROUPID,MMEID,ENODEBID,CELLID,TCELLID,TCELLPCI,TCELLFREQ,RSRPDIFABS,RSRPDIFCOUNT,MRCOUNT,CELLRSRPSUM,CELLRSRPCOUNT,TCELLRSRPSUM,TCELLRSRPCOUNT,ADJACENTAREAINTERFERENCEINTENS,OVERLAPDISTURBRSRPDIFCOUNT,ADJEFFECTRSRPCOUNT'

LTE2LTEADJ_PCI='MMEGROUPID,MMEID,ENODEBID,CELLNAME,CELLID,PCI,FREQ1,ADJMMEGROUPID,ADJMMEID,ADJENODEBID,ADJCELLNAME,ADJCELLID,ADJPCI,ADJFREQ1'

LTE2LTEADJ='MMEGROUPID,MMEID,ENODEBID,CELLNAME,CELLID,ADJMMEGROUPID,ADJMMEID,ADJENODEBID,ADJCELLNAME,ADJCELLID'

LTE_MRO_DISTURB_PRETREATE60tmp='starttime,endtime,timeseq,mmeid,enodebid,cellid,cellname,cellpci,cellfreq,tenodebid,tcellid,tcellname,tcellpci,tcellfreq,cellrsrpsum,cellrsrpcount,tcellrsrpsum,tcellrsrpcount,rsrpdifseqls12,rsrpdifseqn12,rsrpdifseqn11,rsrpdifseqn10,rsrpdifseqn9,rsrpdifseqn8,rsrpdifseqn7,rsrpdifseqn6,rsrpdifseqn5,rsrpdifseqn4,rsrpdifseqn3,rsrpdifseqn2,rsrpdifseqn1,rsrpdifseqn0,rsrpdifseqp1,rsrpdifseqp2,rsrpdifseqp3,rsrpdifseqp4,rsrpdifseqp5,rsrpdifseqp6,rsrpdifseqp7,rsrpdifseqp8,rsrpdifseqp9,rsrpdifseqp10,rsrpdifseqp11,rsrpdifseqp12,rsrpdifseqp13,rsrpdifseqp14,rsrpdifseqp15,rsrpdifseqp16,rsrpdifseqp17,rsrpdifseqp18,rsrpdifseqp19,rsrpdifseqp20,rsrpdifseqp21,rsrpdifseqp22,rsrpdifseqp23,rsrpdifseqp24,rsrpdifseqp25,rsrpdifseqmo25'

lte2lteadj_pci='mmegroupid,mmeid,enodebid,cellname,cellid,pci,freq1,adjmmegroupid,adjmmeid,adjenodebid,adjcellname,adjcellid,adjpci,adjfreq1'

LTE_MRO_DISTURB_PRETREATE60='id,starttime,endtime,timeseq,mmegroupid,mmeid,enodebid,cellid,cellname,cellpci,cellfreq,tenodebid,tcellid,tcellname,tcellpci,tcellfreq,cellrsrpsum,cellrsrpcount,tcellrsrpsum,tcellrsrpcount,rsrpdifseqls12,rsrpdifseqn12,rsrpdifseqn11,rsrpdifseqn10,rsrpdifseqn9,rsrpdifseqn8,rsrpdifseqn7,rsrpdifseqn6,rsrpdifseqn5,rsrpdifseqn4,rsrpdifseqn3,rsrpdifseqn2,rsrpdifseqn1,rsrpdifseqn0,rsrpdifseqp1,rsrpdifseqp2,rsrpdifseqp3,rsrpdifseqp4,rsrpdifseqp5,rsrpdifseqp6,rsrpdifseqp7,rsrpdifseqp8,rsrpdifseqp9,rsrpdifseqp10,rsrpdifseqp11,rsrpdifseqp12,rsrpdifseqp13,rsrpdifseqp14,rsrpdifseqp15,rsrpdifseqp16,rsrpdifseqp17,rsrpdifseqp18,rsrpdifseqp19,rsrpdifseqp20,rsrpdifseqp21,rsrpdifseqp22,rsrpdifseqp23,rsrpdifseqp24,rsrpdifseqp25,rsrpdifseqmo25'

grid_view='objectid_1,objectid,shapeentity,shapenumpts,shapeminx,shapeminy,shapemaxx,shapemaxy,shapeminz,shapemaxz,shapeminm,shapemaxm,shapearea,shapelen,shapesrid,shapepoints,x,y,x1,y1'

LTE_MRS_DLBESTROW_GRID_ANA60='OID,STARTTIME,ENDTIME,TIMESEQ,ENODEBID,CELLID,GRIDCENTERLONGITUDE,GRIDCENTERLATITUDE,USERCOUNT,IDRUSERCOUNT,RSRPSUM,IDRRSRPSUM,RSRPCOUNT,IDRRSRPCOUNT,WEAKBESTROWMRCOUNT,IDRWEAKBESTROWMRCOUNT,POWERHEADROOMTOTALCOUNT,POWERHEADROOMLOWMRCOUNT'

LTE_MRO_OVERLAP_GRID_ANA60='OID,STARTTIME,ENDTIME,TIMESEQ,ENODEBID,CELLID,GRIDCENTERLONGITUDE,GRIDCENTERLATITUDE,USERCOUNT,OVERLAPBESTROWCELLCOUNT,ADJACENTAREAINTERFERENCEINTENS,RSRQCOUNT,RSRQSUM,CELLOVERLAPBESTROWMRCOUNT,RSRPCOUNT,RSRPSUM'

GRID_LTEMRKPI60='BEGINTIME,ENDTIME,TIMESEQ,ENODEBID,CELLID,GRIDCENTERLONGITUDE,GRIDCENTERLATITUDE,OID,KPI1049,KPI1239,KPI1011,KPI1012,KPI1241,KPI1243,KPI1245,KPI1247'

CELL_LTEMRKPITEMP='BEGINTIME,ENDTIME,TIMESEQ,ENODEBID,CELLID,KPI1001,KPI1002,KPI1003,KPI1004,KPI1005,KPI1006,KPI1011,KPI1012,KPI1009,KPI1010,KPI1049,KPI1050,KPI1119,KPI1120,KPI1123,KPI1124,KPI1127,KPI1128,KPI1129,KPI1130,KPI1131,KPI1132,KPI1133,KPI1134,KPI1135,KPI1136,KPI1137,KPI1138,KPI1239,KPI1249,KPI1250,KPI1251,KPI1252,KPI1253,KPI1254,KPI1121,KPI1122,KPI1125,KPI1126,KPI1183,KPI1184,KPI1189,KPI1190,KPI1195,KPI1196,KPI1197,KPI1198,KPI1199,KPI1200,KPI1201,KPI1202,KPI1203,KPI1204,KPI1205,KPI1206,KPI1207,KPI1208,KPI1209,KPI1210,KPI1211,KPI1212,KPI1213,KPI1214,KPI1013,KPI1014,KPI1015,KPI1016,KPI1017,KPI1018,KPI1019,KPI1020,KPI1021,KPI1022,KPI1023,KPI1024,KPI1025,KPI1026,KPI1027,KPI1028,KPI1029,KPI1030,KPI1031,KPI1032,KPI1033,KPI1034,KPI1035,KPI1036,KPI1037,KPI1038,KPI1039,KPI1040,KPI1041,KPI1042,KPI1043,KPI1044,KPI1045,KPI1046,KPI1047,KPI1048,KPI1007,KPI1008,KPI1241,KPI1242,KPI1245,KPI1246,KPI1237,KPI1243,KPI1247'

LTE_MRO_OVERLAP_B_ANA60='STARTTIME,ENDTIME,TIMESEQ,ENODEBID,CELLID,USERCOUNT,RSRPSUM,RSRPCOUNT,RSRPAVG,OVERLAPBESTROWCELLCOUNT,ADJACENTAREAINTERFERENCEINTENS,ADJACENTAREAINTERFERENCEINDEX,CELLOVERLAPBESTROWRATIO,CELLOVERLAPBESTROWMRCOUNT,RSRQSUM,RSRQCOUNT,RSRQAVG'

CELL_LTENEWMRKPI60='STARTTIME,ENDTIME,TIMESEQ,MMEGROUPID,MMEID,ENODEBID,CELLID,MROVERLAYCOUNT,MROVERCOVERCOUNT,MRLOSENEIBCOUNT,MREDGEWEAKCOVERCOUNT'

CELL_LTEMRKPI60='BEGINTIME,ENDTIME,TIMESEQ,ENODEBID,CELLID,KPI1001,KPI1002,KPI1003,KPI1004,KPI1005,KPI1006,KPI1011,KPI1012,KPI1009,KPI1010,KPI1049,KPI1050,KPI1119,KPI1120,KPI1123,KPI1124,KPI1127,KPI1128,KPI1129,KPI1130,KPI1131,KPI1132,KPI1133,KPI1134,KPI1135,KPI1136,KPI1137,KPI1138,KPI1239,KPI1249,KPI1250,KPI1251,KPI1252,KPI1253,KPI1254,KPI1121,KPI1122,KPI1125,KPI1126,KPI1183,KPI1184,KPI1189,KPI1190,KPI1195,KPI1196,KPI1197,KPI1198,KPI1199,KPI1200,KPI1201,KPI1202,KPI1203,KPI1204,KPI1205,KPI1206,KPI1207,KPI1208,KPI1209,KPI1210,KPI1211,KPI1212,KPI1213,KPI1214,KPI1013,KPI1014,KPI1015,KPI1016,KPI1017,KPI1018,KPI1019,KPI1020,KPI1021,KPI1022,KPI1023,KPI1024,KPI1025,KPI1026,KPI1027,KPI1028,KPI1029,KPI1030,KPI1031,KPI1032,KPI1033,KPI1034,KPI1035,KPI1036,KPI1037,KPI1038,KPI1039,KPI1040,KPI1041,KPI1042,KPI1043,KPI1044,KPI1045,KPI1046,KPI1047,KPI1048,KPI1007,KPI1008,KPI1241,KPI1242,KPI1245,KPI1246,KPI1237,KPI1243,KPI1247'

LTE_MRO_DISTURB_SEC='ID,STARTTIME,ENDTIME,PERIOD,TIMESEQ,MMEGROUPID,MMEID,ENODEBID,CELLID,CELLNAME,PCI,SFN,KPINAME,SEQ0,SEQ1,SEQ2,SEQ3,SEQ4,SEQ5,SEQ6,SEQ7,SEQ8,SEQ9,SEQ10,SEQ11,SEQ12,SEQ13,SEQ14,SEQ15,SEQ16,SEQ17,SEQ18,SEQ19,SEQ20,SEQ21,SEQ22,SEQ23,SEQ24,SEQ25,SEQ26,SEQ27,SEQ28,SEQ29,SEQ30,SEQ31,SEQ32,SEQ33,SEQ34,SEQ35,SEQ36,SEQ37,SEQ38,SEQ39,SEQ40,SEQ41,SEQ42,SEQ43,SEQ44,SEQ45,SEQ46,SEQ47,SEQ48,SEQ49,SEQ50,SEQ51,SEQ52,SEQ53,SEQ54,SEQ55,SEQ56,SEQ57,SEQ58,SEQ59,SEQ60,SEQ61,SEQ62,SEQ63,SEQ64,SEQ65,SEQ66,SEQ67,SEQ68,SEQ69,SEQ70,SEQ71'

lte_mro_disturb_ana='id,starttimestring,endtimestring,period,timeseq,mmegroupid,mmeid,enodebid,cellid,cellnamestring,pci,sfn,adjdisturbtotalnum,adjavailablenum,celldisturbratestring,isstrongdisturbcell,asadjcellrsrptotalvaluestring,asadjcellrsrptotalnum,asadjcellavgrsrpstring,srvcellrsrptotalvaluestring,srvcellrsrptotalnum,srvcellavgrsrpstring'

lte_mro_disturb_mix='id,starttime,endtime,period,timeseq,mmegroupid,mmeid,enodebid,cellid,cellname,pci,sfn,disturbmmegroupid,disturbmmeid,disturbenodebid,disturbcellid,disturbcellname,disturbpci,disturbsfn,disturbnum,adjdisturbtotalnumbig,disturbrate,asadjcellrsrptotalvalue,asadjcellrsrptotalnumbig,asadjcellavgrsrp'

lte_mro_adjcover_ana60='id,starttime,endtime,timeseq,mmegroupid,mmeid,enodebid,cellid,nclackpoorcovercount,poorcoversumval,ncovercovercount,overcoversumval'

LTE_MRO_SOURCE_TMP='OBJECTID,VID,STARTTIME,ENDTIME,TIMESEQ,ENBID,MRNAME,CELLID,MMEUES1APID,MMEGROUPID,MMECODE,MEATIME,GRIDCENTERLONGITUDE,GRIDCENTERLATITUDE,OLDGRIDCENTERLONGITUDE,OLDGRIDCENTERLATITUDE,KPI1,KPI2,KPI3,KPI4,KPI5,KPI6,KPI7,KPI8,KPI9,KPI10,KPI11,KPI12,KPI13,KPI14,KPI15,KPI16,KPI17,KPI18,KPI19,KPI20,KPI21,KPI22,KPI23,KPI24,KPI25,KPI26,KPI27,KPI28,KPI29,OID'

if [ ${COL_NUM} = 1 ];then
    COLS=${LTE_MRO_SOURCE_ANA_TMP}
    TABLE=LTE_MRO_SOURCE_ANA_TMP
elif [ ${COL_NUM} = 2 ];then
    COLS=${lte_mro_disturb_pretreate60}
    TABLE=lte_mro_disturb_pretreate60
elif [ ${COL_NUM} = 3 ];then
    COLS=${ltecell}
    TABLE=ltecell
elif [ ${COL_NUM} = 4 ];then
    COLS=${lte_mrs_dlbestrow_ana60}
    TABLE=lte_mrs_dlbestrow_ana60
elif [ ${COL_NUM} = 5 ];then
    COLS=${MR_INDOORANA_TEMP}
    TABLE=MR_INDOORANA_TEMP
elif [ ${COL_NUM} = 6 ];then
    COLS=${LTE_MRO_JOINUSER_ANA60}
    TABLE=LTE_MRO_JOINUSER_ANA60
elif [ ${COL_NUM} = 7 ];then
    COLS=${LTE_MRS_OVERCOVER_ANA60}
    TABLE=LTE_MRS_OVERCOVER_ANA60
elif [ ${COL_NUM} = 8 ];then
    COLS=${LTE2LTEADJ_PCI}
    TABLE=LTE2LTEADJ_PCI
elif [ ${COL_NUM} = 9 ];then
    COLS=${LTE2LTEADJ}
    TABLE=LTE2LTEADJ
elif [ ${COL_NUM} = 10 ];then
    COLS=${LTE_MRO_DISTURB_PRETREATE60tmp}
    TABLE=LTE_MRO_DISTURB_PRETREATE60tmp
elif [ ${COL_NUM} = 11 ];then
    COLS=${lte2lteadj_pci}
    TABLE=lte2lteadj_pci
elif [ ${COL_NUM} = 12 ];then
    COLS=${LTE_MRO_DISTURB_PRETREATE60}
    TABLE=LTE_MRO_DISTURB_PRETREATE60
elif [ ${COL_NUM} = 13 ];then
    COLS=${grid_view}
    TABLE=grid_view
elif [ ${COL_NUM} = 14 ];then
    COLS=${LTE_MRS_DLBESTROW_GRID_ANA60}
    TABLE=LTE_MRS_DLBESTROW_GRID_ANA60
elif [ ${COL_NUM} = 15 ];then
    COLS=${LTE_MRO_OVERLAP_GRID_ANA60}
    TABLE=LTE_MRO_OVERLAP_GRID_ANA60
elif [ ${COL_NUM} = 16 ];then
    COLS=${GRID_LTEMRKPI60}
    TABLE=GRID_LTEMRKPI60
elif [ ${COL_NUM} = 17 ];then
    COLS=${CELL_LTEMRKPITEMP}
    TABLE=CELL_LTEMRKPITEMP
elif [ ${COL_NUM} = 18 ];then
    COLS=${LTE_MRO_OVERLAP_B_ANA60}
    TABLE=LTE_MRO_OVERLAP_B_ANA60
elif [ ${COL_NUM} = 19 ];then
    COLS=${CELL_LTENEWMRKPI60}
    TABLE=CELL_LTENEWMRKPI60
elif [ ${COL_NUM} = 20 ];then
    COLS=${CELL_LTEMRKPI60}
    TABLE=CELL_LTEMRKPI60
elif [ ${COL_NUM} = 21 ];then
    COLS=${LTE_MRO_DISTURB_SEC}
    TABLE=LTE_MRO_DISTURB_SEC
elif [ ${COL_NUM} = 22 ];then
    COLS=${lte_mro_disturb_ana}
    TABLE=lte_mro_disturb_ana
elif [ ${COL_NUM} = 23 ];then
    COLS=${lte_mro_disturb_mix}
    TABLE=lte_mro_disturb_mix
elif [ ${COL_NUM} = 24 ];then
    COLS=${lte_mro_adjcover_ana60}
    TABLE=lte_mro_adjcover_ana60
else
    COLS=${LTE_MRO_SOURCE_TMP}
    TABLE=LTE_MRO_SOURCE_TMP
fi

#sqoop
sqoop export --connect $URL \
--username ${USERNAME} \
--password ${PASSWD} \
--table ${TABLE} \
--columns ${COLS} \
--export-dir ${HDFS_DIR} \
--input-fields-terminated-by ',' \
--m ${MAP_NUM} \
--input-null-string '\\N' \
--input-null-non-string '\\N'