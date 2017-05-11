/**
  * Created by weiyaqin on 2017/5/2.
  */
package com.dtmobile.spark.biz.gridanalyse

import org.apache.spark.sql.{SaveMode, SparkSession}

class GridCover(ANALY_DATE: String,ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String) {

  var PoorCoverageRSRPTh:String ="-110"
  var sinrulth:String ="-3"
  var poorUEpowerTh:String ="3"
  var packetlossrateulth:String ="0.02"
  var packetlossratedlth:String ="0.02"
  var adjDisturbRSRP:String ="-10"
  var recovercount:String ="3"
  var adjstrongDisturb:String ="30"
  var receivedip:String ="-110"

  var PoorCoverageRSRPThOp:String ="<"
  var sinrulthOp:String ="<"
  var poorUEpowerThOp:String ="<"
  var packetlossrateulthOp:String =">"
  var packetlossratedlthOp:String =">"
  var adjDisturbRSRPOp:String ="="
  var recovercountOp:String ="="
  var adjstrongDisturbOp:String ="="
  var receivedipOp:String =">"

  var moduluservertoadjrsrpOp:String ="<"
  var moduluservertoadjrsrp:String ="6"
  var adjcellnumOp:String =">="
  var adjcellnum:String ="3"
  var servercelltoadjcellrsrplOp:String =">="
  var servercelltoadjcellrsrpl:String ="0"
  var GoodCoverageRSRPThOp:String =">="
  var GoodCoverageRSRPTh:String ="-80"
  var undefinedadjcellrsrpOp:String =">"
  var undefinedadjcellrsrp:String ="-110"
  var adjcellrsrpOp:String ="<"
  var adjcellrsrp:String ="-110"
  var servercelltoadjcellrsrphOp:String ="<"
  var servercelltoadjcellrsrph:String ="5"
  var undefinedrelationrsrpOp:String =">"
  var undefinedrelationrsrp:String ="3"

  def getqciKpi(i : Int) : String ={
    s"""SUM (CASE WHEN t.kpi$i >= 0 AND t.kpi$i <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN 1 ELSE 0 END)
        | """.stripMargin
  }

  def generatePlrulandPlrdlCount(plrulthArrOp:String,plrulthArr:String,plrdlthArrOp:String,plrdlthArr:String) : String ={
    var i = 11
    var kpi1 = ""
    var kpit = ""

    for( i <- 11 to 28){
      if (i < 19) {
        kpit = getqciKpi(i) + "+"
        kpi1 = kpi1 + kpit
      }
      else if (i == 19)
      {
        kpit = getqciKpi(i) + "as plrulCount,"
        kpi1 = kpi1 + kpit
      }
      else if (i < 28)
      {
        kpit = getqciKpi(i) + "+"
        kpi1 = kpi1 + kpit
      }
      else if (i == 28)
      {
        kpit = getqciKpi(i) + "as plrdlCount,"
        kpi1 = kpi1 + kpit
      }
    }
    val kpi2:String = s""" SUM(CASE WHEN t.kpi11 >= 0 AND t.kpi11 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi11 ELSE 0 END)+SUM (CASE WHEN t.kpi12 >= 0 AND t.kpi12 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi12 ELSE 0 END)+SUM (CASE WHEN t.kpi13 >= 0 AND t.kpi13 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi13 ELSE 0 END)+SUM (CASE WHEN t.kpi14 >= 0 AND t.kpi14 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi14 ELSE 0 END)+SUM (CASE WHEN t.kpi15 >= 0 AND t.kpi15 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi15 ELSE 0 END)+SUM (CASE WHEN t.kpi16 >= 0 AND t.kpi16 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi16 ELSE 0 END)+SUM (CASE WHEN t.kpi17 >= 0 AND t.kpi17 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi17 ELSE 0 END)+SUM (CASE WHEN t.kpi18 >= 0 AND t.kpi18 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi18 ELSE 0 END)+SUM (CASE WHEN t.kpi19 >= 0 AND t.kpi19 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi19 ELSE 0 END)as plrulthmrcount,SUM (CASE WHEN t.kpi20 >= 0 AND t.kpi20 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi20 ELSE 0 END)+SUM (CASE WHEN t.kpi21 >= 0 AND t.kpi21 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi21 ELSE 0 END)+SUM (CASE WHEN t.kpi22 >= 0 AND t.kpi22 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi22 ELSE 0 END)+SUM (CASE WHEN t.kpi23 >= 0 AND t.kpi23 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi23 ELSE 0 END)+SUM (CASE WHEN t.kpi24 >= 0 AND t.kpi24 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi24 ELSE 0 END)+SUM (CASE WHEN t.kpi25 >= 0 AND t.kpi25 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi25 ELSE 0 END)+SUM (CASE WHEN t.kpi26 >= 0 AND t.kpi26 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi26 ELSE 0 END)+SUM (CASE WHEN t.kpi27 >= 0 AND t.kpi27 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi27 ELSE 0 END)+SUM (CASE WHEN t.kpi28 >= 0 AND t.kpi28 <= 27 AND t.MRNAME = 'MR.LteScRIP0' THEN
                          | kpi28 ELSE 0 END) as plrdlthmrcount """.stripMargin

    kpi1 + kpi2
  }

  def getRip1_10KpiCOUNT(i:Int):String ={
    s"SUM (CASE WHEN t.kpi$i >= 0 AND t.MRNAME = 'MR.LteScRIP0' THEN 1 ELSE 0 END) "
  }

  def getRip1_10KpiSum(i:Int):String = {
    s"SUM (CASE WHEN t.kpi$i >= 0 and t.MRNAME = 'MR.LteScRIP0' THEN(0.1 * (t.kpi$i -1) -126) ELSE 0 END) "
  }

  def genLrToRealValue(i:Int):String = {
    s""" CASE WHEN t.mrname='MR.LteScRIP0' and t.kpi$i = 0 THEN 0
        | WHEN t.mrname='MR.LteScRIP0' and t.kpi$i = 1 THEN 2
        | WHEN t.mrname='MR.LteScRIP0' and t.kpi$i = 2 THEN 5
        | WHEN t.mrname='MR.LteScRIP0' and t.kpi$i >=3  AND t.kpi$i <=11 THEN (t.kpi$i - 3) * 10 + 10
        | WHEN t.mrname='MR.LteScRIP0' and t.kpi$i >=12 AND t.kpi$i <=16 THEN (t.kpi$i - 12)* 20 + 100
        | WHEN t.mrname='MR.LteScRIP0' and t.kpi$i >=17 AND t.kpi$i <=22 THEN (t.kpi$i - 17)* 50 + 200
        | WHEN t.mrname='MR.LteScRIP0' and t.kpi$i >=23 AND t.kpi$i <=27 THEN (t.kpi$i - 23) *100 + 500 END)
        | """.stripMargin
  }

  def generateMrRipKpi(plrulthArrOp:String,plrulthArr:String,plrdlthArrOp:String,plrdlthArr:String,
                       receivedipOp:String,receivedip:String):String ={
    var i = 1
    var kpi1_10count = ""
    var kpi11_28count = ""
    var kpi1_10countsum = ""
    var kpi1_10sum = ""
    var kpi1_10thmrcount = "sum("
    var kpi11_19countsum = ""
    var kpi11_19sum = ""
    var kpi20_28countsum = ""
    var kpi20_28sum = ""
    var kpi11_19phCount = "sum("
    var kpi20_28phCount = "sum("

    var v_sql = ""

    for (i <- 1 to 10) {
      kpi1_10count = kpi1_10count + getRip1_10KpiCOUNT(i) + "," + getRip1_10KpiSum(i) + ","
    }

    for (i <- 11 to 28) {
      kpi11_28count = kpi11_28count + getqciKpi(i) + ", sum(" + genLrToRealValue(i) + ","
    }

    for (i <- 1 to 10) {
      if (i != 10) {
        kpi1_10countsum = kpi1_10countsum + getRip1_10KpiCOUNT(i) + "+"
        kpi1_10sum = kpi1_10sum + getRip1_10KpiSum(i) + "+"
        kpi1_10thmrcount = kpi1_10thmrcount + s"case when kpi$i $receivedipOp 10 * ($receivedip+126) +1 and MRNAME = 'MR.LteScRIP0 ' THEN 1 ELSE 0 END + "
      }else{
        kpi1_10countsum = kpi1_10countsum + getRip1_10KpiCOUNT(i) + ","
        kpi1_10sum = kpi1_10sum + getRip1_10KpiSum(i) + ","
        kpi1_10thmrcount = kpi1_10thmrcount + s"case when kpi$i $receivedipOp 10 *($receivedip + 126) + 1 and MRNAME = 'MR.LteScRIP0' THEN 1 ELSE 0 END),"
      }
    }

    for ( i <- 11 to 19){
      if (i != 19){
        kpi11_19countsum = kpi11_19countsum + getqciKpi(i) + "+"
        kpi11_19sum = kpi11_19sum + "sum(" + genLrToRealValue(i) + "+"
        kpi11_19phCount= kpi11_19phCount + "case when(" + genLrToRealValue(i) + plrulthArrOp + "1000 * " + plrulthArr + " THEN 1 else 0 end +"
      }else{
        kpi11_19countsum = kpi11_19countsum + getqciKpi(i) + ","
        kpi11_19sum = kpi11_19sum + "sum(" + genLrToRealValue(i) + ","
        kpi11_19phCount= kpi11_19phCount + "case when(" + genLrToRealValue( i) + plrulthArrOp + "1000 * " + plrulthArr + " THEN 1 else 0 end),"
      }
    }

    for ( i <- 20 to 28){
      if (i != 28){
        kpi20_28countsum = kpi20_28countsum + getqciKpi(i) + "+"
        kpi20_28sum = kpi20_28sum + "sum(" + genLrToRealValue(i) + "+"
        kpi20_28phCount = kpi20_28phCount + "case when(" + genLrToRealValue( i) + plrulthArrOp + "1000 * " + plrulthArr + " THEN 1 else 0 end +"
      }else{
        kpi20_28countsum = kpi20_28countsum + getqciKpi(i) + ","
        kpi20_28sum = kpi20_28sum + "sum(" + genLrToRealValue(i) + ","
        kpi20_28phCount = kpi20_28phCount + "case when(" + genLrToRealValue( i) + plrulthArrOp + "1000 * " + plrulthArr + " THEN 1 else 0 end)"
      }
    }

    s"""$kpi1_10count $kpi11_28count $kpi1_10countsum $kpi1_10sum $kpi11_19countsum $kpi11_19sum $kpi20_28countsum
       | $kpi20_28sum $kpi1_10thmrcount $kpi11_19phCount $kpi20_28phCount""".stripMargin

  }

  def analyse(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.sql

    sql(s"use $DDB")
    //    |insert into lte_mrs_dlbestrow_grid_ana60(oid,starttime,endtime,timeseq,enodebid,cellid,gridcenterlongitude,gridcenterlatitude,
    //    | usercount,idrUserCount,rsrpsum,idrRsrpsum,rsrpcount,idrRsrpcount,weakbestrowmrcount,idrWEAKBESTROWMRCOUNT,powerheadroomtotalcount,powerheadroomlowmrcount)
    sql(s"""alter table lte_mrs_dlbestrow_grid_ana60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/lte_mrs_dlbestrow_grid_ana60/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
    sql(
      s"""
         | SELECT t1.oid,t1.STARTTIME,t1.endtime,t1.TIMESEQ,t1.ENBID,t1.CELLID,t1.GRIDCENTERLONGITUDE,t1.GRIDCENTERLATITUDE,
         | count(DISTINCT CASE WHEN t1.MMEUES1APID>0 THEN t1.MMEUES1APID END)USERCOUNT,
         | count(DISTINCT CASE WHEN t1.MMEUES1APID>0 and t2.InDoorFlag = 1 THEN t1.MMEUES1APID END) idrUSERCOUNT,
         | SUM(case when t1.kpi1 >= 0 then t1.kpi1-141 else 0 end) RSRPSUM,
         | SUM(case when t2.InDoorFlag = 1 and t1.kpi1 >= 0 then t1.kpi1-141 else 0 end) dirRSRPSUM,
         | SUM(case when t1.kpi1 >= 0 then 1 else 0 end) RSRPTOTALCOUNT,
         | SUM(case when t2.InDoorFlag = 1 and t1.kpi1 >= 0 then 1 else 0 end) idrRSRPTOTALCOUNT,
         | SUM(CASE WHEN t1.kpi1 -141
         | $PoorCoverageRSRPThOp $PoorCoverageRSRPTh and t1.kpi1 >=0 THEN 1 ELSE 0 END) poolcovercount,
         | SUM(CASE WHEN t2.InDoorFlag = 1 and t1.kpi1-141
         | $PoorCoverageRSRPThOp $PoorCoverageRSRPTh and t1.kpi1 >=0 THEN 1 ELSE 0 END) idrpoolcovercount,
         | sum(CASE WHEN t1.kpi6>=0 then 1 else 0 end) as phrtotalcount,
         | sum(CASE WHEN t1.kpi6 -23 $poorUEpowerThOp $poorUEpowerTh and t1.kpi6>=0 THEN 1 ELSE 0 END) poorUEpowerCount FROM
         | lte_mro_source_ana_tmp t1,Mr_InDoorAna_Temp t2 WHERE  t1.vid = 0 and t1.mrname = 'MR.LteScRSRP'
         | and t1.cellid = t2.cellid and t1.oid>0
         | and t1.MMEUES1APID = t2.MMEUES1APID
         | GROUP BY t1.oid,t1.starttime,t1.endtime,t1.timeseq,t1.enbid,t1.CELLID,t1.GRIDCENTERLONGITUDE,t1.GRIDCENTERLATITUDE
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/lte_mrs_dlbestrow_grid_ana60/dt=$ANALY_DATE/h=$ANALY_HOUR")

    //    | insert  into lte_mro_overlap_grid_ana60(oid,starttime,endtime,timeseq,enodebid,cellid,gridcenterlongitude,gridcenterLatitude,usercount,overlapbestrowcellcount,adjacentareainterferenceintens,rsrqcount,rsrqsum,celloverlapbestrowmrcount,rsrpcount,rsrpsum)
    sql(s"""alter table lte_mro_overlap_grid_ana60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/lte_mro_overlap_grid_ana60/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
    sql(
      s"""
         | SELECT A.oid,A.STARTTIME,A.endtime,A.timeseq,A.ENBID,A.CELLID,A.GRIDCENTERLONGITUDE,A.GRIDCENTERLATITUDE,A.USERCOUNT,B.OVERLAPBESTROWCELLCOUNT,B.ADJDISTURDEGREE,A.RSRQCOUNT,A.RSRQSUM,C.CELLOVERLAPBESTROWMRCOUNT,A.RSRPCOUNT,A.RSRPSUM FROM(
         | SELECT oid,STARTTIME,endtime,timeseq,CELLID,ENBID,GRIDCENTERLONGITUDE,GRIDCENTERLATITUDE,COUNT (DISTINCT CASE WHEN MMEUES1APID > 0 THEN MMEUES1APID END) USERCOUNT,
         | SUM (case when kpi1 >=0 then 1 else 0 end) RSRPCOUNT,COUNT (case when kpi3>=0 then 1 else 0 end) RSRQCOUNT,
         | SUM(case when kpi1 >=0 then kpi1-141 else 0 end) RSRPSUM,SUM(case when kpi3>=0 then (0.5*kpi3 - 20) else 0 end) RSRQSUM
         | FROM
         | lte_mro_source_ana_tmp WHERE  MRNAME = 'MR.LteScRSRP' and oid >0
         | GROUP BY oid,STARTTIME,endtime,timeseq,ENBID,CELLID,GRIDCENTERLONGITUDE,GRIDCENTERLATITUDE
         | )A left join (
         | SELECT
         | t.oid,STARTTIME,endtime,timeseq,ENBID,CELLID,SUM (DISTRUBCOUNT) ADJDISTURDEGREE,SUM (OVERCELLCOUNT + 1) OVERLAPBESTROWCELLCOUNT
         | FROM
         | ( SELECT oid,STARTTIME,endtime,timeseq,ENBID,CELLID,COUNT (*) OVERCELLCOUNT,
         | SUM (POWER(10,(kpi2 - kpi1)/10))/POWER (10,$adjDisturbRSRP/ 10) DISTRUBCOUNT
         | FROM lte_mro_source_ana_tmp
         | WHERE  (kpi2 - kpi1) >= $adjDisturbRSRP and oid>0
         | AND MRNAME = 'MR.LteScRSRP' AND kpi1>=0  AND kpi2>=0
         | GROUP BY oid,STARTTIME,endtime,timeseq,ENBID,CELLID
         | ) T
         | GROUP BY t.oid,STARTTIME,endtime,timeseq,ENBID,CELLID
         | )B on A.cellid = B.cellid and A.oid = B.oid  left join(
         | SELECT s.oid,STARTTIME,endtime,timeseq,ENBID,CELLID,COUNT(*) CELLOVERLAPBESTROWMRCOUNT
         | FROM
         | (SELECT oid,STARTTIME,endtime,timeseq,ENBID,CELLID,COUNT (*) OVERCELLCOUNT,
         | SUM (POWER(10,(kpi2 - kpi1)/10)) / POWER(10, $adjDisturbRSRP/10) DISTRUBCOUNT
         | FROM lte_mro_source_ana_tmp
         | WHERE oid >0
         | AND (kpi2 - kpi1) > $adjDisturbRSRP AND MRNAME = 'MR.LteScRSRP'
         | AND kpi1>=0  AND kpi2>=0  GROUP BY oid,STARTTIME,endtime,timeseq,ENBID,CELLID,MMEUES1APID
         | ) S
         | WHERE DISTRUBCOUNT > $adjstrongDisturb AND (OVERCELLCOUNT + 1) > $recovercount
         | GROUP BY oid,STARTTIME,endtime,timeseq,ENBID,CELLID
         | )C on A.cellid=C.cellid and A.oid=C.oid
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/lte_mro_overlap_grid_ana60/dt=$ANALY_DATE/h=$ANALY_HOUR")

    //    | insert all into GRID_LTEMRKPI60(begintime,endtime,timeseq,
    //    | enodebid,cellid,gridcenterlongitude,gridcenterlatitude,oid,KPI1049,KPI1239,KPI1011,KPI1012,KPI1241,KPI1243,KPI1245,KPI1247)
    val PlrulandPlrdlSql:String = generatePlrulandPlrdlCount(packetlossrateulthOp,packetlossrateulth,packetlossratedlthOp,packetlossratedlth)
    sql(s"""alter table GRID_LTEMRKPI60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/GRID_LTEMRKPI60/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
    sql(
      s"""
         | select t.startTime, t.endTime, t.timeseq,t.ENBID,t.CELLID,t.GRIDCENTERLONGITUDE,t.GRIDCENTERLATITUDE,t.oid,
         | SUM(case when t.kpi8 >=0 and t.MRNAME='MR.LteScRSRP' then 1 else 0 end) SinrULCount,
         | SUM(case when t.kpi8 >=0 and t.kpi8-11 $sinrulthOp $sinrulth and t.MRNAME='MR.LteScRSRP' then 1 else 0 end) Sinulthmrcount,
         | SUM(case when t.kpi6>=0 and t.MRNAME='MR.LteScRSRP'then 1 else 0 end) PHRCount,
         | sum(case when t.kpi6>=0 and t.kpi6-23 $poorUEpowerThOp $poorUEpowerTh and t.MRNAME='MR.LteScRSRP'then 1 else 0 end) PHRthmrCount,
         | $PlrulandPlrdlSql FROM lte_mro_source_ana_tmp t where  vid = 0 and t.oid >0 GROUP BY t.starttime,t.endtime,t.timeseq,
         | t.ENBID,t.oid,t.CELLID,t.GRIDCENTERLONGITUDE,t.GRIDCENTERLATITUDE
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/GRID_LTEMRKPI60/dt=$ANALY_DATE/h=$ANALY_HOUR")

    //  |insert into CELL_LTEMRKPITEMP(begintime,endtime,timeseq,
    //  |        enodebid,cellid,KPI1001,KPI1002,KPI1003,KPI1004,KPI1005,KPI1006,KPI1011,KPI1012,KPI1009,KPI1010,KPI1049,
    //  |  KPI1050,KPI1119,KPI1120,KPI1123,KPI1124,KPI1127,KPI1128,KPI1129,KPI1130,KPI1131,KPI1132,KPI1133,KPI1134,KPI1135,KPI1136,KPI1137,
    //  |  KPI1138,KPI1239,KPI1249,KPI1250,KPI1251,KPI1252,KPI1253,KPI1254,
    //  |  KPI1121,KPI1122,KPI1125,KPI1126,KPI1183,KPI1184,KPI1189,KPI1190,
    //  |  KPI1195,KPI1196,KPI1197,KPI1198,KPI1199,KPI1200,KPI1201,KPI1202,
    //  |  KPI1203,KPI1204,KPI1205,KPI1206,KPI1207,KPI1208,KPI1209,KPI1210,
    //  |  KPI1211,KPI1212,KPI1213,KPI1214,KPI1013,KPI1014,KPI1015,KPI1016,
    //  |  KPI1017,KPI1018,KPI1019,KPI1020,KPI1021,KPI1022,KPI1023,KPI1024,
    //  |  KPI1025,KPI1026,KPI1027,KPI1028,KPI1029,KPI1030,KPI1031,KPI1032,
    //  |  KPI1033,KPI1034,KPI1035,KPI1036,KPI1037,KPI1038,KPI1039,KPI1040,
    //  |  KPI1041,KPI1042,KPI1043,KPI1044,KPI1045,KPI1046,KPI1047,KPI1048,
    //  |  KPI1007,KPI1008,KPI1241,KPI1242,KPI1245,KPI1246,KPI1237,KPI1243,KPI1247)
    val MrRipKpiSql:String = generateMrRipKpi(packetlossrateulthOp,packetlossrateulth, packetlossratedlthOp,packetlossratedlth,receivedipOp,receivedip)
    sql(s"""alter table CELL_LTEMRKPITEMP add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/CELL_LTEMRKPITEMP/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
    sql(
      s"""
         | select t.startTime, t.endTime, t.timeseq,t.ENBID,t.CELLID,
         | SUM (CASE WHEN t.kpi1>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS kpi1count,
         | SUM (CASE WHEN t.kpi1>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN t .kpi1 - 141 END) AS kpi1sum,
         | SUM (CASE WHEN t.kpi3>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS kpi3count,
         | SUM (CASE WHEN t.kpi3>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 0.5*t.kpi3 - 20 END) AS kpi3sum,
         | SUM (CASE WHEN t.kpi5>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS kpi5count,
         | SUM (CASE WHEN t.kpi5>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 16 * t.kpi5 END) AS kpi5sum,
         | SUM (CASE WHEN t.kpi6>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS kpi6count,
         | SUM (CASE WHEN t.kpi6>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN t.kpi6 - 23 END) AS kpi6sum,
         | SUM (CASE WHEN t.kpi7>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS kpi7count,
         | SUM (CASE WHEN t.kpi7>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 0.5 * t.kpi7 END) AS kpi7sum,
         | SUM (CASE WHEN t.kpi8>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS kpi8count,
         | SUM (CASE WHEN t.kpi8>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN t.kpi8 - 11 END) AS kpi8sum,
         | SUM (CASE WHEN t.kpi20>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS kpi20count,
         | SUM (CASE WHEN t.kpi20>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN (t.kpi20 - 31)*16 END) AS kpi20sum,
         | SUM (CASE WHEN t.kpi21>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS kpi21count,
         | SUM (CASE WHEN t.kpi21>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN t.kpi21 END) AS kpi21sum,
         | SUM (CASE WHEN t.kpi22>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS kpi22count,
         | SUM (CASE WHEN t.kpi22>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN t.kpi22 END) AS kpi22sum,
         | SUM (CASE WHEN t.kpi23>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS kpi23count,
         | SUM (CASE WHEN t.kpi23>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN t.kpi23 END) AS kpi23sum,
         | SUM (CASE WHEN t.kpi24>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS kpi24count,
         | SUM (CASE WHEN t.kpi24>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN t.kpi24 END) AS kpi24sum,
         | SUM (CASE WHEN t.kpi25>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS kpi25count,
         | SUM (CASE WHEN t.kpi25>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN t.kpi25 END) AS kpi25sum,
         | SUM (CASE WHEN t.kpi26>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS kpi26count,
         | SUM (CASE WHEN t.kpi26>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN t.kpi26 END) AS kpi26sum,
         | SUM (CASE WHEN t.kpi27>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS kpi27count,
         | SUM (CASE WHEN t.kpi27>=0 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN t.kpi27 END) AS kpi27sum,
         | SUM (CASE WHEN t.kpi8>=0 and t.kpi8 -11 < -3 and t.MRNAME = 'MR.LteScRSRP' and VID =0 THEN 1 END) AS sinulthmrcount,
         | MAX (CASE WHEN t.MRNAME = 'MR.LteScRSRP' then t.kpi1-141 end) AS maxkpi1,
         | MIN (CASE WHEN t.MRNAME = 'MR.LteScRSRP' then t.kpi1-141 end) AS minkpi1,
         | MAX (CASE WHEN t.kpi3 >=0 and t.MRNAME = 'MR.LteScRSRP' then 0.5 * t.kpi3 - 20 end) AS maxkpi3,
         | MIN (CASE WHEN t.kpi3 >=0 and t.MRNAME = 'MR.LteScRSRP' then 0.5 * t.kpi3 - 20 end) AS minkpi3,
         | MAX (CASE WHEN t.kpi5 >=0 and t.MRNAME = 'MR.LteScRSRP' then t.kpi5 * 16 end) AS maxkpi5,
         | MIN (CASE WHEN t.kpi5 >=0 and t.MRNAME = 'MR.LteScRSRP' then t.kpi5 * 16 end) AS minkpi5,
         | SUM (CASE WHEN t.kpi2>0 and t.MRNAME = 'MR.LteScRSRP' THEN 1 ELSE 0 END) AS kpi2count,
         | SUM (CASE WHEN t.kpi2>0 and t.MRNAME = 'MR.LteScRSRP' THEN t.kpi2-141 ELSE 0 END) AS kpi2sum,
         | SUM (CASE WHEN t.kpi4>=0 and t.MRNAME = 'MR.LteScRSRP' THEN 1 ELSE 0 END) AS kpi4count,
         | SUM (CASE WHEN t.kpi4>=0 and t.MRNAME = 'MR.LteScRSRP' THEN 0.5 * t.kpi4 - 20 ELSE 0 END) AS kpi4sum,
         | SUM (CASE WHEN t.kpi14>=0 and t.MRNAME = 'MR.LteScRSRP' THEN 1 ELSE 0 END) AS kpi14count,
         | SUM (CASE WHEN t.kpi14>=0 and t.MRNAME = 'MR.LteScRSRP' THEN t.kpi14 ELSE 0 END) AS kpi14sum,
         | SUM (CASE WHEN t.kpi17>=0 and t.MRNAME = 'MR.LteScRSRP' THEN 1 ELSE 0 END) AS kpi17count,
         | SUM (CASE WHEN t.kpi17>=0 and t.MRNAME = 'MR.LteScRSRP' THEN t.kpi17 ELSE 0 END) AS kpi17sum,$MrRipKpiSql
         | FROM lte_mro_source_ana_tmp t GROUP BY t.starttime,t.endtime,t.timeseq,t.ENBID
         | ,t.CELLID
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/CELL_LTEMRKPITEMP/dt=$ANALY_DATE/h=$ANALY_HOUR")

//    insert all into CELL_LTEMRKPI60(begintime,endtime,timeseq,
//      enodebid,cellid,KPI1001,KPI1002,KPI1003,KPI1004,KPI1005,KPI1006,KPI1011,KPI1012,KPI1009,KPI1010,KPI1049,
//      KPI1050,KPI1119,KPI1120,KPI1123,KPI1124,KPI1127,KPI1128,KPI1129,KPI1130,KPI1131,KPI1132,KPI1133,KPI1134,KPI1135,KPI1136,KPI1137,
//      KPI1138,KPI1239,KPI1249,KPI1250,KPI1251,KPI1252,KPI1253,KPI1254,
//      KPI1121,KPI1122,KPI1125,KPI1126,KPI1183,KPI1184,KPI1189,KPI1190,
//      KPI1195,KPI1196,KPI1197,KPI1198,KPI1199,KPI1200,KPI1201,KPI1202,
//      KPI1203,KPI1204,KPI1205,KPI1206,KPI1207,KPI1208,KPI1209,KPI1210,
//      KPI1211,KPI1212,KPI1213,KPI1214,KPI1013,KPI1014,KPI1015,KPI1016,
//      KPI1017,KPI1018,KPI1019,KPI1020,KPI1021,KPI1022,KPI1023,KPI1024,
//      KPI1025,KPI1026,KPI1027,KPI1028,KPI1029,KPI1030,KPI1031,KPI1032,
//      KPI1033,KPI1034,KPI1035,KPI1036,KPI1037,KPI1038,KPI1039,KPI1040,
//      KPI1041,KPI1042,KPI1043,KPI1044,KPI1045,KPI1046,KPI1047,KPI1048,
//      KPI1007,KPI1008,KPI1241,KPI1242,KPI1245,KPI1246,KPI1237,KPI1243,KPI1247)
    sql(s"""alter table CELL_LTEMRKPI60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/CELL_LTEMRKPI60/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
    sql(
      s"""
         | select begintime,endtime,timeseq,enodebid,cellid,KPI1001,KPI1002,KPI1003,KPI1004,KPI1005,KPI1006,KPI1011,KPI1012,KPI1009,KPI1010,KPI1049,
         |  KPI1050,KPI1119,KPI1120,KPI1123,KPI1124,KPI1127,KPI1128,KPI1129,KPI1130,KPI1131,KPI1132,KPI1133,KPI1134,KPI1135,KPI1136,KPI1137,
         |  KPI1138,KPI1239,KPI1249,KPI1250,KPI1251,KPI1252,KPI1253,KPI1254,
         |  KPI1121,KPI1122,KPI1125,KPI1126,KPI1183,KPI1184,KPI1189,KPI1190,
         |  KPI1195,KPI1196,KPI1197,KPI1198,KPI1199,KPI1200,KPI1201,KPI1202,
         |  KPI1203,KPI1204,KPI1205,KPI1206,KPI1207,KPI1208,KPI1209,KPI1210,
         |  KPI1211,KPI1212,KPI1213,KPI1214,KPI1013,KPI1014,KPI1015,KPI1016,
         |  KPI1017,KPI1018,KPI1019,KPI1020,KPI1021,KPI1022,KPI1023,KPI1024,
         |  KPI1025,KPI1026,KPI1027,KPI1028,KPI1029,KPI1030,KPI1031,KPI1032,
         |  KPI1033,KPI1034,KPI1035,KPI1036,KPI1037,KPI1038,KPI1039,KPI1040,
         |  KPI1041,KPI1042,KPI1043,KPI1044,KPI1045,KPI1046,KPI1047,KPI1048,
         |  KPI1007,KPI1008,KPI1241,KPI1242,KPI1245,KPI1246,KPI1237,KPI1243,KPI1247 from CELL_LTEMRKPITEMP
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/CELL_LTEMRKPI60/dt=$ANALY_DATE/h=$ANALY_HOUR")

    //    | insert  into LTE_MRO_OVERLAP_B_ANA60(STARTTIME, ENDTIME, TIMESEQ,ENODEBID, CELLID,
    //   | USERCOUNT, RSRPSUM, RSRPCOUNT, RSRPAVG, OVERLAPBESTROWCELLCOUNT,  ADJACENTAREAINTERFERENCEINTENS,
    //   | ADJACENTAREAINTERFERENCEINDEX, CELLOVERLAPBESTROWRATIO, CELLOVERLAPBESTROWMRCOUNT, RSRQSUM, RSRQCOUNT, RSRQAVG)
    sql(s"""alter table LTE_MRO_OVERLAP_B_ANA60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/LTE_MRO_OVERLAP_B_ANA60/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
    sql(
      s"""
         | select T1.startTime,T1.endTime,T1.timeseq,T1.ENBID,T1.CELLID,T1.COUNT,T1.rsrpsum,T1.rsrpcount,
         | round((case when T1.rsrpcount=0 then NULL else T1.rsrpsum/T1.rsrpcount end),4),T2.overcellcount,T2.distrubcount,
         | round((case when T2.overcellcount=0 then NULL else T2.distrubcount/T2.overcellcount end),4),
         | round((case when T1.rsrpcount=0 then NULL else T3.COUNT/T1.rsrpcount end),4),T3.COUNT,T1.rsrqsum,T1.rsrpcount,
         | round((case when T1.rsrpcount=0 then NULL else T1.rsrqsum/T1.rsrpcount end),4)  from
         | (SELECT T .startTime,T .endTime,T.timeseq,T .CELLID,T .ENBID,COUNT (DISTINCT MMeUeS1apId) AS COUNT,
         | SUM (CASE WHEN T .kpi1 >= 0 THEN 1 ELSE  0 END) rsrpcount,
         | SUM (CASE WHEN T .kpi1 >= 0 THEN T .kpi1 - 141 ELSE 0 END) rsrpsum,
         | SUM (CASE WHEN T .kpi3 >= 0 THEN 0.5 * (T .kpi3 - 1) - 19.5 ELSE 0 END) rsrqsum
         | FROM(SELECT * FROM lte_mro_source_ana_tmp A WHERE A.MRNAME = 'MR.LteScRSRP' AND A.VID = 0) T
         | GROUP BY T.startTime,T.endTime,T.timeseq,T.CELLID,T.ENBID) T1
         | left join
         | (SELECT T.CELLID,T .ENBID,
         | SUM(T.distrubcount / POWER (10, $adjDisturbRSRP/10)) distrubcount ,SUM (T.overcellcount + 1) overcellcount
         | FROM(SELECT cellId,enbId,mmecode,mmeues1apId,mmegroupId,meaTime,COUNT (*) AS overcellcount,
         | SUM (POWER (10,(A .kpi2 - A.kpi1) / 10)) AS distrubcount
         | FROM lte_mro_source_ana_tmp A where (A.kpi2 - A.kpi1) > $adjDisturbRSRP AND A.MRNAME = 'MR.LteScRSRP'
         | AND A.kpi2 >= 0 AND A.kpi1 >= 0 AND A.kpi9 = A.kpi11
         | GROUP BY A.CELLID,A .ENBID,A.MMECODE,A.MMeUeS1apId,A.MmeGroupId,A.meaTime) T
         | GROUP BY T.CELLID,T.ENBID) T2
         | on T1.CELLID = T2.CELLID and T1.ENBID = T2.ENBID
         | left join
         | (SELECT T.CELLID,T.ENBID,COUNT (*) AS COUNT
         | FROM(SELECT cellId,enbId,mmecode,mmeues1apId,mmegroupId,meaTime,COUNT (*) AS overcellcount,
         | SUM (POWER (10,(A .kpi2 - A .kpi1) / 10)) AS distrubcount
         | FROM lte_mro_source_ana_tmp A
         | WHERE (A .kpi2 - A .kpi1) > $adjDisturbRSRP AND A .MRNAME = 'MR.LteScRSRP' AND A .kpi2 >= 0 AND A .kpi1 >= 0 AND A .kpi9 = A .kpi11
         | GROUP BY A .CELLID,A .ENBID,A .MMECODE,A .MMeUeS1apId,A .MmeGroupId,A .meaTime) T
         | WHERE (T .distrubcount / POWER (10, $adjDisturbRSRP/10)) > $adjstrongDisturb AND (T.overcellcount + 1) > $recovercount
         | GROUP BY T .CELLID,T .ENBID) T3
         | on T1.CELLID = T3 .CELLID and T1.ENBID = T3.ENBID
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/LTE_MRO_OVERLAP_B_ANA60/dt=$ANALY_DATE/h=$ANALY_HOUR")

//    |INSERT INTO CELL_LTENEWMRKPI60 (STARTTIME,ENDTIME,TIMESEQ,MMEGROUPID,MMEID,ENODEBID,CELLID,MrOverlayCount,MrOverCoverCount,MrLoseNeibCount,MrEdgeWeakCoverCount)
    sql(s"""alter table CELL_LTENEWMRKPI60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/CELL_LTENEWMRKPI60/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
    sql(
      s"""
         |	 SELECT T.startTime,T.endTime,T.timeseq, T.mmegroupid,T.mmecode,T.enbid,T.cellid,
         |	 (case when (SUM (CASE WHEN (t.kpi1>=0 and t.kpi2 >=0 AND ABS(t.kpi1 - t.kpi2) $moduluservertoadjrsrpOp $moduluservertoadjrsrp) then 1 ELSE  0
         | END) $adjcellnumOp $adjcellnum) then SUM (CASE WHEN (t.kpi1>=0 and t.kpi2 >=0 AND ABS(t.kpi1 - t.kpi2) $moduluservertoadjrsrpOp $moduluservertoadjrsrp) then 1 ELSE  0
         | END) else 0 end) as MrOverlayCount,
         |	 SUM (CASE WHEN (t.kpi1>=0 and t.kpi2 >=0 AND t.kpi1 - t.kpi2 $servercelltoadjcellrsrplOp $servercelltoadjcellrsrpl
         | AND t.kpi1 - t.kpi2 $servercelltoadjcellrsrphOp $servercelltoadjcellrsrph and T .kpi1 - 141 $GoodCoverageRSRPThOp $GoodCoverageRSRPTh
         | ) THEN 1  ELSE 0  END) AS MrOverCoverCount,
         |	 SUM(  CASE WHEN (T.kpi1-141 $PoorCoverageRSRPThOp $PoorCoverageRSRPTh and (T4.cellid is null or (T5.cellid is null and T.tcellid is not null))
         | AND t .kpi2-141 $undefinedadjcellrsrpOp $undefinedadjcellrsrp AND t.kpi1 >=0 AND t.kpi2>=0
         | AND - 1 * (t.kpi1 - t.kpi2) $undefinedrelationrsrpOp $undefinedrelationrsrp) THEN  1  ELSE  0 END) AS MrLoseNeibCount,
         | SUM(CASE WHEN (T.kpi1 - 141 $PoorCoverageRSRPThOp $PoorCoverageRSRPTh AND (T .kpi2 -141 $adjcellrsrpOp $adjcellrsrp or T .kpi2 < 0)
         | ) THEN 1 ELSE 0 END ) AS MrEdgeWeakCoverCount FROM
         | (select T1.startTime,T1.endTime,T1.timeseq,T1.mmegroupid,T1.mmecode,T1.enbid,T1.cellId,T1.kpi1,T1.kpi2,T2.adjCellId as tcellid from
         | (select * from lte_mro_source_ana_tmp l where l.mrname='MR.LteScRSRP') T1 left join lte2lteadj_pci T2
         | on T1.cellId = T2.cellid and T2.adjpci = T1.kpi12 and T2.adjfreq1 = T1.kpi11) T
         | left join (select distinct cellid from lte2lteadj) T4
         | on T.cellid=T4.cellid left join lte2lteadj T5 on T.cellid=T5.cellid and T.tcellid=T5.adjcellid
         | GROUP BY T.startTime,T.endTime,T.timeseq,T.mmegroupid,T.mmecode,T.enbid,T.cellId
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/CELL_LTENEWMRKPI60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }
}
