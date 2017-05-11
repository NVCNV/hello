package com.dtmobile.spark.biz.gridanalyse

import com.dtmobile.spark.SparkSessionSingleton
import com.dtmobile.util.DBUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
/**
  * Created by shenkaili on 17-5-8.
  */
class Init(ANALY_DATE: String,ANALY_HOUR: String,SDB: String, DDB: String, warhouseDir: String,ORCAL: String) {
  def analyse(implicit sparkSession: SparkSession): Unit = {
    mrfilter(sparkSession)

  }
  def mrfilter(sparkSession: SparkSession): Unit ={

    val CellDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@172.30.4.187:1521:morpho0307")
      .option("dbtable", "grid_view")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load().createOrReplaceTempView("grid_view")
    import sparkSession.sql
    sql(
      s"""
         |SELECT t1.OBJECTID, t1.VID, t1.STARTTIME, t1.ENDTIME, t1.meatime
         | , t1.ENBID, t1.MRNAME, t1.CELLID, t1.MMEUES1APID, t1.MMEGROUPID
         | , t1.MMECODE, t1.MEATIME, t2.shapeminx,t2.shapemaxy, t1.KPI1
         | , t1.KPI2, t1.KPI3, t1.KPI4, t1.KPI5, t1.KPI6
         | , t1.KPI7, t1.KPI8, t1.KPI9, t1.KPI10, t1.KPI11
         | , t1.KPI12, t1.KPI13, t1.KPI14, t1.KPI15, t1.KPI16
         | , t1.KPI17, t1.KPI18, t1.KPI19, t1.KPI20, t1.KPI21
         | , t1.KPI22, t1.KPI23, t1.KPI24, t1.KPI25, t1.KPI26
         | , t1.KPI27, t1.KPI28, t1.KPI29,t2.OBJECTID
         |FROM lte_mro_source t1 left join grid_view t2 WHERE t1.mrname = 'MR.LteScRSRP' and t1.GRIDCENTERLONGITUDE > t2.shapeminx and t1.GRIDCENTERLONGITUDE < t2.shapemaxx
         |            and t1.GRIDCENTERLATITUDE > t2.shapeminy
         |            and t1.GRIDCENTERLATITUDE < t2.shapemaxy and ((ROUND(t1.GRIDCENTERLONGITUDE,2) = t2.x
         |            and ROUND(t1.GRIDCENTERLATITUDE,2) =  t2.y)   or  (ROUND(t1.GRIDCENTERLONGITUDE,2) = t2.x1
         |            and ROUND(t1.GRIDCENTERLATITUDE,2) =  t2.y1)) and dt="$ANALY_DATE" and h="$ANALY_HOUR"
       """.stripMargin).createOrReplaceTempView("lte_mro_source_ana_tmp")
  }
  def lte2lteadj(sparkSession: SparkSession): Unit ={
    import sparkSession.sql
    sql(
      s"""
         |SELECT a.mmeGroupId, a.mmeId, a.eNodeBId, a.cellId, a.cellName
         |	, c1.pci, c1.freq1, a.adjMmeGroupId, a.adjMmeId, a.adjENodeBId
         |	, a.adjCellId, a.adjCellName, c2.pci, c2.freq1
         |FROM lte2lteadj a, ltecell c1, ltecell c2
         |WHERE a.cellId = c1.cellId
         |	AND a.eNodeBId = c1.eNodeBId
         |	AND a.adjCellId = c2.cellId
         |	AND a.adjENodeBId = c2.eNodeBId and dt="$ANALY_DATE" and h="$ANALY_HOUR"
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/lte2lteadj_pci/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }
  def InDoorAna(sparkSession: SparkSession): Unit ={
    import sparkSession.sql
    sql(
      s"""
         |SELECT MMEUES1APID, enbID, CELLID, MrCount, VARI_TA
         | , VARI_AOA, AVG_SRCRSRP, CASE WHEN MrCount >= 10
         | AND VARI_TA < 1
         | AND VARI_AOA < 1 THEN 1 END
         |FROM (SELECT MmeUeS1apId AS MMEUES1APID, enbID, cellID AS CELLID, COUNT(*) AS MrCount, VARIANCE(CASE WHEN kpi5 * 16 < 20512 THEN kpi5 * 16 / 16 END) AS VARI_TA
         |  , VARIANCE(CASE WHEN kpi7 >= 0 THEN kpi7 END) AS VARI_AOA, AVG(CASE WHEN kpi1 >= 0 THEN kpi1-14 END) AS AVG_SRCRSRP
         | FROM lte_mro_source_ana_tmp t
         | WHERE t.mmeues1apId > 0
         |  AND t.mrname = 'MR.LteScRSRP'
         |  AND vid = 0
         | GROUP BY t.enbID, t.cellid, t.mmeues1apId dt="$ANALY_DATE" and h="$ANALY_HOUR"
         | )t1
       """.stripMargin).createOrReplaceTempView("Mr_InDoorAna_Temp")
  }

}
