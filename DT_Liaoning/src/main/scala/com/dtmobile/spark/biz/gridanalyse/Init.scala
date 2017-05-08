package com.dtmobile.spark.biz.gridanalyse

import com.dtmobile.util.DBUtil
import org.apache.spark.sql.{SaveMode, SparkSession}
/**
  * Created by shenkaili on 17-5-8.
  */
class Init(ANALY_DATE: String,ANALY_HOUR: String,SDB: String, DDB: String, warhouseDir: String,ORCAL: String) {
  def analyse(implicit sparkSession: SparkSession): Unit = {
    mrfilter(sparkSession)

  }
  def mrfilter(sparkSession: SparkSession): Unit ={
    import sparkSession.sql
    sql(
      s"""
         |SELECT OBJECTID, VID, STARTTIME, ENDTIME, TIMESEQ
         |	, ENBID, MRNAME, CELLID, MMEUES1APID, MMEGROUPID
         |	, MMECODE, MEATIME, GRIDCENTERLONGITUDE, GRIDCENTERLATITUDE, KPI1
         |	, KPI2, KPI3, KPI4, KPI5, KPI6
         |	, KPI7, KPI8, KPI9, KPI10, KPI11
         |	, KPI12, KPI13, KPI14, KPI15, KPI16
         |	, KPI17, KPI18, KPI19, KPI20, KPI21
         |	, KPI22, KPI23, KPI24, KPI25, KPI26
         |	, KPI27, KPI28, KPI29
         |FROM lte_mro_source
         |WHERE dt = '$ANALY_DATE'
         |	AND h = '$ANALY_HOUR'
         |	AND mrname = 'MR.LteScRSRP'
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/t_xdr_event_msg/dt=$ANALY_DATE/h=$ANALY_HOUR")
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

}
