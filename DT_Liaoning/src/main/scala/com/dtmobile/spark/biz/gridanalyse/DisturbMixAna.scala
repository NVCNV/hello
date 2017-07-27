package com.dtmobile.spark.biz.gridanalyse

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhangchao15 on 2017/4/28.
  */
class DisturbMixAna(ANALY_DATE: String, ANALY_HOUR: String, period: String, anahour: String, SDB: String, DDB: String, warhouseDir: String) {
  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"
  val cal_date2 = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR.toInt+1) + ":00:00"
  def analyse(implicit HiveContext: HiveContext): Unit = {
    disturbMixAna(HiveContext)
  }


  def disturbMixAna(HiveContext: HiveContext): Unit ={
    import HiveContext.sql
    sql(s"use $DDB")
    sql(s"""alter table lte_mro_disturb_mix add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
           LOCATION 'hdfs://dtcluster/$warhouseDir/lte_mro_disturb_mix/dt=$ANALY_DATE/h=$ANALY_HOUR'
      """)



    sql(s"""
           |SELECT
           '$cal_date' as starttime,
         '$cal_date2' as endtime,
         ${period} as period,
         $ANALY_HOUR as timeseq ,
           |M.TMMEGROUPID,
           |M.TMMEID,
           |M.TENODEBID,
           |M.TCELLID,
           |M.TCellName,
           |M.tcellpci,
           |M.tcellfreq,
           |M.MMEGROUPID,
           |M.MMEID,
           |M.ENODEBID,
           |M.CELLID,
           |M.CellName,
           |M.cellpci,
           |M.cellfreq,
           |M.disturbMrNum,
           |'' as col1,
           |'' as col2,
           |M.TCELLRSRPSum,
           |M.TCELLRSRPCount,
           |CASE
           |WHEN sign(M.TCELLRSRPCount) = 1 THEN M.TCELLRSRPSum / M.TCELLRSRPCount
           |ELSE 0
           |END
           |from LTE_MRS_OVERCOVER_TEMP M
           |where CELLFREQ = TCELLFREQ
      """.stripMargin).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", "false").save (s"$warhouseDir/lte_mro_disturb_mix/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }
}
