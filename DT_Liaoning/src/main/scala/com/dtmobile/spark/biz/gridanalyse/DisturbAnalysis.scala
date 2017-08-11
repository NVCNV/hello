package com.dtmobile.spark.biz.gridanalyse

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by zhangchao15 on 2017/5/3.
  */
class DisturbAnalysis(ANALY_DATE: String, ANALY_HOUR: String,  period: String, anahour: String, SDB: String, DDB: String, warhouseDir: String) {

  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"
  val cal_date2 = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR.toInt+1) + ":00:00"

  def analyse(implicit SparkSession: SparkSession): Unit = {
    disturbAnalysis(SparkSession)

  }

  def disturbAnalysis(SparkSession: SparkSession): Unit ={
    import SparkSession.sql
    val adjStrngDstrbAvailMrNumThresd :Integer = 100
    val adjStrongDisturbRateThreshold :Integer = 5
    sql(s"use $DDB")
    sql(s"""alter table lte_mro_disturb_ana add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
           LOCATION 'hdfs://dtcluster/$warhouseDir/lte_mro_disturb_ana/dt=$ANALY_DATE/h=$ANALY_HOUR'
      """)
    sql(s"""
           |  SELECT
           |         '$cal_date' as starttime,
           |         '$cal_date2' as endtime,
           |         ${period} as period,
           |         $ANALY_HOUR as timeseq ,
           |         M.TMMEGROUPID,
           |         M.TMMEID,
           |         M.TENODEBID,
           |         M.TCELLID,
           |         M.TCellName,
           |         M.tcellpci,
           |         M.tcellfreq,
           |         M.disturbMrNum,
           |         M.disturbAvalableNum,
           |         CASE
           |           WHEN SIGN(M.disturbAvalableNum) = 1 THEN (M.disturbMrNum / M.disturbAvalableNum) * 100
           |           ELSE 0
           |         END  AS disturbRate,
           |         CASE
           |           WHEN SIGN(M.disturbMrNum - ${adjStrngDstrbAvailMrNumThresd}) = 1 THEN
           |                CASE
           |                  WHEN SIGN((M.disturbMrNum / M.disturbAvalableNum) * 100 - ${adjStrongDisturbRateThreshold}) = 1 THEN 1
           |                  ELSE 0
           |                END
           |           ELSE 0
           |         END  AS isStrong,
           |         M.TCELLRSRPSum,
           |         M.TCELLRSRPCount,
           |         CASE
           |           WHEN SIGN(M.TCELLRSRPCount) = 1 THEN M.TCELLRSRPSum / M.TCELLRSRPCount
           |           ELSE 0
           |         END   AS adj_rsrp_avg_dbm,
           |         M.CELLRSRPSum,
           |         CASE
           |           WHEN M.CELLRSRPCount IS NOT NULL THEN  M.CELLRSRPCount
           |           ELSE 0
           |         END,
           |         CASE
           |           WHEN SIGN(M.CELLRSRPCount) = 1 THEN  M.CELLRSRPSum / M.CELLRSRPCount
           |           ELSE 0
           |         END  AS srv_rsrp_avg_dbm
           |    from (select TMMEGROUPID,
           |                 TMMEID,
           |                 TENODEBID,
           |                 TCELLID,
           |                 TCellName,
           |                 tcellpci,
           |                 tcellfreq,
           |                 sum(disturbMrNum) disturbMrNum,
           |                 sum(disturbAvalableNum) disturbAvalableNum,
           |                 sum(TCELLRSRPSum) TCELLRSRPSum,
           |                 sum(TCELLRSRPCount) TCELLRSRPCount,
           |                 sum(CELLRSRPSum) CELLRSRPSum,
           |                 sum(CELLRSRPCount) CELLRSRPCount
           |            from LTE_MRS_OVERCOVER_TEMP
           |           where CELLFREQ = TCELLFREQ
           |           group by TMMEGROUPID,
           |                    TMMEID,
           |                    TENODEBID,
           |                    TCELLID,
           |                    TCellName,
           |                    tcellpci,
           |                    tcellfreq) M
      """.stripMargin).write.mode(SaveMode.Overwrite).csv (s"$warhouseDir/lte_mro_disturb_ana/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

}
