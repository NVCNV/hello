package com.dtmobile.spark.biz.gridanalyse

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by shenkaili on 17-5-2.
  */
class Overcover(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String) {



  //  ltecover_degree_condition
//  adjDisturbRSRP

  var adjDisturbRSRPOp:String = "="
  var adjDisturbRSRP :BigDecimal= -10

//  overcoveradjcellrsrpdvalue
  var overcoveradjcellrsrpOp:String = "<"
  var overcoveradjcellrsrp :BigDecimal= 6


//  select AdjAvailableRsrpTh from LTEDISTURB_DEGREE_CONDITION where field = 'AdjAvailableRsrpThreshold';
    var AdjAvailableRsrpTh:BigDecimal = -110

//    ltedisturb_degree_condition

   var AdjAvailableRsrpThreshold:BigDecimal = -110 + 141

   var AdjDisturbRsrpDiffThreshold:BigDecimal = -6

//  AdjStrongDisturbAvailableMrNumThreshold
   var AdjStrngDstrbAvailMrNumThresd:BigDecimal = 100

  var AdjStrongDisturbRateThreshold :BigDecimal= 0.05*100

  def analyse(implicit SparkSession: SparkSession): Unit = {
    import SparkSession.sql

    val t = sql("select operator,value from ltepci_degree_condition where field = 'adjDisturbRSRP'").collectAsList()

    if (t.size() > 0) {
      adjDisturbRSRPOp = t.get(0).getString(0)
      adjDisturbRSRP = t.get(0).getDecimal(0)

  }
    val t1 = sql("select operator,value from ltepci_degree_condition where field = 'overcoveradjcellrsrpdvalue'").collectAsList()
    if (t1.size() > 0) {
      overcoveradjcellrsrpOp = t1.get(0).getString(0)
      overcoveradjcellrsrp = t1.get(0).getDecimal(0)
    }
    val t2 = sql("select value from LTEDISTURB_DEGREE_CONDITION where field = 'AdjAvailableRsrpThreshold'").collectAsList()
    if (t2.size() > 0) {
      AdjAvailableRsrpTh = t2.get(0).getDecimal(0)
    }
    val t3 = sql("select value from LTEDISTURB_DEGREE_CONDITION where field = 'AdjAvailableRsrpTh'").collectAsList()
     val bs : BigDecimal=141
    if (t3.size() > 0) {
      AdjAvailableRsrpThreshold = t3.get(0).getDecimal(0).doubleValue()+141
    }
    val t4 = sql("select value from LTEDISTURB_DEGREE_CONDITION where field = 'AdjDisturbRsrpDiffThreshold'").collectAsList()
    if (t4.size() > 0) {
      AdjDisturbRsrpDiffThreshold = t4.get(0).getDecimal(0)
    }
    val t5 = sql("select value from LTEDISTURB_DEGREE_CONDITION where field = 'AdjStrngDstrbAvailMrNumThresd'").collectAsList()
    if (t5.size() > 0) {
      AdjStrngDstrbAvailMrNumThresd = t5.get(0).getDecimal(0)
    }
    val t6 = sql("select value from LTEDISTURB_DEGREE_CONDITION where field = 'AdjStrongDisturbRateThreshold'").collectAsList()
    if (t6.size() > 0) {
      AdjStrongDisturbRateThreshold = t6.get(0).getDecimal(0).doubleValue()*100
    }

    sql(
      s"""
        |alter table ${DDB}.lte_mrs_overcover_ana60 add if not exists partition(dt=${ANALY_DATE},h=${ANALY_HOUR})
      """.stripMargin)
    sql(
      s"""
         |select
         |t.startTime as STARTTIME, t.endTime as ENDTIME, t.timeseq as TIMESEQ,
         |t.mmecode as MMEID,t.MMEGROUPID as MMEGROUPID, t.enbid as ENODEBID, t.cellid as CELLID, t.kpi10 as CELLPCI,t.kpi9 as CELLFREQ,t2.cellname as CELLNAME,t2.ADJMMEGROUPID as TMMEGROUPID,t2.ADJMMEID as TMMEID,
         |t2.ADJENODEBID as TENODEBID,t2.adjcellID as TCELLID, t2.adjcellname as TCELLNAME,(case when t.kpi12!= -1 then t.kpi12 else null end) as TCELLPCI,
         |(case when t.kpi11!= -1 then t.kpi11 else null end) as TCELLFREQ,
         |sum(case when t.kpi1>=0 and t.kpi2 >=0 then t.kpi1 - t.kpi2 end)as RSRPDIFABS,
         |sum(case when (t.kpi1>=0 and t.kpi2 >=0 and abs(t.kpi1 - t.kpi2) $overcoveradjcellrsrpOp$overcoveradjcellrsrp) then 1 else 0 end) as RSRPDifCount,
         |count(*) as MrCount,
         |sum(case when t.kpi1>=0 then t.kpi1 -141 else 0 end) as CELLRSRPSum,
         |sum(case when t.kpi1>=0 then 1 else 0 end) as CELLRSRPCount,
         |sum(case when t.kpi2>=0 then t.kpi2 -141 else 0 end) as TCELLRSRPSum,
         |sum(case when t.kpi2>=0 then 1 else 0 end) as TCELLRSRPCount,
         |SUM (case when t.kpi1>=0 and t.kpi2 >=0  and t.kpi1 -141 > $AdjAvailableRsrpTh and t.kpi2 -141 > $AdjAvailableRsrpTh and t.kpi2 - t.kpi1 < $adjDisturbRSRP then t.kpi2 - t.kpi1 else null end)/$adjDisturbRSRP as adjacentareainterferenceintens,
         |sum(case when t.kpi1>=0 and t.kpi2 >=0 and t.kpi1 -141 > $AdjAvailableRsrpTh and t.kpi2 -141 > $AdjAvailableRsrpTh  and t.kpi2 - t.kpi1 < $adjDisturbRSRP  then 1 else 0 end) as overlapDisturbRSRPDIFCount,
         |sum(case when t.kpi1>=0 and t.kpi2 >=0 and t.kpi1 -141 > $AdjAvailableRsrpTh  and t.kpi2 -141 > $AdjAvailableRsrpTh then 1 else 0 end) as adjeffectRSRPCount,
         |SUM (case when kpi9 - kpi11 = 0 and kpi2 > $AdjAvailableRsrpThreshold and t.kpi2 - t.kpi1 > $AdjDisturbRsrpDiffThreshold then 1 else 0 end) AS disturbMrNum,
         |SUM (case when kpi9 - kpi11 = 0 and kpi2 > $AdjAvailableRsrpThreshold then 1 else 0 end) AS disturbAvalableNum
         |from (select * from lte_mro_source_ana_tmp where STARTTIME is not null and mrname='MR.LteScRSRP') t
         |left join lte2lteadj_pci T2 on t.cellId = T2.cellid and T2.adjpci = t.kpi12 and T2.adjfreq1 = t.kpi11
         |group by t.startTime, t.endTime, t.timeseq,t.mmecode,t.MMEGROUPID, t.enbid, t.cellid,t.kpi10,
         |t.kpi9,t2.cellname,t2.ADJMMEGROUPID,t2.ADJMMEID,
         |t2.ADJENODEBID,t2.adjcellID, t2.adjcellname, t.kpi11, t.kpi12
       """.stripMargin).createOrReplaceTempView("LTE_MRS_OVERCOVER_TEMP")


    SparkSession.sql(
      s"""select s1.STARTTIME,s1.ENDTIME,s1.TIMESEQ,s1.MMEGROUPID,s1.MMEID,s1.ENODEBID,s1.CELLID,s2.tcellid,s1.TCELLPCI,
         |s1.TCELLFREQ,s1.RSRPDIFABS,s1.RSRPDifCount,
         |s1.MrCount,s1.CELLRSRPSum,s1.CELLRSRPCount,s1.TCELLRSRPSum,s1.TCELLRSRPCount,s1.ADJACENTAREAINTERFERE,
         |s1.overlapDisturbRSRPDIF,s1.adjeffectRSRPCount,s1.CELLFREQ,s1.CELLNAME,s1.TMMEGROUPID,
         |s1.TMMEID,s2.tenbid,s1.TCELLNAME,s1.disturbMrNum,s1.disturbAvalableNum
         |from LTE_MRS_OVERCOVER_TEMP s1 left join fill_tenbid_tcellid s2 on s1.cellid=s2.cellid
        """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/lte_mrs_overcover_ana60/dt=$ANALY_DATE/h=$ANALY_HOUR")



/*    SparkSession.sql(
      s"""select s1.STARTTIME, s1.ENDTIME, s1.TIMESEQ, s1.MMEID, s1.ENODEBID, s1.CELLID, s1.CELLPCI,s1.CELLFREQ,s1.CELLNAME,
         |s1.TMMEGROUPID,s1.TMMEID,s2.tenbid,s2.tcellid, s1.TCELLNAME,s1.TCELLPCI, s1.TCELLFREQ,
         |s1.RSRPDIFABS ,s1.RSRPDifCount, s1.MrCount,s1.CELLRSRPSum,s1.CELLRSRPCount,s1.TCELLRSRPSum,s1.TCELLRSRPCount,s1.ADJACENTAREAINTERFERENCEINTENS,
         |s1.overlapDisturbRSRPDIFCount,s1.adjeffectRSRPCount,s1.disturbMrNum,s1.disturbAvalableNum
         |from LTE_MRS_OVERCOVER_TEMP s1 left join fill_tenbid_tcellid s2 on s1.cellid=s2.cellid
        """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/lte_mrs_overcover_ana60/dt=$ANALY_DATE/h=$ANALY_HOUR")*/
  }


}
