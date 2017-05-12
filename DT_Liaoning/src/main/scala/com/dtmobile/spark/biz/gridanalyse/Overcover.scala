package com.dtmobile.spark.biz.gridanalyse

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by shenkaili on 17-5-2.
  */
class Overcover(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String) {



  //  ltecover_degree_condition
//  adjDisturbRSRP
  var adjDisturbRSRPOp = '='
  var adjDisturbRSRP = -10

//  overcoveradjcellrsrpdvalue
  var overcoveradjcellrsrpOp = '<'
  var overcoveradjcellrsrp = 6





//  select AdjAvailableRsrpTh from LTEDISTURB_DEGREE_CONDITION where field = 'AdjAvailableRsrpThreshold';
    var AdjAvailableRsrpTh = -110

//    ltedisturb_degree_condition

   var AdjAvailableRsrpThreshold= -110 + 141

   var AdjDisturbRsrpDiffThreshold = -6

//  AdjStrongDisturbAvailableMrNumThreshold
   var AdjStrngDstrbAvailMrNumThresd = 100

  var AdjStrongDisturbRateThreshold = 0.05*100

  def analyse(implicit sparkSession: SparkSession): Unit =
  {
    import sparkSession.sql
    sql(s"alter table $DDB.lte_mrs_dlbestrow_ana60 add if not exists partition(dt='$ANALY_DATE',h='$ANALY_HOUR')")
    sql(
      s"""select s1.STARTTIME, s1.ENDTIME, s1.TIMESEQ, s1.MMEID, s1.ENODEBID, s1.CELLID, s1.CELLPCI,s1.CELLFREQ,s1.CELLNAME,
         |s1.TMMEGROUPID,s1.TMMEID,s2.tenbid,s2.tcellid, s1.TCELLNAME,s1.TCELLPCI, s1.TCELLFREQ,
         |s1.RSRPDIFABS ,s1.RSRPDifCount, s1.MrCount,s1.CELLRSRPSum,s1.CELLRSRPCount,s1.TCELLRSRPSum,s1.TCELLRSRPCount,s1.ADJACENTAREAINTERFERENCEINTENS,
         |s1.overlapDisturbRSRPDIFCount,s1.adjeffectRSRPCount,s1.disturbMrNum,s1.disturbAvalableNum
         |from (select t.startTime as STARTTIME, t.endTime as ENDTIME, t.timeseq as TIMESEQ,
         |t.mmecode as MMEID, t.enbid as ENODEBID, t.cellid as CELLID, t.kpi10 as CELLPCI,t.kpi9 as CELLFREQ,t2.cellname as CELLNAME,t2.ADJMMEGROUPID as TMMEGROUPID,t2.ADJMMEID as TMMEID,
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
         |from (select * from lte_mro_source_ana_tmp where startTime is not null and mrname='MR.LteScRSRP') t
         |left join lte2lteadj_pci T2 on t.cellId = T2.cellid and T2.adjpci = t.kpi12 and T2.adjfreq1 = t.kpi11
         |group by t.startTime, t.endTime, t.timeseq,t.mmecode, t.enbid, t.cellid,t.kpi10,
         |t.kpi9,t2.cellname,t2.ADJMMEGROUPID,t2.ADJMMEID,
         |t2.ADJENODEBID,t2.adjcellID, t2.adjcellname, t.kpi11, t.kpi12)s1 left join fill_tenbid_tcellid s2 on s1.cellid=s2.cellid
        """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/lte_mrs_dlbestrow_ana60/dt=$ANALY_DATE/h=$ANALY_HOUR")

  }
}
