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
    sql(s"use $DDB")
    sql(
      s"""alter table lte_mrs_dlbestrow_ana60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
        """.stripMargin)
    sql(
      s"""select STARTTIME, ENDTIME, TIMESEQ, MMEID, ENODEBID, CELLID, CELLPCI,CELLFREQ,CELLNAME,
                 TMMEGROUPID,TMMEID,tenbid,ttcellid, TCELLNAME,TCELLPCI, TCELLFREQ,
                 RSRPDIFABS ,RSRPDifCount, MrCount,CELLRSRPSum,CELLRSRPCount,TCELLRSRPSum,TCELLRSRPCount,ADJACENTAREAINTERFERENCEINTENS,
                 overlapDisturbRSRPDIFCount,adjeffectRSRPCount,disturbMrNum,disturbAvalableNum from
         (select STARTTIME, ENDTIME, TIMESEQ, MMEID, ENODEBID, CELLID, CELLPCI,CELLFREQ,CELLNAME,
                 TMMEGROUPID,TMMEID,TENODEBID,TCELLID, TCELLNAME,TCELLPCI, TCELLFREQ,
                 RSRPDIFABS ,RSRPDifCount, MrCount,CELLRSRPSum,CELLRSRPCount,TCELLRSRPSum,TCELLRSRPCount,ADJACENTAREAINTERFERENCEINTENS,
                 overlapDisturbRSRPDIFCount,adjeffectRSRPCount,disturbMrNum,disturbAvalableNum,tenbid,ttcellid,row_number()over(partition by CELLID,TIMESEQ
                order by (POWER(ABS(longitude-gridcenterlongitude), 2) + POWER(ABS(latitude-gridcenterlatitude), 2))) as rank
                 from(select * from (select t.startTime as STARTTIME, t.endTime as ENDTIME, t.timeseq as TIMESEQ,
                 t.mmecode as MMEID, t.enbid as ENODEBID, t.cellid as CELLID, t.kpi10 as CELLPCI,t.kpi9 as CELLFREQ,t2.cellname as CELLNAME,t2.ADJMMEGROUPID as TMMEGROUPID,t2.ADJMMEID as TMMEID,
                 t2.ADJENODEBID as TENODEBID,t2.adjcellID as TCELLID, t2.adjcellname as TCELLNAME,(case when t.kpi12!= -1 then t.kpi12 else null end) as TCELLPCI,
                 (case when t.kpi11!= -1 then t.kpi11 else null end) as TCELLFREQ,
                 sum(case when t.kpi1>=0 and t.kpi2 >=0 then t.kpi1 - t.kpi2 end)as RSRPDIFABS,
                 sum(case when (t.kpi1>=0 and t.kpi2 >=0 and abs(t.kpi1 - t.kpi2) $overcoveradjcellrsrpOp$overcoveradjcellrsrp) then 1 else 0 end) as rsrpDifSum,
                 count(*) as MrCount,
                 sum(case when t.kpi1>=0 then t.kpi1 -141 else 0 end) as CELLRSRPSum,
                 sum(case when t.kpi1>=0 then 1 else 0 end) as CELLRSRPCount,
                 sum(case when t.kpi2>=0 then t.kpi2 -141 else 0 end) as TCELLRSRPSum,
                 sum(case when t.kpi2>=0 then 1 else 0 end) as TCELLRSRPCount,
                 SUM (case when t.kpi1>=0 and t.kpi2 >=0  and t.kpi1 -141 > $AdjAvailableRsrpTh and t.kpi2 -141 > $AdjAvailableRsrpTh
                 ' and t.kpi2 - t.kpi1 < $adjDisturbRSRP then t.kpi2 - t.kpi1 else null end)/$adjDisturbRSRP as adjacentareainterferenceintens,
                 sum(case when t.kpi1>=0 and t.kpi2 >=0 and t.kpi1 -141 > $AdjAvailableRsrpTh and t.kpi2 -141 > $AdjAvailableRsrpTh  and t.kpi2 - t.kpi1 < $adjDisturbRSRP  then 1 else 0 end) as overlapDisturbRSRPDIFCount,
                 sum(case when t.kpi1>=0 and t.kpi2 >=0 and t.kpi1 -141 > $AdjAvailableRsrpTh  and t.kpi2 -141 > $AdjAvailableRsrpTh then 1 else 0 end) as adjeffectRSRPCount,
                 SUM (case when kpi9 - kpi11 = 0 and kpi2 > $AdjAvailableRsrpThreshold and t.kpi2 - t.kpi1 > $AdjDisturbRsrpDiffThreshold then 1 else 0 end) AS disturbMrNum,
                 SUM (case when kpi9 - kpi11 = 0 and kpi2 > $AdjAvailableRsrpThreshold then 1 else 0 end) AS disturbAvalableNum,
                 gridcenterlongitude,gridcenterlatitude
                 from (select * from lte_mro_source_ana_tmp where startTime is not null and mrname='MR.LteScRSRP') t
                 left join lte2lteadj_pci T2 on t.cellId = T2.cellid and T2.adjpci = t.kpi12 and T2.adjfreq1 = t.kpi11
                 group by t.startTime, t.endTime, t.timeseq,t.mmecode, t.enbid, t.cellid,t.kpi10,
                 t.kpi9,t2.cellname,t2.ADJMMEGROUPID,t2.ADJMMEID,
                 t2.ADJENODEBID,t2.adjcellID, t2.adjcellname, t.kpi11, t.kpi12,gridcenterlongitude,gridcenterlatitude)tmp1
                 ,
                 (select longitude,latitude,pci as tpci,freq1 as freq,enodebid as tenbid,CELLID as ttcellid  from ltecell
                 )tmp2 where tmp1.cellpci=tmp2.tpci and tmp1.cellfreq=tmp2.freq and tmp1.CELLID<>tmp2.ttcellid)s1)s2 where rank=1
        """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/lte_mrs_dlbestrow_ana60/dt=$ANALY_DATE/h=ANALY_HOUR")

  }
}
