package com.dtmobile.spark.biz.kpi

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoudehu on 2017/3/21/0021.
  */
class KpiDayAnaly(ANALY_DATE: String,SDB: String, DDB: String, warhouseDir: String) {

  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6)+ " " +  "00:00:00"


  def analyse(implicit sparkSession: SparkSession): Unit = {
    imsiCellDayAnalyse(sparkSession)
    cellDayAnalyse(sparkSession)
    mrImsiDayAnalyse(sparkSession)
    mrCellDayAnalyse(sparkSession)
  }

  def imsiCellDayAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table volte_gt_user_ana_baseday add if not exists partition(dt=$ANALY_DATE)")
    sql(
      s"""
         |SELECT
         |	imsi,
         |  imei,
         |	msisdn,
         |	CELLID,
         | '$cal_date',
         |	sum(voltemcsucc),
         |	sum(voltemcatt),
         |	sum(voltevdsucc),
         |	sum(voltevdatt),
         |	sum(voltetime),
         |	sum(voltemctime),
         |	sum(voltemctimey),
         |  sum(voltevdtime),
         |  sum(voltevdtimey),
         |	sum(voltemchandover),
         |	sum(volteanswer),
         |	sum(voltevdhandover),
         |	sum(voltevdanswer),
         |	sum(srvccsucc),
         |	sum(srvccatt),
         |	sum(srvcctime),
         |	sum(lteswsucc),
         |	sum(lteswatt),
         |	sum(srqatt),
         |	sum(srqsucc),
         |	sum(tauatt),
         |	sum(tausucc),
         |	sum(rrcrebuild),
         |	sum(rrcsucc),
         |	sum(rrcreq),
         |	sum(imsiregatt),
         |	sum(imsiregsucc),
         |	sum(wirelessdrop),
         |	sum(wireless),
         |	sum(eabdrop),
         |	sum(eab),
         |	sum(eabs1swx),
         |	sum(eabs1swy),
         |	sum(s1tox2swx),
         |	sum(s1tox2swy),
         |	sum(enbx2swx),
         |	sum(enbx2swy),
         |	sum(uuenbswx),
         |	sum(uuenbswy),
         |	sum(uuenbinx),
         |	sum(uuenbiny),
         |	sum(swx),
         |	sum(swy),
         |	sum(attachx),
         |	sum(attachy),
         |	sum(voltesucc),
         | sum(srvccsuccS1),
         | sum(s1contextbuild),
         |sum(enbrelese),
         |sum(nenbrelese),
         |sum(remaincontext),
         |sum(srvccsucc_Sv),
         |sum(srvccatt_s1),
         |sum(erab_nbrattestab),
         |sum(erab_nbrsuccestab),
         |sum(SuccInitalSetup),
         |sum(sm_adebrequest_qci),
         |sum(sm_adebaccept_qci1),
         |sum(sm_adebrequest_qci2),
         |sum(sm_adebaccept_qci2),
         |sum(erab_nbrreqrelenb_qci1),
         |sum(nbrreqrelenb_qci1),
         |sum(s1hooutsucc),
         |sum(s1hoout),
         |sum(s1hoinsucc),
         |sum(s1hoin),
         |sum(voltecallingmcsucc),
         |sum(voltecallingmcatt),
         |sum(voltecalledmcsucc),
         |sum(voltecalledmcatt),
         |sum(voltecallingvdsucc),
         |sum(voltecallingvdatt),
         |sum(voltecalledvdsucc),
         |sum(voltecalledvdatt),
         |sum(voltemcnetsucc),
         |sum(voltemcnetatt),
         |sum(voltevdnetsucc),
         |sum(voltevdnetatt),
         |sum(voltecallingmctime),
         |sum(voltecallingvdtime),
         |sum(srvcctime_sv),
         |sum(voltemcdur),
         |sum(voltevddur),
         |sum(rrcsucc_rebuild),
         |sum(srvccsucc_s1),
         |sum(enbx2insucc),
         |sum(enbx2outsucc)
         |from volte_gt_user_ana_base60
         |where dt=$ANALY_DATE
         |group by
         |  imsi,
         |  imei,
         |	msisdn,
         |	CELLID
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/volte_gt_user_ana_baseday/dt=$ANALY_DATE")
  }


  def cellDayAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table volte_gt_cell_ana_baseday add if not exists partition(dt=$ANALY_DATE)")
    sql(
      s"""
         |SELECT
         |'$cal_date',
         |	CELLID,
         |	sum(voltemcsucc),
         |	sum(voltemcatt),
         |	sum(voltevdsucc),
         |	sum(voltevdatt),
         |	sum(voltetime),
         |	sum(voltemctime),
         |	sum(voltemctimey),
         |  sum(voltevdtime),
         |  sum(voltevdtimey),
         |	sum(voltemchandover),
         |	sum(volteanswer),
         |	sum(voltevdhandover),
         |	sum(voltevdanswer),
         |	sum(srvccsucc),
         |	sum(srvccatt),
         |	sum(srvcctime),
         |	sum(lteswsucc),
         |	sum(lteswatt),
         |	sum(srqatt),
         |	sum(srqsucc),
         |	sum(tauatt),
         |	sum(tausucc),
         |	sum(rrcrebuild),
         |	sum(rrcsucc),
         |	sum(rrcreq),
         |	sum(imsiregatt),
         |	sum(imsiregsucc),
         |	sum(wirelessdrop),
         |	sum(wireless),
         |	sum(eabdrop),
         |	sum(eab),
         |	sum(eabs1swx),
         |	sum(eabs1swy),
         |	sum(s1tox2swx),
         |	sum(s1tox2swy),
         |	sum(enbx2swx),
         |	sum(enbx2swy),
         |	sum(uuenbswx),
         |	sum(uuenbswy),
         |	sum(uuenbinx),
         |	sum(uuenbiny),
         |	sum(swx),
         |	sum(swy),
         |	sum(attachx),
         |	sum(attachy),
         |	sum(voltesucc),
         | sum(srvccsuccS1),
         | sum(s1contextbuild),
         |sum(enbrelese),
         |sum(nenbrelese),
         |sum(remaincontext),
         |sum(srvccsucc_Sv),
         |sum(srvccatt_s1),
         |sum(erab_nbrattestab),
         |sum(erab_nbrsuccestab),
         |sum(SuccInitalSetup),
         |sum(sm_adebrequest_qci),
         |sum(sm_adebaccept_qci1),
         |sum(sm_adebrequest_qci2),
         |sum(sm_adebaccept_qci2),
         |sum(erab_nbrreqrelenb_qci1),
         |sum(nbrreqrelenb_qci1),
         |sum(s1hooutsucc),
         |sum(s1hoout),
         |sum(s1hoinsucc),
         |sum(s1hoin),
         |sum(voltecallingmcsucc),
         |sum(voltecallingmcatt),
         |sum(voltecalledmcsucc),
         |sum(voltecalledmcatt),
         |sum(voltecallingvdsucc),
         |sum(voltecallingvdatt),
         |sum(voltecalledvdsucc),
         |sum(voltecalledvdatt),
         |sum(voltemcnetsucc),
         |sum(voltemcnetatt),
         |sum(voltevdnetsucc),
         |sum(voltevdnetatt),
         |sum(voltecallingmctime),
         |sum(voltecallingvdtime),
         |sum(srvcctime_sv),
         |sum(voltemcdur),
         |sum(voltevddur),
         |sum(rrcsucc_rebuild),
         |sum(srvccsucc_s1),
         |sum(enbx2insucc),
         |sum(enbx2outsucc)
         |FROM
         |volte_gt_cell_ana_base60
         |where dt=$ANALY_DATE
         |GROUP BY
         |	cellid
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/volte_gt_cell_ana_baseday/dt=$ANALY_DATE")
  }

  def mrImsiDayAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table mr_gt_user_ana_baseday add if not exists partition(dt=$ANALY_DATE)")
    sql(
      s"""
         |SELECT
         |	imsi,
         |  imei,
         |	msisdn,
         |  cellid,
         |  rruid,
         |  gridid,
         | '$cal_date',
         |  dir_state,
         |  elong,
         |  elat,
         |	sum(avgrsrpx),
         |	sum(commy),
         |	sum(avgrsrqx),
         |	sum(ltecoverratex),
         |	sum(weakcoverratex),
         |	sum(overlapcoverratex),
         |	sum(overlapcoverratey),
         |	sum(upsigrateavgx),
         |	sum(upsigrateavgy),
         |	sum(updiststrox),
         |	sum(updiststroy),
         |  sum(model3diststrox),
         |	sum(model3diststroy),
         |	sum(uebootx),
         |	sum(uebooty)
         |FROM
         |	mr_gt_user_ana_base60
         | where dt=$ANALY_DATE
         |GROUP BY
         |	imsi,
         |  imei,
         |	msisdn,
         |  cellid,
         |  rruid,
         |  gridid,
         |  dir_state,
         |  elong,
         |  elat
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/mr_gt_user_ana_baseday/dt=$ANALY_DATE")
  }

  def mrCellDayAnalyse(implicit sparkSession: SparkSession): Unit = {
//    val CAL_DATE = ANALY_DATE + " " + ANALY_HOUR + "00:00"
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table mr_gt_cell_ana_baseday add if not exists partition(dt=$ANALY_DATE)")
    sql(
      s"""
         |SELECT
         |	cellid,
         | '$cal_date',
         |  dir_state,
         |	sum(avgrsrpx),
         |	sum(commy),
         |	sum(avgrsrqx),
         |	sum(ltecoverratex),
         |	sum(weakcoverratex),
         |	sum(overlapcoverratex) ,
         |	sum(overlapcoverratey),
         |  sum(upsigrateavgx),
         |  sum(upsigrateavgy),
         |	sum(updiststrox),
         |	sum(updiststroy),
         |	sum(model3diststrox),
         |	sum(model3diststroy),
         | 	sum(uebootx),
         |	sum(uebooty)
         |FROM
         |	mr_gt_cell_ana_base60
         | where dt=$ANALY_DATE
         |GROUP BY
         |	cellid,
         | dir_state
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/mr_gt_cell_ana_baseday/dt=$ANALY_DATE")
  }


}
