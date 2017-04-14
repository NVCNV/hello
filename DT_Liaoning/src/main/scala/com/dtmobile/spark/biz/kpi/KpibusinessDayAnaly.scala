package com.dtmobile.spark.biz.kpi

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by shenkaili on 17-3-31.
  */
class KpibusinessDayAnaly(ANALY_DATE: String,SDB: String, DDB: String, warhouseDir: String) {
  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6)+ " " +"00:00:00"
  def analyse(implicit sparkSession: SparkSession): Unit = {
    tacDayAnalyse(sparkSession)
    cellDayAnalyse(sparkSession)
    spDayAnalyse(sparkSession)
    ueDayAnalyse(sparkSession)
    sgwDayAnalyse(sparkSession)
    imsicellDayAnalyse(sparkSession)
  }

  def tacDayAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table tac_day_http add if not exists partition(dt=$ANALY_DATE)")
    sql(
      s"""
         |select
           |'$cal_date',
           |tac,
           |sum(browsedownloadvisits),
           |sum(videoservicevisits),
           |sum(instantmessagevisits),
           |sum(appvisits),
           |sum(browsedownloadbusiness),
           |sum(videoservicebusiness),
           |sum(instantmessagebusiness),
           |sum(appbusiness),
           |sum(dnsQuerySucc),
           |sum(dnsQueryAtt),
           |sum(tcpSetupSucc),
           |sum(tcpSetupReq),
           |sum(BearerULTCPRetransmit),
           |sum(BearerULTCPTransmit),
           |sum(BearerDLTCPRetransmit),
           |sum(BearerDLTCPTransmit),
           |sum(BearerULTCPMissequence),
           |sum(BearerDLTCPMissequence),
           |sum(pageresp),
           |sum(pagereq),
           |sum(pageresptimeall),
           |sum(pageshowsucc),
           |sum(pageshowtimeall),
           |sum(httpdownflow),
           |sum(httpdowntime),
           |sum(mediasucc),
           |sum(mediareq),
           |sum(mediadownflow),
           |sum(mediadowntime),
           |sum(ServiceIMSucc),
           |sum(ServiceIMReq)
           |from tac_hour_http where dt="$ANALY_DATE" group by tac
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/tac_day_http/dt=$ANALY_DATE")

  }

  def cellDayAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table cell_day_http add if not exists partition(dt=$ANALY_DATE)")
    sql(
      s"""
         |select
         |'$cal_date',
         |cellid,
         |sum(browsedownloadvisits),
         |sum(videoservicevisits),
         |sum(instantmessagevisits),
         |sum(appvisits),
         |sum(browsedownloadbusiness),
         |sum(videoservicebusiness),
         |sum(instantmessagebusiness),
         |sum(appbusiness),
         |sum(dnsQuerySucc),
         |sum(dnsQueryAtt),
         |sum(tcpSetupSucc),
         |sum(tcpSetupReq),
         |sum(BearerULTCPRetransmit),
         |sum(BearerULTCPTransmit),
         |sum(BearerDLTCPRetransmit),
         |sum(BearerDLTCPTransmit),
         |sum(BearerULTCPMissequence),
         |sum(BearerDLTCPMissequence),
         |sum(pageresp),
         |sum(pagereq),
         |sum(pageresptimeall),
         |sum(pageshowsucc),
         |sum(pageshowtimeall),
         |sum(httpdownflow),
         |sum(httpdowntime),
         |sum(mediasucc),
         |sum(mediareq),
         |sum(mediadownflow),
         |sum(mediadowntime),
         |sum(ServiceIMSucc),
         |sum(ServiceIMReq)
         |from cell_hour_http where dt="$ANALY_DATE" group by cellid
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/cell_day_http/dt=$ANALY_DATE")

  }

  def spDayAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table sp_day_http add if not exists partition(dt=$ANALY_DATE)")
    sql(
      s"""
         select
         |'$cal_date',
         |appserveripipv4,
         |sum(browsedownloadvisits),
         |sum(videoservicevisits),
         |sum(instantmessagevisits),
         |sum(appvisits),
         |sum(browsedownloadbusiness),
         |sum(videoservicebusiness),
         |sum(instantmessagebusiness),
         |sum(appbusiness),
         |sum(dnsQuerySucc),
         |sum(dnsQueryAtt),
         |sum(tcpSetupSucc),
         |sum(tcpSetupReq),
         |sum(BearerULTCPRetransmit),
         |sum(BearerULTCPTransmit),
         |sum(BearerDLTCPRetransmit),
         |sum(BearerDLTCPTransmit),
         |sum(BearerULTCPMissequence),
         |sum(BearerDLTCPMissequence),
         |sum(pageresp),
         |sum(pagereq),
         |sum(pageresptimeall),
         |sum(pageshowsucc),
         |sum(pageshowtimeall),
         |sum(httpdownflow),
         |sum(httpdowntime),
         |sum(mediasucc),
         |sum(mediareq),
         |sum(mediadownflow),
         |sum(mediadowntime),
         |sum(ServiceIMSucc),
         |sum(ServiceIMReq)
         |from sp_hour_http where dt="$ANALY_DATE" group by appserveripipv4
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/sp_day_http/dt=$ANALY_DATE")
  }

  def ueDayAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table ue_day_http add if not exists partition(dt=$ANALY_DATE)")
    sql(
      s"""
         |select
         |'$cal_date',
         |imsi,
         |msisdn,
         |sum(browsedownloadvisits),
         |sum(videoservicevisits),
         |sum(instantmessagevisits),
         |sum(appvisits),
         |sum(browsedownloadbusiness),
         |sum(videoservicebusiness),
         |sum(instantmessagebusiness),
         |sum(appbusiness),
         |sum(dnsQuerySucc),
         |sum(dnsQueryAtt),
         |sum(tcpSetupSucc),
         |sum(tcpSetupReq),
         |sum(BearerULTCPRetransmit),
         |sum(BearerULTCPTransmit),
         |sum(BearerDLTCPRetransmit),
         |sum(BearerDLTCPTransmit),
         |sum(BearerULTCPMissequence),
         |sum(BearerDLTCPMissequence),
         |sum(pageresp),
         |sum(pagereq),
         |sum(pageresptimeall),
         |sum(pageshowsucc),
         |sum(pageshowtimeall),
         |sum(httpdownflow),
         |sum(httpdowntime),
         |sum(mediasucc),
         |sum(mediareq),
         |sum(mediadownflow),
         |sum(mediadowntime),
         |sum(ServiceIMSucc),
         |sum(ServiceIMReq)
         |from ue_hour_http where dt="$ANALY_DATE" group by imsi,msisdn
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/ue_day_http/dt=$ANALY_DATE")
  }

  def sgwDayAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table sgw_day_http add if not exists partition(dt=$ANALY_DATE)")
    sql(
      s"""
         |select
         |'$cal_date',
         |sgwipaddr,
         |sum(browsedownloadvisits),
         |sum(videoservicevisits),
         |sum(instantmessagevisits),
         |sum(appvisits),
         |sum(browsedownloadbusiness),
         |sum(videoservicebusiness),
         |sum(instantmessagebusiness),
         |sum(appbusiness),
         |sum(dnsQuerySucc),
         |sum(dnsQueryAtt),
         |sum(tcpSetupSucc),
         |sum(tcpSetupReq),
         |sum(BearerULTCPRetransmit),
         |sum(BearerULTCPTransmit),
         |sum(BearerDLTCPRetransmit),
         |sum(BearerDLTCPTransmit),
         |sum(BearerULTCPMissequence),
         |sum(BearerDLTCPMissequence),
         |sum(pageresp),
         |sum(pagereq),
         |sum(pageresptimeall),
         |sum(pageshowsucc),
         |sum(pageshowtimeall),
         |sum(httpdownflow),
         |sum(httpdowntime),
         |sum(mediasucc),
         |sum(mediareq),
         |sum(mediadownflow),
         |sum(mediadowntime),
         |sum(ServiceIMSucc),
         |sum(ServiceIMReq)
         |from sgw_hour_http where dt="$ANALY_DATE" group by sgwipaddr
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/sgw_day_http/dt=$ANALY_DATE")
  }

  def imsicellDayAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table imsi_cell_day_http add if not exists partition(dt=$ANALY_DATE)")
    sql(
      s"""
         |select
         |'$cal_date',
         |imsi,
         |msisdn,
         |cellid,
         |sum(browsedownloadvisits),
         |sum(videoservicevisits),
         |sum(instantmessagevisits),
         |sum(appvisits),
         |sum(browsedownloadbusiness),
         |sum(videoservicebusiness),
         |sum(instantmessagebusiness),
         |sum(appbusiness),
         |sum(dnsQuerySucc),
         |sum(dnsQueryAtt),
         |sum(tcpSetupSucc),
         |sum(tcpSetupReq),
         |sum(BearerULTCPRetransmit),
         |sum(BearerULTCPTransmit),
         |sum(BearerDLTCPRetransmit),
         |sum(BearerDLTCPTransmit),
         |sum(BearerULTCPMissequence),
         |sum(BearerDLTCPMissequence),
         |sum(pageresp),
         |sum(pagereq),
         |sum(pageresptimeall),
         |sum(pageshowsucc),
         |sum(pageshowtimeall),
         |sum(httpdownflow),
         |sum(httpdowntime),
         |sum(mediasucc),
         |sum(mediareq),
         |sum(mediadownflow),
         |sum(mediadowntime),
         |sum(ServiceIMSucc),
         |sum(ServiceIMReq)
         |from imsi_cell_hour_http where dt="$ANALY_DATE" group by imsi,msisdn,cellid
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/imsi_cell_day_http/dt=$ANALY_DATE")
  }

}
