package com.dtmobile.spark.biz.kpi

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by shenkaili on 17-3-31.
  */
class KpibusinessHourAnaly(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String) {
  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"

  def analyse(implicit sparkSession: SparkSession): Unit = {
    tacHourAnalyse(sparkSession)
    cellHourAnalyse(sparkSession)
    spHourAnalyse(sparkSession)
    ueHourAnalyse(sparkSession)
    sgwHourAnalyse(sparkSession)
    imsicellHourAnalyse(sparkSession)

  }

  def tacHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table tac_hour_http add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(
      s"""
         |select
         |'$cal_date',
         |substring(imei,1,8)tac,
         |sum(case when interface=11 and apptypecode=103 and apptype=15 then 1 else 0 end)browsedownloadvisits,
         |sum(case when interface=11 and (apptypecode=103 or apptypecode=107) and apptype=5 then 1 else 0 end)videoservicevisits,
         |sum(case when interface=11 and apptypecode=108 and apptype=1 then 1 else 0 end)instantmessagevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=7 then 1 else 0 end)appvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 then DLDATA+ULDATA end )browsedownloadbusiness,
         |sum(case when Interface = 11 and (APPTYPECODE = 103 or APPTYPECODE = 107)and APPTYPE = 5 then DLDATA+ULDATA end )videoservicebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 108 and APPTYPE = 1 then DLDATA+ULDATA end )instantmessagebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 7 then DLDATA+ULDATA end )appbusiness,
         |0 as dnsQuerySucc,
         |0 as dnsQueryAtt,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 and tcplinkstatus=0 then 1 else 0 end)tcpSetupSucc,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then 1 else 0 end)tcpSetupReq,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ultcppacketre else 0 end)BearerULTCPRetransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ulippacket else 0 end)BearerULTCPTransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dltcppacketre else 0 end)BearerDLTCPRetransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dlippacket else 0 end)BearerDLTCPTransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ultcppacketor else 0 end)BearerULTCPMissequence,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dltcppacketor else 0 end)BearerDLTCPMissequence,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPFIRSTREDE != 0 and HTTPFIRSTREDE is not null then 1 else 0 end)pageresp,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 then 1 else 0 end )pagereq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPFIRSTREDE != 0 and HTTPFIRSTREDE is not null then HTTPFIRSTREDE else 0 end)pageresptimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then 1 else 0 end)pageshowsucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPLASTREDE != 0  and HTTPLASTREDE is not null then HTTPLASTREDE else 0 end)pageshowtimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 then DLData/1024 end)httpdownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then (CASE WHEN (HTTPLASTREDE - HTTPFIRSTREDE)<10 then HTTPFIRSTREDE else (HTTPLASTREDE - HTTPFIRSTREDE) END) end)httpdowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS = 0  then 1 else 0 end)mediasucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 then 1 else 0 end)mediareq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5  then DLData/1024 end )mediadownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5  then (procedureendtime - procedurestarttime) else 0 end)mediadowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and appstatus=0  then DLData/1024 else 0 end)ServiceIMSucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1  then (procedureendtime - procedurestarttime) else 0 end)ServiceIMReq
         |from tb_xdr_ifc_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by imei
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/tac_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

  def cellHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table cell_hour_http add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(
      s"""
         |select
         |'$cal_date',
         |ecgi,
         |sum(case when interface=11 and apptypecode=103 and apptype=15 then 1 else 0 end)browsedownloadvisits,
         |sum(case when interface=11 and (apptypecode=103 or apptypecode=107) and apptype=5 then 1 else 0 end)videoservicevisits,
         |sum(case when interface=11 and apptypecode=108 and apptype=1 then 1 else 0 end)instantmessagevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=7 then 1 else 0 end)appvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 then DLDATA+ULDATA end )browsedownloadbusiness,
         |sum(case when Interface = 11 and (APPTYPECODE = 103 or APPTYPECODE = 107)and APPTYPE = 5 then DLDATA+ULDATA end )videoservicebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 108 and APPTYPE = 1 then DLDATA+ULDATA end )instantmessagebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 7 then DLDATA+ULDATA end )appbusiness,
         |0 as dnsQuerySucc,
         |0 as dnsQueryAtt,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 and tcplinkstatus=0 then 1 else 0 end)tcpSetupSucc,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then 1 else 0 end)tcpSetupReq,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ultcppacketre else 0 end)BearerULTCPRetransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ulippacket else 0 end)BearerULTCPTransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dltcppacketre else 0 end)BearerDLTCPRetransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dlippacket else 0 end)BearerDLTCPTransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ultcppacketor else 0 end)BearerULTCPMissequence,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dltcppacketor else 0 end)BearerDLTCPMissequence,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPFIRSTREDE != 0 and HTTPFIRSTREDE is not null then 1 else 0 end)pageresp,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 then 1 else 0 end )pagereq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPFIRSTREDE != 0 and HTTPFIRSTREDE is not null then HTTPFIRSTREDE else 0 end)pageresptimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then 1 else 0 end)pageshowsucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPLASTREDE != 0  and HTTPLASTREDE is not null then HTTPLASTREDE else 0 end)pageshowtimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 then DLData/1024 end)httpdownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then (CASE WHEN (HTTPLASTREDE - HTTPFIRSTREDE)<10 then HTTPFIRSTREDE else (HTTPLASTREDE - HTTPFIRSTREDE) END) end)httpdowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS = 0  then 1 else 0 end)mediasucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 then 1 else 0 end)mediareq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5  then DLData/1024 end )mediadownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5  then (procedureendtime - procedurestarttime) else 0 end)mediadowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and appstatus=0  then DLData/1024 else 0 end)ServiceIMSucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1  then (procedureendtime - procedurestarttime) else 0 end)ServiceIMReq
         |from tb_xdr_ifc_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by ecgi

       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/cell_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR")

  }

  def spHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table sp_hour_http add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(
      s"""
         |select
         |'$cal_date',
         |appserveripipv4,
         |sum(case when interface=11 and apptypecode=103 and apptype=15 then 1 else 0 end)browsedownloadvisits,
         |sum(case when interface=11 and (apptypecode=103 or apptypecode=107) and apptype=5 then 1 else 0 end)videoservicevisits,
         |sum(case when interface=11 and apptypecode=108 and apptype=1 then 1 else 0 end)instantmessagevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=7 then 1 else 0 end)appvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 then DLDATA+ULDATA end )browsedownloadbusiness,
         |sum(case when Interface = 11 and (APPTYPECODE = 103 or APPTYPECODE = 107)and APPTYPE = 5 then DLDATA+ULDATA end )videoservicebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 108 and APPTYPE = 1 then DLDATA+ULDATA end )instantmessagebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 7 then DLDATA+ULDATA end )appbusiness,
         |0 as dnsQuerySucc,
         |0 as dnsQueryAtt,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 and tcplinkstatus=0 then 1 else 0 end)tcpSetupSucc,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then 1 else 0 end)tcpSetupReq,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ultcppacketre else 0 end)BearerULTCPRetransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ulippacket else 0 end)BearerULTCPTransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dltcppacketre else 0 end)BearerDLTCPRetransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dlippacket else 0 end)BearerDLTCPTransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ultcppacketor else 0 end)BearerULTCPMissequence,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dltcppacketor else 0 end)BearerDLTCPMissequence,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPFIRSTREDE != 0 and HTTPFIRSTREDE is not null then 1 else 0 end)pageresp,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 then 1 else 0 end )pagereq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPFIRSTREDE != 0 and HTTPFIRSTREDE is not null then HTTPFIRSTREDE else 0 end)pageresptimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then 1 else 0 end)pageshowsucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPLASTREDE != 0  and HTTPLASTREDE is not null then HTTPLASTREDE else 0 end)pageshowtimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 then DLData/1024 end)httpdownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then (CASE WHEN (HTTPLASTREDE - HTTPFIRSTREDE)<10 then HTTPFIRSTREDE else (HTTPLASTREDE - HTTPFIRSTREDE) END) end)httpdowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS = 0  then 1 else 0 end)mediasucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 then 1 else 0 end)mediareq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5  then DLData/1024 end )mediadownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5  then (procedureendtime - procedurestarttime) else 0 end)mediadowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and appstatus=0  then DLData/1024 else 0 end)ServiceIMSucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1  then (procedureendtime - procedurestarttime) else 0 end)ServiceIMReq
         |from tb_xdr_ifc_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by appserveripipv4
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/sp_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

  def ueHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table ue_hour_http add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(
      s"""
         |select
         |'$cal_date',
         |imsi,
         |msisdn,
         |sum(case when interface=11 and apptypecode=103 and apptype=15 then 1 else 0 end)browsedownloadvisits,
         |sum(case when interface=11 and (apptypecode=103 or apptypecode=107) and apptype=5 then 1 else 0 end)videoservicevisits,
         |sum(case when interface=11 and apptypecode=108 and apptype=1 then 1 else 0 end)instantmessagevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=7 then 1 else 0 end)appvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 then DLDATA+ULDATA end )browsedownloadbusiness,
         |sum(case when Interface = 11 and (APPTYPECODE = 103 or APPTYPECODE = 107)and APPTYPE = 5 then DLDATA+ULDATA end )videoservicebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 108 and APPTYPE = 1 then DLDATA+ULDATA end )instantmessagebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 7 then DLDATA+ULDATA end )appbusiness,
         |0 as dnsQuerySucc,
         |0 as dnsQueryAtt,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 and tcplinkstatus=0 then 1 else 0 end)tcpSetupSucc,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then 1 else 0 end)tcpSetupReq,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ultcppacketre else 0 end)BearerULTCPRetransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ulippacket else 0 end)BearerULTCPTransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dltcppacketre else 0 end)BearerDLTCPRetransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dlippacket else 0 end)BearerDLTCPTransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ultcppacketor else 0 end)BearerULTCPMissequence,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dltcppacketor else 0 end)BearerDLTCPMissequence,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPFIRSTREDE != 0 and HTTPFIRSTREDE is not null then 1 else 0 end)pageresp,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 then 1 else 0 end )pagereq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPFIRSTREDE != 0 and HTTPFIRSTREDE is not null then HTTPFIRSTREDE else 0 end)pageresptimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then 1 else 0 end)pageshowsucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPLASTREDE != 0  and HTTPLASTREDE is not null then HTTPLASTREDE else 0 end)pageshowtimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 then DLData/1024 end)httpdownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then (CASE WHEN (HTTPLASTREDE - HTTPFIRSTREDE)<10 then HTTPFIRSTREDE else (HTTPLASTREDE - HTTPFIRSTREDE) END) end)httpdowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS = 0  then 1 else 0 end)mediasucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 then 1 else 0 end)mediareq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5  then DLData/1024 end )mediadownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5  then (procedureendtime - procedurestarttime) else 0 end)mediadowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and appstatus=0  then DLData/1024 else 0 end)ServiceIMSucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1  then (procedureendtime - procedurestarttime) else 0 end)ServiceIMReq
         |from tb_xdr_ifc_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by imsi,msisdn
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/ue_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

  def sgwHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table sgw_hour_http add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(
      s"""
         |select
         |'$cal_date',
         |sgwipaddr,
         |sum(case when interface=11 and apptypecode=103 and apptype=15 then 1 else 0 end)browsedownloadvisits,
         |sum(case when interface=11 and (apptypecode=103 or apptypecode=107) and apptype=5 then 1 else 0 end)videoservicevisits,
         |sum(case when interface=11 and apptypecode=108 and apptype=1 then 1 else 0 end)instantmessagevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=7 then 1 else 0 end)appvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 then DLDATA+ULDATA end )browsedownloadbusiness,
         |sum(case when Interface = 11 and (APPTYPECODE = 103 or APPTYPECODE = 107)and APPTYPE = 5 then DLDATA+ULDATA end )videoservicebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 108 and APPTYPE = 1 then DLDATA+ULDATA end )instantmessagebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 7 then DLDATA+ULDATA end )appbusiness,
         |0 as dnsQuerySucc,
         |0 as dnsQueryAtt,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 and tcplinkstatus=0 then 1 else 0 end)tcpSetupSucc,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then 1 else 0 end)tcpSetupReq,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ultcppacketre else 0 end)BearerULTCPRetransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ulippacket else 0 end)BearerULTCPTransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dltcppacketre else 0 end)BearerDLTCPRetransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dlippacket else 0 end)BearerDLTCPTransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ultcppacketor else 0 end)BearerULTCPMissequence,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dltcppacketor else 0 end)BearerDLTCPMissequence,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPFIRSTREDE != 0 and HTTPFIRSTREDE is not null then 1 else 0 end)pageresp,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 then 1 else 0 end )pagereq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPFIRSTREDE != 0 and HTTPFIRSTREDE is not null then HTTPFIRSTREDE else 0 end)pageresptimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then 1 else 0 end)pageshowsucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPLASTREDE != 0  and HTTPLASTREDE is not null then HTTPLASTREDE else 0 end)pageshowtimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 then DLData/1024 end)httpdownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then (CASE WHEN (HTTPLASTREDE - HTTPFIRSTREDE)<10 then HTTPFIRSTREDE else (HTTPLASTREDE - HTTPFIRSTREDE) END) end)httpdowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS = 0  then 1 else 0 end)mediasucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 then 1 else 0 end)mediareq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5  then DLData/1024 end )mediadownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5  then (procedureendtime - procedurestarttime) else 0 end)mediadowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and appstatus=0  then DLData/1024 else 0 end)ServiceIMSucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1  then (procedureendtime - procedurestarttime) else 0 end)ServiceIMReq
         |from tb_xdr_ifc_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by sgwipaddr
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/sgw_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

  def imsicellHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table imsi_cell_hour_http add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(
      s"""
         |select
         |'$cal_date',
         |imsi,
         |msisdn,
         |ecgi,
         |sum(case when interface=11 and apptypecode=103 and apptype=15 then 1 else 0 end)browsedownloadvisits,
         |sum(case when interface=11 and (apptypecode=103 or apptypecode=107) and apptype=5 then 1 else 0 end)videoservicevisits,
         |sum(case when interface=11 and apptypecode=108 and apptype=1 then 1 else 0 end)instantmessagevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=7 then 1 else 0 end)appvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 then DLDATA+ULDATA end )browsedownloadbusiness,
         |sum(case when Interface = 11 and (APPTYPECODE = 103 or APPTYPECODE = 107)and APPTYPE = 5 then DLDATA+ULDATA end )videoservicebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 108 and APPTYPE = 1 then DLDATA+ULDATA end )instantmessagebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 7 then DLDATA+ULDATA end )appbusiness,
         |0 as dnsQuerySucc,
         |0 as dnsQueryAtt,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 and tcplinkstatus=0 then 1 else 0 end)tcpSetupSucc,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then 1 else 0 end)tcpSetupReq,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ultcppacketre else 0 end)BearerULTCPRetransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ulippacket else 0 end)BearerULTCPTransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dltcppacketre else 0 end)BearerDLTCPRetransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dlippacket else 0 end)BearerDLTCPTransmit,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then ultcppacketor else 0 end)BearerULTCPMissequence,
         |sum(case when interface=11 and apptypecode=103 and l4protocal=0 then dltcppacketor else 0 end)BearerDLTCPMissequence,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPFIRSTREDE != 0 and HTTPFIRSTREDE is not null then 1 else 0 end)pageresp,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 then 1 else 0 end )pagereq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPFIRSTREDE != 0 and HTTPFIRSTREDE is not null then HTTPFIRSTREDE else 0 end)pageresptimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then 1 else 0 end)pageshowsucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and HTTPLASTREDE != 0  and HTTPLASTREDE is not null then HTTPLASTREDE else 0 end)pageshowtimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 then DLData/1024 end)httpdownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then (CASE WHEN (HTTPLASTREDE - HTTPFIRSTREDE)<10 then HTTPFIRSTREDE else (HTTPLASTREDE - HTTPFIRSTREDE) END) end)httpdowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS = 0  then 1 else 0 end)mediasucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 then 1 else 0 end)mediareq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5  then DLData/1024 end )mediadownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5  then (procedureendtime - procedurestarttime) else 0 end)mediadowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and appstatus=0  then DLData/1024 else 0 end)ServiceIMSucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1  then (procedureendtime - procedurestarttime) else 0 end)ServiceIMReq
         |from tb_xdr_ifc_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by imsi,msisdn,ecgi
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/imsi_cell_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

}

