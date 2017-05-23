package com.dtmobile.spark.biz.kpi

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by shenkaili on 17-3-31.
  */
class KpibusinessHourAnaly(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String) {
  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"
  val onoff=0
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
    sql(s"""alter table tac_hour_http add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
           LOCATION 'hdfs://dtcluster/$warhouseDir/tac_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR'
       """
      )
    sql(
      s"""
         |select
         |'$cal_date',
         |substring(imei,1,8)tac,
         |sum(case when interface=11 and apptypecode=103 and apptype=15 then 1 else 0 end)browsedownloadvisits,
         |sum(case when interface=11 and (apptypecode=103 or apptypecode=107) and apptype=5 then 1 else 0 end)videoservicevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=1 then 1 else 0 end)instantmessagevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=7 then 1 else 0 end)appvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )browsedownloadbusiness,
         |sum(case when Interface = 11 and (APPTYPECODE = 103 or APPTYPECODE = 107)and APPTYPE = 5 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )videoservicebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )instantmessagebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 7 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )appbusiness,
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
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and httplastrede is not null and httplastrede<>0 and DLDATA is not null then DLData else 0 end)httpdownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then (CASE WHEN (HTTPLASTREDE - HTTPFIRSTREDE)<10 then 10 else (HTTPLASTREDE - HTTPFIRSTREDE) END) end)httpdowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS = 0  then 1 else 0 end)mediasucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 then 1 else 0 end)mediareq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS=0 and DLDATA is not null then DLData else 0 end )mediadownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS=0 then (procedureendtime - procedurestarttime) else 0 end)mediadowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and appstatus=0  then 1 else 0 end)ServiceIMSucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 then 1 else 0 end)ServiceIMReq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 2  then 1 else 0 end)readvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 3  then 1 else 0 end)wbvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 4  then 1 else 0 end)navigationvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 6  then 1 else 0 end)musicvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 8  then 1 else 0 end)gamevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 9  then 1 else 0 end)payvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 10  then 1 else 0 end)Animevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 11  then 1 else 0 end)mailvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 12  then 1 else 0 end)p2pvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 13  then 1 else 0 end)voipvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 14  then 1 else 0 end)MultimediaMsgvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 16 then 1 else 0 end)financialvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 17 then 1 else 0 end)securityvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 18 then 1 else 0 end)shoppingvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 19 then 1 else 0 end)travelvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 20 then 1 else 0 end)cloudstoragevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 21 then 1 else 0 end)othervisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 2  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)readbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 3  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)wbbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 4  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)navigationbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 6  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)musicbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 8  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)gamebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 9  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)paybusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 10 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)Animebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 11 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)mailbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 12 and DLDATA is not null and ULDATA is not null  then (uldata+dldata) else 0 end)p2pbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 13 and DLDATA is not null and ULDATA is not null  then (uldata+dldata) else 0 end)voipbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 14 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)MultimediaMsgbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 16 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)financialbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 17 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)securitybusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 18 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)shoppingbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 19 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)travelbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 20 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)cloudstoragebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 21 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)otherbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and busrede is not null and busrede<>0 then busrede else 0 end)mediaRespTimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and busrede is not null and busrede<>0 then 1 else 0 end)mediaResp,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and busrede is not null and busrede<>0 then 1 else 0 end)ServiceIMTrans,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and busrede is not null and busrede<>0 and DLDATA is not null then dldata else 0 end)ServiceIMFlow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 and busrede is not null and busrede<>0 then (case when (httplastrede-httpfirstrede)<10 then httpfirstrede else (httplastrede-httpfirstrede) end) else 0 end)ServiceIMTime
         |from tb_xdr_ifc_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by imei
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/tac_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

  def cellHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(
      s"""alter table cell_hour_http add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         LOCATION 'hdfs://dtcluster/$warhouseDir/cell_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR'
       """.stripMargin)
    sql(
      s"""
         |select
         |'$cal_date',
         |ecgi,
         |sum(case when interface=11 and apptypecode=103 and apptype=15 then 1 else 0 end)browsedownloadvisits,
         |sum(case when interface=11 and (apptypecode=103 or apptypecode=107) and apptype=5 then 1 else 0 end)videoservicevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=1 then 1 else 0 end)instantmessagevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=7 then 1 else 0 end)appvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )browsedownloadbusiness,
         |sum(case when Interface = 11 and (APPTYPECODE = 103 or APPTYPECODE = 107)and APPTYPE = 5 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )videoservicebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )instantmessagebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 7 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )appbusiness,
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
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and httplastrede is not null and httplastrede<>0 and DLDATA is not null then DLData else 0 end)httpdownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then (CASE WHEN (HTTPLASTREDE - HTTPFIRSTREDE)<10 then 10 else (HTTPLASTREDE - HTTPFIRSTREDE) END) end)httpdowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS = 0  then 1 else 0 end)mediasucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 then 1 else 0 end)mediareq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS=0 and DLDATA is not null then DLData else 0 end )mediadownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS=0 then (procedureendtime - procedurestarttime) else 0 end)mediadowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and appstatus=0  then 1 else 0 end)ServiceIMSucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 then 1 else 0 end)ServiceIMReq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 2  then 1 else 0 end)readvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 3  then 1 else 0 end)wbvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 4  then 1 else 0 end)navigationvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 6  then 1 else 0 end)musicvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 8  then 1 else 0 end)gamevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 9  then 1 else 0 end)payvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 10  then 1 else 0 end)Animevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 11  then 1 else 0 end)mailvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 12  then 1 else 0 end)p2pvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 13  then 1 else 0 end)voipvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 14  then 1 else 0 end)MultimediaMsgvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 16 then 1 else 0 end)financialvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 17 then 1 else 0 end)securityvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 18 then 1 else 0 end)shoppingvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 19 then 1 else 0 end)travelvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 20 then 1 else 0 end)cloudstoragevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 21 then 1 else 0 end)othervisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 2  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)readbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 3  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)wbbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 4  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)navigationbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 6  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)musicbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 8  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)gamebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 9  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)paybusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 10 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)Animebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 11 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)mailbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 12 and DLDATA is not null and ULDATA is not null  then (uldata+dldata) else 0 end)p2pbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 13 and DLDATA is not null and ULDATA is not null  then (uldata+dldata) else 0 end)voipbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 14 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)MultimediaMsgbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 16 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)financialbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 17 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)securitybusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 18 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)shoppingbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 19 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)travelbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 20 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)cloudstoragebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 21 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)otherbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and busrede is not null and busrede<>0 then busrede else 0 end)mediaRespTimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and busrede is not null and busrede<>0 then 1 else 0 end)mediaResp,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and busrede is not null and busrede<>0 then 1 else 0 end)ServiceIMTrans,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and busrede is not null and busrede<>0 and DLDATA is not null then dldata else 0 end)ServiceIMFlow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 and busrede is not null and busrede<>0 then (case when (httplastrede-httpfirstrede)<10 then httpfirstrede else (httplastrede-httpfirstrede) end) else 0 end)ServiceIMTime
         |from tb_xdr_ifc_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by ecgi

       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/cell_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR")

  }

  def spHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(
      s"""alter table sp_hour_http add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         LOCATION 'hdfs://dtcluster/$warhouseDir/sp_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR'
       """.stripMargin)
    sql(
      s"""
         |select
         |'$cal_date',
         |appserveripipv4,
         |sum(case when interface=11 and apptypecode=103 and apptype=15 then 1 else 0 end)browsedownloadvisits,
         |sum(case when interface=11 and (apptypecode=103 or apptypecode=107) and apptype=5 then 1 else 0 end)videoservicevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=1 then 1 else 0 end)instantmessagevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=7 then 1 else 0 end)appvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )browsedownloadbusiness,
         |sum(case when Interface = 11 and (APPTYPECODE = 103 or APPTYPECODE = 107)and APPTYPE = 5 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )videoservicebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )instantmessagebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 7 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )appbusiness,
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
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and httplastrede is not null and httplastrede<>0 and DLDATA is not null then DLData else 0 end)httpdownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then (CASE WHEN (HTTPLASTREDE - HTTPFIRSTREDE)<10 then 10 else (HTTPLASTREDE - HTTPFIRSTREDE) END) end)httpdowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS = 0  then 1 else 0 end)mediasucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 then 1 else 0 end)mediareq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS=0 and DLDATA is not null then DLData else 0 end )mediadownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS=0 then (procedureendtime - procedurestarttime) else 0 end)mediadowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and appstatus=0  then 1 else 0 end)ServiceIMSucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 then 1 else 0 end)ServiceIMReq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 2  then 1 else 0 end)readvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 3  then 1 else 0 end)wbvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 4  then 1 else 0 end)navigationvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 6  then 1 else 0 end)musicvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 8  then 1 else 0 end)gamevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 9  then 1 else 0 end)payvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 10  then 1 else 0 end)Animevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 11  then 1 else 0 end)mailvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 12  then 1 else 0 end)p2pvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 13  then 1 else 0 end)voipvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 14  then 1 else 0 end)MultimediaMsgvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 16 then 1 else 0 end)financialvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 17 then 1 else 0 end)securityvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 18 then 1 else 0 end)shoppingvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 19 then 1 else 0 end)travelvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 20 then 1 else 0 end)cloudstoragevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 21 then 1 else 0 end)othervisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 2  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)readbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 3  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)wbbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 4  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)navigationbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 6  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)musicbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 8  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)gamebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 9  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)paybusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 10 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)Animebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 11 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)mailbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 12 and DLDATA is not null and ULDATA is not null  then (uldata+dldata) else 0 end)p2pbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 13 and DLDATA is not null and ULDATA is not null  then (uldata+dldata) else 0 end)voipbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 14 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)MultimediaMsgbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 16 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)financialbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 17 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)securitybusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 18 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)shoppingbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 19 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)travelbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 20 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)cloudstoragebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 21 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)otherbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and busrede is not null and busrede<>0 then busrede else 0 end)mediaRespTimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and busrede is not null and busrede<>0 then 1 else 0 end)mediaResp,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and busrede is not null and busrede<>0 then 1 else 0 end)ServiceIMTrans,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and busrede is not null and busrede<>0 and DLDATA is not null then dldata else 0 end)ServiceIMFlow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 and busrede is not null and busrede<>0 then (case when (httplastrede-httpfirstrede)<10 then httpfirstrede else (httplastrede-httpfirstrede) end) else 0 end)ServiceIMTime
         |from tb_xdr_ifc_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by appserveripipv4
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/sp_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

  def ueHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(
      s"""alter table ue_hour_http add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         LOCATION 'hdfs://dtcluster/$warhouseDir/ue_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR'
       """.stripMargin)
    sql(
      s"""
         |select
         |'$cal_date',
         |imsi,
         |msisdn,
         |sum(case when interface=11 and apptypecode=103 and apptype=15 then 1 else 0 end)browsedownloadvisits,
         |sum(case when interface=11 and (apptypecode=103 or apptypecode=107) and apptype=5 then 1 else 0 end)videoservicevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=1 then 1 else 0 end)instantmessagevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=7 then 1 else 0 end)appvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )browsedownloadbusiness,
         |sum(case when Interface = 11 and (APPTYPECODE = 103 or APPTYPECODE = 107)and APPTYPE = 5 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )videoservicebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )instantmessagebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 7 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )appbusiness,
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
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and httplastrede is not null and httplastrede<>0 and DLDATA is not null then DLData else 0 end)httpdownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then (CASE WHEN (HTTPLASTREDE - HTTPFIRSTREDE)<10 then 10 else (HTTPLASTREDE - HTTPFIRSTREDE) END) end)httpdowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS = 0  then 1 else 0 end)mediasucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 then 1 else 0 end)mediareq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS=0 and DLDATA is not null then DLData else 0 end )mediadownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS=0 then (procedureendtime - procedurestarttime) else 0 end)mediadowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and appstatus=0  then 1 else 0 end)ServiceIMSucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 then 1 else 0 end)ServiceIMReq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 2  then 1 else 0 end)readvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 3  then 1 else 0 end)wbvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 4  then 1 else 0 end)navigationvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 6  then 1 else 0 end)musicvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 8  then 1 else 0 end)gamevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 9  then 1 else 0 end)payvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 10  then 1 else 0 end)Animevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 11  then 1 else 0 end)mailvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 12  then 1 else 0 end)p2pvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 13  then 1 else 0 end)voipvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 14  then 1 else 0 end)MultimediaMsgvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 16 then 1 else 0 end)financialvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 17 then 1 else 0 end)securityvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 18 then 1 else 0 end)shoppingvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 19 then 1 else 0 end)travelvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 20 then 1 else 0 end)cloudstoragevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 21 then 1 else 0 end)othervisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 2  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)readbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 3  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)wbbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 4  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)navigationbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 6  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)musicbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 8  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)gamebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 9  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)paybusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 10 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)Animebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 11 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)mailbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 12 and DLDATA is not null and ULDATA is not null  then (uldata+dldata) else 0 end)p2pbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 13 and DLDATA is not null and ULDATA is not null  then (uldata+dldata) else 0 end)voipbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 14 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)MultimediaMsgbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 16 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)financialbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 17 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)securitybusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 18 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)shoppingbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 19 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)travelbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 20 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)cloudstoragebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 21 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)otherbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and busrede is not null and busrede<>0 then busrede else 0 end)mediaRespTimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and busrede is not null and busrede<>0 then 1 else 0 end)mediaResp,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and busrede is not null and busrede<>0 then 1 else 0 end)ServiceIMTrans,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and busrede is not null and busrede<>0 and DLDATA is not null then dldata else 0 end)ServiceIMFlow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 and busrede is not null and busrede<>0 then (case when (httplastrede-httpfirstrede)<10 then httpfirstrede else (httplastrede-httpfirstrede) end) else 0 end)ServiceIMTime
         |from tb_xdr_ifc_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by imsi,msisdn
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/ue_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

  def sgwHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(
      s"""alter table sgw_hour_http add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         LOCATION 'hdfs://dtcluster/$warhouseDir/sgw_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR'
       """.stripMargin)
    sql(
      s"""
         |select
         |'$cal_date',
         |sgwipaddr,
         |sum(case when interface=11 and apptypecode=103 and apptype=15 then 1 else 0 end)browsedownloadvisits,
         |sum(case when interface=11 and (apptypecode=103 or apptypecode=107) and apptype=5 then 1 else 0 end)videoservicevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=1 then 1 else 0 end)instantmessagevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=7 then 1 else 0 end)appvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )browsedownloadbusiness,
         |sum(case when Interface = 11 and (APPTYPECODE = 103 or APPTYPECODE = 107)and APPTYPE = 5 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )videoservicebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )instantmessagebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 7 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )appbusiness,
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
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and httplastrede is not null and httplastrede<>0 and DLDATA is not null then DLData else 0 end)httpdownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then (CASE WHEN (HTTPLASTREDE - HTTPFIRSTREDE)<10 then 10 else (HTTPLASTREDE - HTTPFIRSTREDE) END) end)httpdowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS = 0  then 1 else 0 end)mediasucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 then 1 else 0 end)mediareq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS=0 and DLDATA is not null then DLData else 0 end )mediadownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS=0 then (procedureendtime - procedurestarttime) else 0 end)mediadowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and appstatus=0  then 1 else 0 end)ServiceIMSucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 then 1 else 0 end)ServiceIMReq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 2  then 1 else 0 end)readvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 3  then 1 else 0 end)wbvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 4  then 1 else 0 end)navigationvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 6  then 1 else 0 end)musicvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 8  then 1 else 0 end)gamevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 9  then 1 else 0 end)payvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 10  then 1 else 0 end)Animevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 11  then 1 else 0 end)mailvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 12  then 1 else 0 end)p2pvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 13  then 1 else 0 end)voipvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 14  then 1 else 0 end)MultimediaMsgvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 16 then 1 else 0 end)financialvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 17 then 1 else 0 end)securityvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 18 then 1 else 0 end)shoppingvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 19 then 1 else 0 end)travelvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 20 then 1 else 0 end)cloudstoragevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 21 then 1 else 0 end)othervisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 2  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)readbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 3  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)wbbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 4  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)navigationbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 6  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)musicbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 8  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)gamebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 9  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)paybusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 10 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)Animebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 11 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)mailbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 12 and DLDATA is not null and ULDATA is not null  then (uldata+dldata) else 0 end)p2pbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 13 and DLDATA is not null and ULDATA is not null  then (uldata+dldata) else 0 end)voipbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 14 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)MultimediaMsgbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 16 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)financialbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 17 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)securitybusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 18 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)shoppingbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 19 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)travelbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 20 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)cloudstoragebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 21 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)otherbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and busrede is not null and busrede<>0 then busrede else 0 end)mediaRespTimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and busrede is not null and busrede<>0 then 1 else 0 end)mediaResp,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and busrede is not null and busrede<>0 then 1 else 0 end)ServiceIMTrans,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and busrede is not null and busrede<>0 and DLDATA is not null then dldata else 0 end)ServiceIMFlow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 and busrede is not null and busrede<>0 then (case when (httplastrede-httpfirstrede)<10 then httpfirstrede else (httplastrede-httpfirstrede) end) else 0 end)ServiceIMTime
         |from tb_xdr_ifc_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by sgwipaddr
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/sgw_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

  def imsicellHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(
      s"""alter table imsi_cell_hour_http add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         LOCATION 'hdfs://dtcluster/$warhouseDir/imsi_cell_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR'
       """.stripMargin)
    sql(
      s"""
         |select
         |'$cal_date',
         |imsi,
         |msisdn,
         |ecgi,
         |sum(case when interface=11 and apptypecode=103 and apptype=15 then 1 else 0 end)browsedownloadvisits,
         |sum(case when interface=11 and (apptypecode=103 or apptypecode=107) and apptype=5 then 1 else 0 end)videoservicevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=1 then 1 else 0 end)instantmessagevisits,
         |sum(case when interface=11 and apptypecode=103 and apptype=7 then 1 else 0 end)appvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )browsedownloadbusiness,
         |sum(case when Interface = 11 and (APPTYPECODE = 103 or APPTYPECODE = 107)and APPTYPE = 5 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )videoservicebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )instantmessagebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 7 and DLDATA is not null and ULDATA is not null then DLDATA+ULDATA else 0 end )appbusiness,
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
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE=15 and httplastrede is not null and httplastrede<>0 and DLDATA is not null then DLData else 0 end)httpdownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and HTTPLASTREDE != 0 and HTTPLASTREDE is not null then (CASE WHEN (HTTPLASTREDE - HTTPFIRSTREDE)<10 then 10 else (HTTPLASTREDE - HTTPFIRSTREDE) END) end)httpdowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS = 0  then 1 else 0 end)mediasucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 then 1 else 0 end)mediareq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS=0 and DLDATA is not null then DLData else 0 end )mediadownflow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and APPSTATUS=0 then (procedureendtime - procedurestarttime) else 0 end)mediadowntime,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and appstatus=0  then 1 else 0 end)ServiceIMSucc,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 then 1 else 0 end)ServiceIMReq,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 2  then 1 else 0 end)readvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 3  then 1 else 0 end)wbvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 4  then 1 else 0 end)navigationvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 6  then 1 else 0 end)musicvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 8  then 1 else 0 end)gamevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 9  then 1 else 0 end)payvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 10  then 1 else 0 end)Animevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 11  then 1 else 0 end)mailvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 12  then 1 else 0 end)p2pvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 13  then 1 else 0 end)voipvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 14  then 1 else 0 end)MultimediaMsgvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 16 then 1 else 0 end)financialvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 17 then 1 else 0 end)securityvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 18 then 1 else 0 end)shoppingvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 19 then 1 else 0 end)travelvisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 20 then 1 else 0 end)cloudstoragevisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 21 then 1 else 0 end)othervisits,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 2  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)readbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 3  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)wbbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 4  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)navigationbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 6  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)musicbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 8  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)gamebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 9  and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)paybusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 10 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)Animebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 11 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)mailbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 12 and DLDATA is not null and ULDATA is not null  then (uldata+dldata) else 0 end)p2pbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 13 and DLDATA is not null and ULDATA is not null  then (uldata+dldata) else 0 end)voipbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 14 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)MultimediaMsgbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 16 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)financialbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 17 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)securitybusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 18 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)shoppingbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 19 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)travelbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 20 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)cloudstoragebusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 21 and DLDATA is not null and ULDATA is not null then (uldata+dldata) else 0 end)otherbusiness,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and busrede is not null and busrede<>0 then busrede else 0 end)mediaRespTimeall,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 5 and busrede is not null and busrede<>0 then 1 else 0 end)mediaResp,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and busrede is not null and busrede<>0 then 1 else 0 end)ServiceIMTrans,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 1 and busrede is not null and busrede<>0 and DLDATA is not null then dldata else 0 end)ServiceIMFlow,
         |sum(case when Interface = 11 and APPTYPECODE = 103 and APPTYPE = 15 and busrede is not null and busrede<>0 then (case when (httplastrede-httpfirstrede)<10 then httpfirstrede else (httplastrede-httpfirstrede) end) else 0 end)ServiceIMTime
         |from tb_xdr_ifc_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by imsi,msisdn,ecgi
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/imsi_cell_hour_http/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

}

