package com.dtmobile.spark.biz.nssp

import com.dtmobile.util.DateUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * QueryPt
  *
  * @author heyongjin
  * @ create 2017/08/15 9:17
  *
  **/
class QueryPt(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DCL: String, DDB: String, DDBDIR: String) {
  val startTimes = DateUtils.convert(s"""$ANALY_DATE $ANALY_HOUR:00:00""", "yyyyMMdd HH:mm:ss").getTime
  var startTime = startTimes
  var endTime = startTime + 300000
  val arr = Array("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")

  def analyse(implicit sparkSession: SparkSession): Unit = {
    arr.foreach(query(sparkSession, _, "lte"))

    startTime = startTimes
    endTime = startTime + 300000
    arr.foreach(query(sparkSession, _, "x2"))

    startTime = startTimes
    endTime = startTime + 300000
    arr.foreach(query(sparkSession, _, "uu"))

    startTime = startTimes
    endTime = startTime + 300000
    arr.foreach(query(sparkSession, _, "s1mme"))

    startTime = startTimes
    endTime = startTime + 300000
    arr.foreach(query(sparkSession, _, "sv"))

    startTime = startTimes
    endTime = startTime + 300000
    arr.foreach(query(sparkSession, _, "gx"))

    startTime = startTimes
    endTime = startTime + 300000
    arr.foreach(query(sparkSession, _, "mw"))

    arr.foreach(unionAll(sparkSession, _))
  }

  def query(sparkSession: SparkSession, MIN: String, interfaces: String): Unit = {
    import sparkSession.sql
    //缓存数据
    if ("01".equals(MIN)) {
      startTime = startTimes - 600000
      if ("lte".equals(interfaces)) {
        sql(s"use $DCL")
        sql(s"select * from lte_mro_source where dt=$ANALY_DATE and h=$ANALY_HOUR").createOrReplaceTempView("lte_mro_source_cache")
        sparkSession.sqlContext.cacheTable("lte_mro_source_cache")
      } else if ("x2".equals(interfaces)) {
        sql(s"use $DCL")
        sql(s"select * from tb_xdr_ifc_x2 where dt=$ANALY_DATE and h=$ANALY_HOUR").createOrReplaceTempView("tb_xdr_ifc_x2_cache")
        sparkSession.sqlContext.cacheTable("tb_xdr_ifc_x2_cache")
      } else if ("uu".equals(interfaces)) {
        sql(s"use $DCL")
        sql(s"select * from tb_xdr_ifc_uu where dt=$ANALY_DATE and h=$ANALY_HOUR").createOrReplaceTempView("tb_xdr_ifc_uu_cache")
        sparkSession.sqlContext.cacheTable("tb_xdr_ifc_uu_cache")
      } else if ("s1mme".equals(interfaces)) {
        sql(s"use $SDB")
        sql(s"select * from tb_xdr_ifc_s1mme where dt=$ANALY_DATE and h=$ANALY_HOUR").createOrReplaceTempView("tb_xdr_ifc_s1mme_cache")
        sparkSession.sqlContext.cacheTable("tb_xdr_ifc_s1mme_cache")
      } else if ("sv".equals(interfaces)) {
        sql(s"use $SDB")
        sql(s"select * from tb_xdr_ifc_sv where dt=$ANALY_DATE and h=$ANALY_HOUR").createOrReplaceTempView("tb_xdr_ifc_sv_cache")
        sparkSession.sqlContext.cacheTable("tb_xdr_ifc_sv_cache")
      } else if ("gx".equals(interfaces)) {
        sql(s"use $SDB")
        sql(s"select * from tb_xdr_ifc_gxrx where dt=$ANALY_DATE and h=$ANALY_HOUR").createOrReplaceTempView("tb_xdr_ifc_gxrx_cache")
        sparkSession.sqlContext.cacheTable("tb_xdr_ifc_gxrx_cache")
      } else if ("mw".equals(interfaces)) {
        sql(s"use $SDB")
        sql(s"select * from tb_xdr_ifc_mw where dt=$ANALY_DATE and h=$ANALY_HOUR").createOrReplaceTempView("tb_xdr_ifc_mw_cache")
        sparkSession.sqlContext.cacheTable("tb_xdr_ifc_mw_cache")
      }
    }else  if ("12".equals(MIN)) {
      endTime = endTime + 600000
    }else{
      startTime = endTime
      endTime = startTime + 300000
    }

    sql(s"use $DDB")
    sql(s"alter table lte_mro_source add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR,m=$MIN)")
    sql(s"alter table tb_xdr_ifc_uu add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR,m=$MIN)")
    sql(s"alter table tb_xdr_ifc_x2 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR,m=$MIN)")
    sql(s"alter table tb_xdr_ifc_s1mme add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR,m=$MIN)")
    sql(s"alter table tb_xdr_ifc_sv add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR,m=$MIN)")
    sql(s"alter table tb_xdr_ifc_gxrx add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR,m=$MIN)")
    sql(s"alter table tb_xdr_ifc_mw add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR,m=$MIN)")
    sql(s"alter table tb_xdr_ifc_all add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR,m=$MIN)")

    if ("lte".equals(interfaces)) {
      sql(s"use $DCL")
      sql(
        s"""
           |select
           |objectid
           |,vid
           |,fileformatversion
           |,starttime
           |,endtime
           |,period
           |,enbid
           |,userlabel
           |,mrname
           |,cellid
           |,earfcn
           |,subframenbr
           |,prbnbr
           |,mmeues1apid
           |,mmegroupid
           |,mmecode
           |,meatime
           |,eventtype
           |,gridcenterlongitude
           |,gridcenterlatitude
           |,kpi1
           |,kpi2
           |,kpi3
           |,kpi4
           |,kpi5
           |,kpi6
           |,kpi7
           |,kpi8
           |,kpi9
           |,kpi10
           |,kpi11
           |,kpi12
           |,kpi13
           |,kpi14
           |,kpi15
           |,kpi16
           |,kpi17
           |,kpi18
           |,kpi19
           |,kpi20
           |,kpi21
           |,kpi22
           |,kpi23
           |,kpi24
           |,kpi25
           |,kpi26
           |,kpi27
           |,kpi28
           |,kpi29
           |,kpi30
           |,kpi31
           |,kpi32
           |,kpi33
           |,kpi34
           |,kpi35
           |,kpi36
           |,kpi37
           |,kpi38
           |,kpi39
           |,kpi40
           |,kpi41
           |,kpi42
           |,kpi43
           |,kpi44
           |,kpi45
           |,kpi46
           |,kpi47
           |,kpi48
           |,kpi49
           |,kpi50
           |,kpi51
           |,kpi52
           |,kpi53
           |,kpi54
           |,kpi55
           |,kpi56
           |,kpi57
           |,kpi58
           |,kpi59
           |,kpi60
           |,kpi61
           |,kpi62
           |,kpi63
           |,kpi64
           |,kpi65
           |,kpi66
           |,kpi67
           |,kpi68
           |,kpi69
           |,kpi70
           |,kpi71
           |,length
           |,city
           |,xdrtype
           |,interface
           |,xdrid
           |,rat
           |,imsi
           |,imei
           |,msisdn
           |,mrtype
           |,neighborcellnumber
           |,gsmneighborcellnumber
           |,tdsneighborcellnumber
           |,v_enb
           |,mrtime
           |from lte_mro_source_cache
           |where  mrtime>=$startTime and mrtime<$endTime
       """.stripMargin).repartition(100).write.mode(SaveMode.Overwrite)
        .parquet(s"$DDBDIR/lte_mro_source/dt=$ANALY_DATE/h=$ANALY_HOUR/m=$MIN")
    } else if ("x2".equals(interfaces)) {
      sql(s"use $DCL")
      sql(
        s"""
           |select
           |length
           |,city
           |,interface
           |,xdrid
           |,rat
           |,imsi
           |,imei
           |,msisdn
           |,proceduretype
           |,procedurestarttime
           |,procedureendtime
           |,procedurestatus
           |,cellid
           |,targetcellid
           |,enbid
           |,targetenbid
           |,mmeues1apid
           |,mmegroupid
           |,mmecode
           |,requestcause
           |,failurecause
           |,epsbearernumber
           |,bearer0id
           |,bearer0status
           |,bearer1id
           |,bearer1status
           |,bearer2id
           |,bearer2status
           |,bearer3id
           |,bearer3status
           |,bearer4id
           |,bearer4status
           |,bearer5id
           |,bearer5status
           |,bearer6id
           |,bearer6status
           |,bearer7id
           |,bearer7status
           |,bearer8id
           |,bearer8status
           |,bearer9id
           |,bearer9status
           |,bearer10id
           |,bearer10status
           |,bearer11id
           |,bearer11status
           |,bearer12id
           |,bearer12status
           |,bearer13id
           |,bearer13status
           |,bearer14id
           |,bearer14status
           |,bearer15id
           |,bearer15status
           |,rangetime
           |from tb_xdr_ifc_x2_cache
           |where  procedurestarttime>=$startTime and procedurestarttime<$endTime
       """.stripMargin).repartition(100).write.mode(SaveMode.Overwrite)
        .parquet(s"$DDBDIR/tb_xdr_ifc_x2/dt=$ANALY_DATE/h=$ANALY_HOUR/m=$MIN")
    } else if ("uu".equals(interfaces)) {
      sql(s"use $DCL")
      sql(
        s"""
           |select
           |length
           |,city
           |,interface
           |,xdrid
           |,rat
           |,imsi
           |,imei
           |,msisdn
           |,proceduretype
           |,procedurestarttime
           |,procedureendtime
           |,keyword1
           |,keyword2
           |,procedurestatus
           |,plmnid
           |,enbid
           |,cellid
           |,crnti
           |,targetenbid
           |,targetcellid
           |,targetcrnti
           |,mmeues1apid
           |,mmegroupid
           |,mmecode
           |,mtmsi
           |,csfbindication
           |,redirectednetwork
           |,epsbearernumber
           |,bearer0id
           |,bearer0status
           |,bearer1id
           |,bearer1status
           |,bearer2id
           |,bearer2status
           |,bearer3id
           |,bearer3status
           |,bearer4id
           |,bearer4status
           |,bearer5id
           |,bearer5status
           |,bearer6id
           |,bearer6status
           |,bearer7id
           |,bearer7status
           |,bearer8id
           |,bearer8status
           |,bearer9id
           |,bearer9status
           |,bearer10id
           |,bearer10status
           |,bearer11id
           |,bearer11status
           |,bearer12id
           |,bearer12status
           |,bearer13id
           |,bearer13status
           |,bearer14id
           |,bearer14status
           |,bearer15id
           |,bearer15status
           |,rangetime
           |from tb_xdr_ifc_uu_cache
           |where  procedurestarttime>=$startTime and procedurestarttime<$endTime
       """.stripMargin).repartition(100).write.mode(SaveMode.Overwrite)
        .parquet(s"$DDBDIR/tb_xdr_ifc_uu/dt=$ANALY_DATE/h=$ANALY_HOUR/m=$MIN")
    } else if ("s1mme".equals(interfaces)) {
      sql(s"use $SDB")
      sql(
        s"""
           |select
           |length_s
           |,local_province_s
           |,local_city_s
           |,owner_province_s
           |,owner_city_s
           |,roaming_type_s
           |,interface_s
           |,xdrid_s
           |,rat_s
           |,imsi_s
           |,imei_s
           |,msisdn_s
           |,procedure_type_s
           |,start_time_s
           |,end_time_s
           |,start_lon_s
           |,start_lat_s
           |,location_source_s
           |,msgflag_s
           |,procedure_status_s
           |,req_cause_g_s
           |,request_cause_s
           |,fail_cause_g_s
           |,failure_cause_s
           |,keyword1_s
           |,keyword2_s
           |,keyword3_s
           |,keyword4_s
           |,mme_ue_s1apid_s
           |,old_mme_groupid_s
           |,old_mmecode_s
           |,old_mtmsi_s
           |,old_guti_type_s
           |,mme_groupid_s
           |,mmecode_s
           |,mtmsi_s
           |,tmsi_s
           |,useripv4_s
           |,useripv6_s
           |,mme_ip_s
           |,enb_ip_s
           |,mmeport_s
           |,enbport_s
           |,tac_s
           |,eci_s
           |,other_tac_s
           |,other_eci_s
           |,apn_s
           |,epsbearernumber
           |,bearer0id
           |,bearer0type
           |,bearer0qci
           |,bearer0status
           |,bearer0requestcause
           |,bearer0failurecause
           |,bearer0enbgtpteid
           |,bearer0sgwgtpteid
           |,bearer1id
           |,bearer1type
           |,bearer1qci
           |,bearer1status
           |,bearer1requestcause
           |,bearer1failurecause
           |,bearer1enbgtpteid
           |,bearer1sgwgtpteid
           |,bearer2id
           |,bearer2type
           |,bearer2qci
           |,bearer2status
           |,bearer2requestcause
           |,bearer2failurecause
           |,bearer2enbgtpteid
           |,bearer2sgwgtpteid
           |,bearer3id
           |,bearer3type
           |,bearer3qci
           |,bearer3status
           |,bearer3requestcause
           |,bearer3failurecause
           |,bearer3enbgtpteid
           |,bearer3sgwgtpteid
           |,bearer4id
           |,bearer4type
           |,bearer4qci
           |,bearer4status
           |,bearer4requestcause
           |,bearer4failurecause
           |,bearer4enbgtpteid
           |,bearer4sgwgtpteid
           |,bearer5id
           |,bearer5type
           |,bearer5qci
           |,bearer5status
           |,bearer5requestcause
           |,bearer5failurecause
           |,bearer5enbgtpteid
           |,bearer5sgwgtpteid
           |,bearer6id
           |,bearer6type
           |,bearer6qci
           |,bearer6status
           |,bearer6requestcause
           |,bearer6failurecause
           |,bearer6enbgtpteid
           |,bearer6sgwgtpteid
           |,bearer7id
           |,bearer7type
           |,bearer7qci
           |,bearer7status
           |,bearer7requestcause
           |,bearer7failurecause
           |,bearer7enbgtpteid
           |,bearer7sgwgtpteid
           |,bearer8id
           |,bearer8type
           |,bearer8qci
           |,bearer8status
           |,bearer8requestcause
           |,bearer8failurecause
           |,bearer8enbgtpteid
           |,bearer8sgwgtpteid
           |,bearer9id
           |,bearer9type
           |,bearer9qci
           |,bearer9status
           |,bearer9requestcause
           |,bearer9failurecause
           |,bearer9enbgtpteid
           |,bearer9sgwgtpteid
           |,bearer10id
           |,bearer10type
           |,bearer10qci
           |,bearer10status
           |,bearer10requestcause
           |,bearer10failurecause
           |,bearer10enbgtpteid
           |,bearer10sgwgtpteid
           |,bearer11id
           |,bearer11type
           |,bearer11qci
           |,bearer11status
           |,bearer11requestcause
           |,bearer11failurecause
           |,bearer11enbgtpteid
           |,bearer11sgwgtpteid
           |,bearer12id
           |,bearer12type
           |,bearer12qci
           |,bearer12status
           |,bearer12requestcause
           |,bearer12failurecause
           |,bearer12enbgtpteid
           |,bearer12sgwgtpteid
           |,bearer13id
           |,bearer13type
           |,bearer13qci
           |,bearer13status
           |,bearer13requestcause
           |,bearer13failurecause
           |,bearer13enbgtpteid
           |,bearer13sgwgtpteid
           |,bearer14id
           |,bearer14type
           |,bearer14qci
           |,bearer14status
           |,bearer14requestcause
           |,bearer14failurecause
           |,bearer14enbgtpteid
           |,bearer14sgwgtpteid
           |,bearer15id
           |,bearer15type
           |,bearer15qci
           |,bearer15status
           |,bearer15requestcause
           |,bearer15failurecause
           |,bearer15enbgtpteid
           |,bearer15sgwgtpteid
           |from tb_xdr_ifc_s1mme_cache
           |where  procedurestarttime>=$startTime and procedurestarttime<$endTime
       """.stripMargin).repartition(100).write.mode(SaveMode.Overwrite)
        .parquet(s"$DDBDIR/tb_xdr_ifc_s1mme/dt=$ANALY_DATE/h=$ANALY_HOUR/m=$MIN")
    } else if ("sv".equals(interfaces)) {
      sql(s"use $SDB")
      sql(
        s"""
           |select
           |length_s
           |,city_s
           |,interface_s
           |,xdr_id_s
           |,rat_s
           |,imsi_s
           |,imei_s
           |,msisdn_s
           |,procedure_type_s
           |,procedure_start_time_s
           |,procedure_end_time_s
           |,source_ne_ip_s
           |,source_ne_port_s
           |,dest_ne_ip_s
           |,dest_ne_port_s
           |,roam_direction_s
           |,home_mcc_s
           |,home_mnc_s
           |,mcc_s
           |,mnc_s
           |,target_lac_s
           |,source_tac_s
           |,source_eci_s
           |,sv_flags_s
           |,ul_c_msc_ip_s
           |,dl_c_mme_ip_s
           |,ul_c_msc_teid_s
           |,dl_c_mme_teid_s
           |,stn_sr_s
           |,target_rnc_id_s
           |,target_cell_id_s
           |,arp_s
           |,request_result_s
           |,result_s
           |,sv_cause_s
           |,post_failure_cause_s
           |,resp_delay_s
           |,sv_delay_s
           |,rangetime
           |from tb_xdr_ifc_sv_cache
           |where  procedurestarttime>=$startTime and procedurestarttime<$endTime
       """.stripMargin).repartition(100).write.mode(SaveMode.Overwrite)
        .parquet(s"$DDBDIR/tb_xdr_ifc_sv/dt=$ANALY_DATE/h=$ANALY_HOUR/m=$MIN")
    } else if ("gx".equals(interfaces)) {
      sql(s"use $SDB")
      sql(
        s"""
           |select
           |length_s
           |,city_s
           |,interface_s
           |,xdr_id_s
           |,rat_s
           |,imsi_s
           |,imei_s
           |,msisdn_s
           |,procedure_type_s
           |,procedure_start_time_s
           |,procedure_end_time_s
           |,icid_s
           |,origin_realm_s
           |,destination_realm_s
           |,origin_host_s
           |,destination_host_s
           |,sgsn_sgw_sig_ip_s
           |,af_app_id_s
           |,cc_request_type_s
           |,rx_request_type_s
           |,media_type_s
           |,abort_cause_s
           |,result_code_s
           |,experimental_result_code_s
           |,session_release_cause_s
           |,rule_failure_code_s
           |,session_id_s
           |,called_station_id_s
           |,framed_ipv6_prefix_s
           |,framed_ip_address_s
           |,source_eci_s
           |,source_tac_s
           |,source_ne_ip_s
           |,source_ne_port_s
           |,destination_ne_ip_s
           |,destination_ne_port_s
           |,qci_s
           |from tb_xdr_ifc_gxrx_cache
           |where  procedurestarttime>=$startTime and procedurestarttime<$endTime
       """.stripMargin).repartition(100).write.mode(SaveMode.Overwrite)
        .parquet(s"$DDBDIR/tb_xdr_ifc_gxrx/dt=$ANALY_DATE/h=$ANALY_HOUR/m=$MIN")
    } else if ("mw".equals(interfaces)) {
      sql(s"use $SDB")
      sql(
        s"""
           |select
           |length_s
           |,city_s
           |,interface_s
           |,xdr_id_s
           |,rat_s
           |,imsi_s
           |,imei_s
           |,msisdn_s
           |,procedure_type_s
           |,procedure_start_time_s
           |,procedure_end_time_s
           |,service_type_s
           |,procedure_status_s
           |,calling_number_s
           |,called_number_s
           |,calling_party_uri_s
           |,request_uri_s
           |,user_ip_s
           |,callid_s
           |,icid_s
           |,source_ne_ip_s
           |,source_ne_port_s
           |,dest_ne_ip_s
           |,dest_ne_port_s
           |,call_side_s
           |,source_access_type_s
           |,source_eci_s
           |,source_tac_s
           |,source_imsi_s
           |,source_imei_s
           |,dest_access_type_s
           |,dest_eci_s
           |,dest_tac_s
           |,dest_imsi_s
           |,dest_imei_s
           |,auth_type_s
           |,expires_time_req_s
           |,expires_time_rsp_s
           |,calling_sdp_ip_addr_s
           |,calling_audio_sdp_port_s
           |,calling_video_sdp_port_s
           |,called_sdp_ip_addr_s
           |,called_audio_sdp_port_s
           |,called_video_port_s
           |,audio_codec_s
           |,video_codec_s
           |,redirecting_party_address_s
           |,original_party_address_s
           |,redirect_reason_s
           |,response_code_s
           |,finish_warning_code_s
           |,finish_reason_protocol_s
           |,finish_reason_cause_s
           |,first_fail_time_s
           |,first_fail_ne_ip_s
           |,first_fail_transaction_s
           |,progress_time_s
           |,update_time_s
           |,alerting_time_s
           |,answer_time_s
           |,release_time_s
           |,call_duration_s
           |,auth_req_time_s
           |,auth_rsp_time_s
           |,stn_sr_s
           |,atcf_mgmt_s
           |,atu_sti_s
           |,c_msisdn_s
           |,ssi_s
           |,sbc_domain_s
           |,multiparty_call_status_s
           |,retryafter_s
           |,release_part_s
           |,finish_warning_s
           |,finish_reason_s
           |,nonce_value_s
           |,auth_response_s
           |,media_s
           |,user_agent_s
           |,executed_service_s
           |,enb_ip_s
           |,egw_ip_s
           |from tb_xdr_ifc_mw_cache
           |where  procedurestarttime>=$startTime and procedurestarttime<$endTime
       """.stripMargin).repartition(100).write.mode(SaveMode.Overwrite)
        .parquet(s"$DDBDIR/tb_xdr_ifc_mw/dt=$ANALY_DATE/h=$ANALY_HOUR/m=$MIN")
    }

    //制裁数据
    if ("12".equals(MIN)) {
      if ("lte".equals(interfaces)) {
        sparkSession.sqlContext.uncacheTable("lte_mro_source_cache")
      } else if ("x2".equals(interfaces)) {
        sparkSession.sqlContext.uncacheTable("tb_xdr_ifc_x2_cache")
      } else if ("uu".equals(interfaces)) {
        sparkSession.sqlContext.uncacheTable("tb_xdr_ifc_uu_cache")
      } else if ("s1mme".equals(interfaces)) {
        sparkSession.sqlContext.uncacheTable("tb_xdr_ifc_s1mme_cache")
      } else if ("sv".equals(interfaces)) {
        sparkSession.sqlContext.uncacheTable("tb_xdr_ifc_sv_cache")
      } else if ("gx".equals(interfaces)) {
        sparkSession.sqlContext.uncacheTable("tb_xdr_ifc_gxrx_cache")
      } else if ("mw".equals(interfaces)) {
        sparkSession.sqlContext.uncacheTable("tb_xdr_ifc_mw_cache")
      }
    }
  }

  def unionAll(sparkSession: SparkSession, MIN: String): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(
      s"""
         |select length,
         |        city,
         |        interface as interfaces,
         |        xdrid,
         |        rat,
         |        imsi,
         |        imei,
         |        msisdn,
         |        mrtime as prostartTime,
         |        meatime as metaTime,
         |        '' as cnProcedureStatus,
         |        '' as failCause,
         |        cellId,
         |        kpi1 as rsrp,
         |        kpi13 as rsrq,
         |        '' as rip,
         |        mrtype as procedureType,
         |        kpi8 as upsinr,
         |        kpi6 as phr,
         |        vid
         |   from  lte_mro_source
         |  where dt =  $ANALY_DATE
         |    AND h = $ANALY_HOUR
         |    AND m=$MIN
         |    AND mrname = 'MR.LteScRSRP'
         |UNION ALL
         |select length,
         |        city,
         |        interface as interfaces,
         |        xdrid,
         |        rat,
         |        imsi,
         |        imei,
         |        msisdn,
         |        mrtime as prostartTime,
         |        meatime as metaTime,
         |        '' as cnProcedureStatus,
         |        '' as failCause,
         |        cellId,
         |        kpi1 as rsrp,
         |        kpi13 as rsrq,
         |        '' as rip,
         |        mrtype as procedureType,
         |        kpi8 as upsinr,
         |        kpi6 as phr,
         |        vid
         |   from  lte_mro_source
         |  where dt =  $ANALY_DATE
         |    AND h = $ANALY_HOUR
         |    AND m=$MIN
         |    AND mrname = 'MR.LteScRIP0'
         |UNION ALL
         |select length_s as length,
         |        city_s as city,
         |        interface_s as interfaces,
         |        xdr_id_s as xdrid,
         |        rat_s as rat,
         |        imsi_s as imsi,
         |        imei_s as imei,
         |        msisdn_s as msisdn,
         |        procedure_start_time_s as prostartTime,
         |        '' as metaTime,
         |        procedure_status_s as cnProcedureStatus,
         |        response_code_s as failCause,
         |        case
         |          when procedure_type_s = 5 then
         |           case
         |             when call_side_s = 1 then
         |              source_eci_s
         |             else
         |              dest_eci_s
         |           end
         |          else
         |           source_eci_s
         |        end as cellId,
         |        '' as rsrp,
         |        '' as rsrq,
         |        '' as rip,
         |        procedure_type_s as procedureType,
         |        '' as upsinr,
         |        '' as phr,
         |        '' as vid
         |   from  tb_xdr_ifc_mw
         |  where dt =  $ANALY_DATE
         |    AND h = $ANALY_HOUR
         |    AND m=$MIN
         |    AND interface_s = 14
         |UNION ALL
         |select length_s as length,
         |        city_s as city,
         |        interface_s as interfaces,
         |        xdr_id_s as xdrid,
         |        rat_s as rat,
         |        imsi_s as imsi,
         |        imei_s as imei,
         |        msisdn_s as msisdn,
         |        procedure_start_time_s as prostartTime,
         |        '' as metaTime,
         |        procedure_status_s  as cnProcedureStatus,
         |        response_code_s  as failCause,
         |        case
         |          when procedure_type_s = 5 then
         |           case
         |             when call_side_s = 1 then
         |              source_eci_s
         |             else
         |              dest_eci_s
         |           end
         |          else
         |           source_eci_s
         |        end as cellId,
         |        '' as rsrp,
         |        '' as rsrq,
         |        '' as rip,
         |        procedure_type_s as  procedureType,
         |        '' as upsinr,
         |        '' as phr,
         |        '' as vid
         |   from  tb_xdr_ifc_mw
         |  where dt =  $ANALY_DATE
         |    AND h = $ANALY_HOUR
         |    AND m=$MIN
         |    AND interface_s = 18
         |UNION ALL
         |select length_s as  length,
         |        city_s,
         |        interface_s as interfaces,
         |        xdr_id_s as xdrid,
         |        rat_s as rat,
         |        imsi_s as imsi,
         |        imei_s as imei,
         |        msisdn_s as msisdn,
         |        procedure_start_time_s as prostartTime,
         |        '' as metaTime,
         |        procedure_status_s as cnProcedureStatus,
         |        response_code_s as failCause,
         |        case
         |          when procedure_type_s = 5 then
         |           case
         |             when call_side_s = 1 then
         |              source_eci_s
         |             else
         |              dest_eci_s
         |           end
         |          else
         |           source_eci_s
         |        end as cellId,
         |        '' as rsrp,
         |        '' as rsrq,
         |        '' as rip,
         |        procedure_type_s as procedureType,
         |        '' as upsinr,
         |        '' as phr,
         |        '' as vid
         |   from  tb_xdr_ifc_mw
         |  where dt =  $ANALY_DATE
         |    AND h = $ANALY_HOUR
         |    AND m=$MIN
         |    AND interface_s = 15
         |UNION ALL
         |select length_s as length,
         |        city_s as city,
         |        interface_s as interfaces,
         |        xdr_id_s as xdrid,
         |        rat_s as rat,
         |        imsi_s as imsi,
         |        imei_s as imei,
         |        msisdn_s as msisdn,
         |        procedure_start_time_s as prostartTime,
         |        '' as metaTime,
         |        '' as cnProcedureStatus,
         |        result_code_s as failCause,
         |        '' as cellId,
         |        '' as rsrp,
         |        '' as rsrq,
         |        '' as rip,
         |        procedure_type_s as procedureType,
         |        '' as upsinr,
         |        '' as phr,
         |        '' as vid
         |   from  tb_xdr_ifc_gxrx
         |  where dt =  $ANALY_DATE
         |    AND h = $ANALY_HOUR
         |    AND m=$MIN
         |    AND interface_s = 25
         |UNION ALL
         |select length_s as length,
         |        city_s as city,
         |        interface_s as interfaces,
         |        xdr_id_s as xdrid,
         |        rat_s as rat,
         |        imsi_s as imsi,
         |        imei_s as imei,
         |        msisdn_s as msisdn,
         |        procedure_start_time_s as prostartTime,
         |        '' as metaTime,
         |        '' as cnProcedureStatus,
         |        result_code_s as failCause,
         |        '' as cellId,
         |        '' as rsrp,
         |        '' as rsrq,
         |        '' as rip,
         |        procedure_type_s as procedureType,
         |        '' as upsinr,
         |        '' as phr,
         |        '' as vid
         |   from  tb_xdr_ifc_gxrx
         |  where dt =  $ANALY_DATE
         |    AND h = $ANALY_HOUR
         |    AND m=$MIN
         |    AND interface_s = 26
         |UNION ALL
         |select length_s as length,
         |        local_city_s as city,
         |        interface_s as interfaces,
         |        xdrid_s as xdrid,
         |        rat_s as rat,
         |        imsi_s as imsi,
         |        imei_s as imei,
         |        msisdn_s as msisdn,
         |        start_time_s as prostartTime,
         |        '' as metaTime,
         |        procedure_status_s as cnProcedureStatus,
         |        failure_cause_s as failCause,
         |        eci_s as cellId,
         |        '' as rsrp,
         |        '' as rsrq,
         |        '' as rip,
         |        procedure_type_s as procedureType,
         |        '' as upsinr,
         |        '' as phr,
         |        '' as vid
         |   from  tb_xdr_ifc_s1mme
         |  where dt =  $ANALY_DATE
         |    AND h = $ANALY_HOUR
         |    AND m=$MIN
         |    AND interface_s = 5
         |UNION ALL
         |select length,
         |        city,
         |        interface as interfaces,
         |        xdrid,
         |        rat,
         |        imsi,
         |        imei,
         |        msisdn,
         |        procedurestarttime as prostartTime,
         |        '' as metaTime,
         |        procedurestatus as cnProcedureStatus,
         |        '' as failCause,
         |        cellid as cellId,
         |        '' as rsrp,
         |        '' as rsrq,
         |        '' as rip,
         |        procedureType,
         |        '' as upsinr,
         |        '' as phr,
         |        '' as vid
         |   from  tb_xdr_ifc_uu
         |   where dt =  $ANALY_DATE
         |    AND h = $ANALY_HOUR
         |    AND m=$MIN
         |UNION ALL
         |  select length,
         |        city,
         |        interface as interfaces,
         |        xdrid,
         |        rat,
         |        imsi,
         |        imei,
         |        msisdn,
         |        procedurestarttime as prostartTime,
         |        '' as metaTime,
         |        procedurestatus as cnProcedureStatus,
         |        failurecause as failCause,
         |        cellId,
         |        '' as rsrp,
         |        '' as rsrq,
         |        '' as rip,
         |        procedureType,
         |        '' as upsinr,
         |        '' as phr,
         |        '' as vid
         |   from  tb_xdr_ifc_x2
         |  where dt =  $ANALY_DATE
         |    AND h = $ANALY_HOUR
         |    AND m=$MIN
         |    AND interface = 2
       """.stripMargin).repartition(100).write.mode(SaveMode.Overwrite)
      .parquet(s"$DDBDIR/tb_xdr_ifc_all/dt=$ANALY_DATE/h=$ANALY_HOUR/m=$MIN")
  }
}
