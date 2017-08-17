package com.dtmobile.spark.biz.nssp

import com.dtmobile.util.DateUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * QueryPt
  *
  * @author heyongjin
  * @create 2017/08/15 9:17
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
      sparkSession.sqlContext.cacheTable("tb_xdr_ifc_uu_cache")
    } else if ("s1mme".equals(interfaces)) {
      sql(s"use $SDB")
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
           |,requestcause
           |,failurecause
           |,keyword1
           |,keyword2
           |,keyword3
           |,keyword4
           |,mmeues1apid
           |,oldmmegroupid
           |,oldmmecode
           |,oldmtmsi
           |,mmegroupid
           |,mmecode
           |,mtmsi
           |,tmsi
           |,useripv4
           |,useripv6
           |,mmeipadd
           |,enbipadd
           |,mmeport
           |,enbport
           |,tac
           |,cellid
           |,othertac
           |,othereci
           |,apn
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
           |,sourceneip
           |,sourceneport
           |,destneip
           |,destneport
           |,roamdirection
           |,homemcc
           |,homemnc
           |,mcc
           |,mnc
           |,targetlac
           |,sourcetac
           |,sourceeci
           |,svflags
           |,ulcmscip
           |,dlcmmeip
           |,ulcmscteid
           |,dlcmmeteid
           |,stnsr
           |,targetrncid
           |,targetcellid
           |,arp
           |,requestresult
           |,result
           |,svcause
           |,postfailurecause
           |,respdelay
           |,svdelay
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
           |,icid
           |,originrealm
           |,destinationrealm
           |,originhost
           |,destinationhost
           |,sgsnsgwsigip
           |,afappid
           |,ccrequesttype
           |,rxrequesttype
           |,mediatype
           |,abortcause
           |,resultcode
           |,experimentalresultcode
           |,sessionreleasecause
           |,rulefailurecode
           |,sessionid
           |,calledstationid
           |,framedipv6prefix
           |,framedipaddress
           |,cellid
           |,sourcetac
           |,sourceneip
           |,sourceneport
           |,destinationneip
           |,destinationneport
           |,qci
           |from tb_xdr_ifc_gxrx_cache
           |where  procedurestarttime>=$startTime and procedurestarttime<$endTime
       """.stripMargin).repartition(100).write.mode(SaveMode.Overwrite)
        .parquet(s"$DDBDIR/tb_xdr_ifc_gxrx/dt=$ANALY_DATE/h=$ANALY_HOUR/m=$MIN")
    } else if ("mw".equals(interfaces)) {
      sql(s"use $SDB")
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
           |,servicetype
           |,procedurestatus
           |,callingnumber
           |,callednumber
           |,callingpartyuri
           |,requesturi
           |,userip
           |,callid
           |,icid
           |,sourceneip
           |,sourceneport
           |,destneip
           |,destneport
           |,callside
           |,sourceaccesstype
           |,sourceeci
           |,sourcetac
           |,sourceimsi
           |,sourceimei
           |,destaccesstype
           |,desteci
           |,desttac
           |,destimsi
           |,destimei
           |,authtype
           |,expirestimereq
           |,expirestimersp
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
}
