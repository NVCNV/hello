package com.dtmobile.spark.biz.businessexception

import  org.apache.spark.sql.{SaveMode, SparkSession}
/**
  * Created by shenkaili on 17-4-1.
  */
class businessexception (ANALY_DATE: String,ANALY_HOUR: String,SDB: String, DDB: String, warhouseDir: String){


    def analyse(implicit sparkSession: SparkSession): Unit = {
      exceptionAnalyse(sparkSession)
      
    }

  val XDRthreshold01:Int=5000
  val XDRthreshold02:Int=100
  val XDRthreshold03:Int=5000
  val XDRthreshold04:Int=800
  val XDRthreshold05:Int=5000
  val XDRthreshold06:Int=25
  val XDRthreshold07:Int=400

  val threshold01:Int=5000
  val threshold02:Int=100
  val threshold03:Int=5000
  val threshold04:Int=800
  val threshold05:Int=5000
  val threshold06:Int=25
  val threshold07:Int=500

   def exceptionAnalyse(implicit sparkSession: SparkSession): Unit = {
     import sparkSession.sql
     sql(s"use $DDB")
     sql(
       s"""alter table t_xdr_event_msg add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
          LOCATION 'hdfs://dtcluster/$warhouseDir/t_xdr_event_msg/dt=$ANALY_DATE/h=$ANALY_HOUR'
        """.stripMargin)
     sql(
       s"""
          |select city,xdrid,procedurestarttime,from_unixtime(cast(round(procedurestarttime/1000) as int)),procedureendtime,imsi,imei,substring(imei,1,8)TEtac,msisdn,
          |ecgi,sgwipaddr,appserveripipv4,apptype,appsubtype,appstatus,errorcode,failtype,
          |(case when errorcode="1" then "11"
          |when errorcode="2" then "22"
          |when errorcode="3" or errorcode="4" then "33"
          |when errorcode="5" or errorcode="6" then "44"
          |when errorcode="7" then "55" end) from
          |(select city,xdrid,procedurestarttime,procedureendtime,imsi,imei,substring(imei,1,8)TEtac,msisdn,
          |ecgi,sgwipaddr,appserveripipv4,apptype,appsubtype,appstatus,
          |(case when httpstate>=${threshold07} then "1"
          |when ((t2.pagedelay>${threshold01} or t2.pagespeed<${threshold02}) and APPTYPE=15) or ((t2.videodelay>${threshold03} or t2.videospeed<${threshold04}) and APPTYPE=5) or ((t2.videodelay>${threshold05} or t2.videospeed<${threshold06}) and APPTYPE=1) then "1"
          |when ((t3.pagedelay>${threshold01} or t3.pagespeed<${threshold02}) and APPTYPE=15) or ((t3.videodelay>${threshold03} or t3.videospeed<${threshold04}) and APPTYPE=5) or ((t3.videodelay>${threshold05} or t3.videospeed<${threshold06}) and APPTYPE=1) then "2"
          |
 |when(((t4.pagedelay>${threshold01} or t4.pagespeed<${threshold02}) and APPTYPE=15) or ((t4.videodelay>${threshold03} or t4.videospeed<${threshold04}) and APPTYPE=5) or ((t4.videodelay>${threshold05} or t4.videospeed<${threshold06}) and APPTYPE=1)) and t5.cnt>0 then "3"
          |when (((t4.pagedelay>${threshold01} or t4.pagespeed<${threshold02}) and APPTYPE=15) or ((t4.videodelay>${threshold03} or t4.videospeed<${threshold04}) and APPTYPE=5) or ((t4.videodelay>${threshold05} or t4.videospeed<${threshold06}) and APPTYPE=1)) and t5.cnt<=0 and t6.cnt>0 then "4"
 |when t9.cnt<=0 and (((t4.pagedelay>${threshold01} or t4.pagespeed<${threshold02}) and APPTYPE=15) or ((t4.videodelay>${threshold03} or t4.videospeed<${threshold04}) and APPTYPE=5) or ((t4.videodelay>${threshold05} or t4.videospeed<${threshold06}) and APPTYPE=1)) then "4"
          |when t9.cnt<=0 and !(((t4.pagedelay>${threshold01} or t4.pagespeed<${threshold02}) and APPTYPE=15) or ((t4.videodelay>${threshold03} or t4.videospeed<${threshold04}) and APPTYPE=5) or ((t4.videodelay>${threshold05} or t4.videospeed<${threshold06}) and APPTYPE=1))then
          |(case when ((t8.pagedelay>${threshold01} or t8.pagespeed<${threshold02}) and APPTYPE=15) or ((t8.videodelay>${threshold03} or t8.videospeed<${threshold04}) and APPTYPE=5) or ((t8.videodelay>${threshold05} or t8.videospeed<${threshold06}) and APPTYPE=1) then "5"
          |when ((t7.pagedelay>${threshold01} or t7.pagespeed<${threshold02}) and APPTYPE=15) or ((t7.videodelay>${threshold03} or t7.videospeed<${threshold04}) and APPTYPE=5) or ((t7.videodelay>${threshold05} or t7.videospeed<${threshold06}) and APPTYPE=1) then "6"
          |when t1.httpstate>400 then "6"
          |else "7"
          |end
          |)
          |when(t6.cnt<=0 and t9.cnt>0) then
          |(case when ((t8.pagedelay>${threshold01} or t8.pagespeed<${threshold02}) and APPTYPE=15) or ((t8.videodelay>${threshold03} or t8.videospeed<${threshold04}) and APPTYPE=5) or ((t8.videodelay>${threshold05} or t8.videospeed<${threshold06}) and APPTYPE=1) then "5"
          |when ((t7.pagedelay>${threshold01} or t7.pagespeed<${threshold02}) and APPTYPE=15) or ((t7.videodelay>${threshold03} or t7.videospeed<${threshold04}) and APPTYPE=5) or ((t7.videodelay>${threshold05} or t7.videospeed<${threshold06}) and APPTYPE=1) then "6"
          |when t1.httpstate>400 then "6"
          |else "7"
          |end
          |)
          |else "7"
          |end)errorcode,
          |(case when httpstate>=400 then "10"
          |      when (apptype=15 and appstatus=0 and busrede>${XDRthreshold01} and (dldata*8/(case when (httplastrede-httpfirstrede)<10 then 10 else httplastrede-httpfirstrede end)*1000)<${XDRthreshold02}) then "7"
          |      when (apptype=5 and appstatus=0 and busrede>${XDRthreshold03} and (dldata*8/(procedureendtime-procedurestarttime)*1000)<${XDRthreshold04}) then "8"
          |      when (apptype=1 and appstatus=0 and busrede>${XDRthreshold05} and (dldata*8/(case when (httplastrede-httpfirstrede)<10 then 10 else httplastrede-httpfirstrede end)*1000)<${XDRthreshold06}) then "9"
          |      when (apptype=15 and appstatus=0 and busrede>${XDRthreshold01}) then "1"
          |      when (apptype=15 and appstatus=0 and (dldata*8/(case when (httplastrede-httpfirstrede)<10 then 10 else httplastrede-httpfirstrede end)*1000)<${XDRthreshold02}) then "2"
          |      when (apptype=5 and appstatus=0 and busrede>${XDRthreshold03}) then "3"
          |      when (apptype=5 and appstatus=0 and (dldata*8/(procedureendtime-procedurestarttime)*1000)>${XDRthreshold04}) then "4"
          |      when (apptype=1 and appstatus=0 and busrede>${XDRthreshold05}) then "5"
          |      when (apptype=1 and appstatus=0 and (dldata*8/(case when httplastrede-httpfirstrede<10 then 10 else httplastrede-httpfirstrede end)*1000)>${XDRthreshold06}) then "6"
          |      end
          |)failtype
          |from (select * from http.tb_xdr_ifc_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" and
          |(httpstate>=${XDRthreshold07} or
          |(apptype=15 and appstatus=0 and busrede>${XDRthreshold01}) or
          |(apptype=15 and appstatus=0 and (dldata*8/(case when (httplastrede-httpfirstrede)<10 then 10 else httplastrede-httpfirstrede end)*1000)<${XDRthreshold02}) or
          |(apptype=5 and appstatus=0 and busrede>${XDRthreshold03}) or
          |(apptype=5 and appstatus=0 and (dldata*8/(procedureendtime-procedurestarttime)*1000)>${XDRthreshold04}) or
          |(apptype=1 and appstatus=0 and busrede>${XDRthreshold05}) or
          |(apptype=1 and appstatus=0 and (dldata*8/(case when (httplastrede-httpfirstrede)<10 then 10 else httplastrede-httpfirstrede end)*1000)>${XDRthreshold06})
          |)
          |)t1
          |left join (select appserveripipv4 as sp,(ServiceIMTime/ServiceIMTrans)instantdelay,(ServiceIMFlow/ServiceIMTime)instantspeed,(mediaRespTimeall/mediaResp)videodelay,(mediadownflow/mediadowntime)videospeed,(pageshowtimeall/pageshowsucc)pagedelay,(httpdownflow/httpdowntime)pagespeed from sp_hour_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by appserveripipv4,ServiceIMTime,ServiceIMTrans,ServiceIMFlow,ServiceIMTime,mediaRespTimeall,mediaResp,mediadownflow,mediadowntime,pageshowtimeall,pageshowsucc,httpdownflow,httpdowntime) t2
          |on t1.appserveripipv4=t2.sp
          |left join (select sgwipaddr as sgw,(ServiceIMTime/ServiceIMTrans)instantdelay,(ServiceIMFlow/ServiceIMTime)instantspeed,(mediaRespTimeall/mediaResp)videodelay,(mediadownflow/mediadowntime)videospeed,(pageshowtimeall/pageshowsucc)pagedelay,(httpdownflow/httpdowntime)pagespeed from sgw_hour_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by sgwipaddr,ServiceIMTime,ServiceIMTrans,ServiceIMFlow,ServiceIMTime,mediaRespTimeall,mediaResp,mediadownflow,mediadowntime,pageshowtimeall,pageshowsucc,httpdownflow,httpdowntime) t3
          |on t1.sgwipaddr=t3.sgw
          |left join (select cellid,(ServiceIMTime/ServiceIMTrans)instantdelay,(ServiceIMFlow/ServiceIMTime)instantspeed,(mediaRespTimeall/mediaResp)videodelay,(mediadownflow/mediadowntime)videospeed,(pageshowtimeall/pageshowsucc)pagedelay,(httpdownflow/httpdowntime)pagespeed from cell_hour_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by cellid,ServiceIMTime,ServiceIMTrans,ServiceIMFlow,ServiceIMTime,mediaRespTimeall,mediaResp,mediadownflow,mediadowntime,pageshowtimeall,pageshowsucc,httpdownflow,httpdowntime) t4
          |on t1.ecgi=t4.cellid
          |left join (select cellid,count(1) cnt from warnningtable where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by cellid) t5
          |on t1.ecgi=t5.cellid
          |left join (select imsi as im,count(1) cnt from lte_mro_source where dt="$ANALY_DATE" and h="$ANALY_HOUR" and (kpi1<-110 or kpi8<-3)and kpi1 is not null and kpi8 is not null group by imsi) t6
          |on t1.imsi=t6.im
          |left join (select imsi as im,(ServiceIMTime/ServiceIMTrans)instantdelay,(ServiceIMFlow/ServiceIMTime)instantspeed,(mediaRespTimeall/mediaResp)videodelay,(mediadownflow/mediadowntime)videospeed,(pageshowtimeall/pageshowsucc)pagedelay,(httpdownflow/httpdowntime)pagespeed from ue_hour_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by imsi,ServiceIMTime,ServiceIMTrans,ServiceIMFlow,ServiceIMTime,mediaRespTimeall,mediaResp,mediadownflow,mediadowntime,pageshowtimeall,pageshowsucc,httpdownflow,httpdowntime) t7
          |on t1.imsi=t7.im
          |left join (select tac,(ServiceIMTime/ServiceIMTrans)instantdelay,(ServiceIMFlow/ServiceIMTime)instantspeed,(mediaRespTimeall/mediaResp)videodelay,(mediadownflow/mediadowntime)videospeed,(pageshowtimeall/pageshowsucc)pagedelay,(httpdownflow/httpdowntime)pagespeed from tac_hour_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by tac,ServiceIMTime,ServiceIMTrans,ServiceIMFlow,ServiceIMTime,mediaRespTimeall,mediaResp,mediadownflow,mediadowntime,pageshowtimeall,pageshowsucc,httpdownflow,httpdowntime) t8
          |on substring(t1.tac,1,8)=t8.tac
          |left join (select imsi as im,count(1) cnt from lte_mro_source where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by imsi) t9
          |on t1.imsi=t9.im
          |) t100

       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/t_xdr_event_msg/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

}
