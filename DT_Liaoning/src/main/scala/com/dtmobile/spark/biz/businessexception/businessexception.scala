package com.dtmobile.spark.biz.businessexception




import java.util

import com.dtmobile.util.DBUtil
import org.apache.spark.sql.{SaveMode, SparkSession}
/**
  * Created by shenkaili on 17-4-1.
  */
class businessexception (ANALY_DATE: String,ANALY_HOUR: String,SDB: String, DDB: String, warhouseDir: String,ORCAL: String){

  val CAL_DATE = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"
  val START_TIME=ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR)
  val END_TIME=ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + (String.valueOf(ANALY_HOUR).toInt-1).toString
    def analyse(implicit sparkSession: SparkSession): Unit = {
      exceptionAnalyse(sparkSession)

    }

   def exceptionAnalyse(implicit sparkSession: SparkSession): Unit = {

     val threshold= new DBUtil(s"jdbc:oracle:thin:@$ORCAL")
     var oracle = "jdbc:oracle:thin:@"+ORCAL
  val CellDF = sparkSession.read
  .format("jdbc")
  .option("url", s"$oracle")
  .option("dbtable", s"(select create_time,clear_time,cell_id,enb_id from fm_alarm_picked_forselect t where t.create_time >= to_date('$START_TIME', 'yyyy-MM-dd HH24') and t.create_time < to_date('$END_TIME', 'yyyy-MM-dd HH24'))t1")
  .option("user", "scott")
  .option("password", "tiger")
  .option("driver", "oracle.jdbc.driver.OracleDriver")
  .load().createOrReplaceTempView("warnningtable")
     val map= threshold.select()

     var XDRthreshold01:Int=5000
     var XDRthreshold02:Int=100
     var XDRthreshold03:Int=5000
     var XDRthreshold04:Int=800
     var XDRthreshold05:Int=5000
     var XDRthreshold06:Int=25
     var XDRthreshold07:Int=400

     var SPSvBrowsedownv = 100
     var SPSvBrowsedelay = 5000
     var SPSvVideodownv = 800
     var SPSvVideodelay = 5000
     var SPSvInstantmessagedownv = 25
     var SPSvlnstantmessagedelay = 5000

     var cellSvBrowsedownv = 100
     var cellSvBrowsedelay = 5000
     var cellSvVideodownv = 800
     var cellSvVideodelay = 5000
     var cellSvInstantmessagedownv = 25
     var cellSvlnstantmessagedelay = 5000

     var tagendSvBrowsedownv = 100
     var tagendSvBrowsedelay = 5000
     var tagendSvVideodownv = 800
     var tagendSvVideodelay = 5000
     var tagendSvInstantmessagedownv = 25
     var tagendSvlnstantmessagedelay = 5000

     var UESvBrowsedownv = 100
     var UESvBrowsedelay = 5000
     var UESvVideodownv = 800
     var UESvVideodelay = 5000
     var UESvInstantmessagedownv = 25
     var UESvlnstantmessagedelay = 5000

     var SGWSvBrowsedownv = 100
     var SGWSvBrowsedelay = 5000
     var SGWSvVideodownv = 800
     var SGWSvVideodelay = 5000
     var SGWSvInstantmessagedownv = 25
     var SGWSvlnstantmessagedelay = 5000

     var tiemdelay = 1
     var vsdelay = 1
     var timeandvsdelay = 1
     var exdrnum = 5000

     if(map.get("browseServicetimedelay")!=0 && map.get("browseServicetimedelay")!=null) {
       XDRthreshold01 = map.get("browseServicetimedelay")
       XDRthreshold02 = map.get("browseServicedownv")
       XDRthreshold03 = map.get("videoServicetimedelay")
       XDRthreshold04 = map.get("videoServicedownv")
       XDRthreshold05 = map.get("instantmessageServicetimedelay")
       XDRthreshold06 = map.get("instantmessageServicedownv")

       SPSvBrowsedownv = map.get("SPSvBrowsedownv")
       SPSvBrowsedelay = map.get("SPSvBrowsedelay")
       SPSvVideodownv = map.get("SPSvVideodownv")
       SPSvVideodelay = map.get("SPSvVideodelay")
       SPSvInstantmessagedownv = map.get("SPSvInstantmessagedownv")
       SPSvlnstantmessagedelay = map.get("SPSvlnstantmessagedelay")

       cellSvBrowsedownv = map.get("cellSvBrowsedownv")
       cellSvBrowsedelay = map.get("cellSvBrowsedelay")
       cellSvVideodownv = map.get("cellSvVideodownv")
       cellSvVideodelay = map.get("cellSvVideodelay")
       cellSvInstantmessagedownv = map.get("cellSvInstantmessagedownv")
       cellSvlnstantmessagedelay = map.get("cellSvlnstantmessagedelay")

       tagendSvBrowsedownv = map.get("tagendSvBrowsedownv")
       tagendSvBrowsedelay = map.get("tagendSvBrowsedelay")
       tagendSvVideodownv = map.get("tagendSvVideodownv")
       tagendSvVideodelay = map.get("tagendSvVideodelay")
       tagendSvInstantmessagedownv = map.get("tagendSvInstantmessagedownv")
       tagendSvlnstantmessagedelay = map.get("tagendSvlnstantmessagedelay")

       UESvBrowsedownv = map.get("UESvBrowsedownv")
       UESvBrowsedelay = map.get("UESvBrowsedelay")
       UESvVideodownv = map.get("UESvVideodownv")
       UESvVideodelay = map.get("UESvVideodelay")
       UESvInstantmessagedownv = map.get("UESvInstantmessagedownv")
       UESvlnstantmessagedelay = map.get("UESvlnstantmessagedelay")

       SGWSvBrowsedownv = map.get("SGWSvBrowsedownv")
       SGWSvBrowsedelay = map.get("SGWSvBrowsedelay")
       SGWSvVideodownv = map.get("SGWSvVideodownv")
       SGWSvVideodelay = map.get("SGWSvVideodelay")
       SGWSvInstantmessagedownv = map.get("SGWSvInstantmessagedownv")
       SGWSvlnstantmessagedelay = map.get("SGWSvlnstantmessagedelay")

       tiemdelay = map.get("tiemdelay")
       vsdelay = map.get("vsdelay")
       timeandvsdelay = map.get("timeandvsdelay")
       exdrnum = map.get("exdrnum")
     }


     import sparkSession.sql
     sql(s"use $DDB")
     sql(
       s"""alter table t_xdr_event_msg add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
          LOCATION 'hdfs://dtcluster$warhouseDir/t_xdr_event_msg/dt=$ANALY_DATE/h=$ANALY_HOUR'
        """.stripMargin)
     sql(
       s"""
          |select city,xdrid,procedurestarttime,from_unixtime(cast(round(procedurestarttime/1000) as int)),procedureendtime,imsi,imei,substring(imei,1,8)TEtac,msisdn,
          |ecgi,sgwipaddr,appserveripipv4,apptype,appsubtype,appstatus,etype,errorcode,
          |(case when errorcode="1" then "11"
          |when errorcode="2" then "22"
          |when errorcode="3" or errorcode="4" then "33"
          |when errorcode="5" or errorcode="6" then "44"
          |when errorcode="7" then "55" end) from
          |(select t10.city as city,xdrid,procedurestarttime,procedureendtime,imsi,imei,substring(imei,1,8)TEtac,msisdn,
          |ecgi,sgwipaddr,appserveripipv4,apptype,appsubtype,appstatus,
          |(case when httpstate>=500 then "1"
          |when ((t2.pagedelay>$SPSvBrowsedelay or t2.pagespeed<$SPSvBrowsedownv) and APPTYPE=15) or ((t2.videodelay>$SPSvVideodelay or t2.videospeed<$SPSvVideodownv) and APPTYPE=5) or ((t2.videodelay>$SPSvlnstantmessagedelay or t2.videospeed<$SPSvInstantmessagedownv) and APPTYPE=1) then "1"
          |when ((t3.pagedelay>$SGWSvBrowsedelay or t3.pagespeed<$SGWSvBrowsedownv) and APPTYPE=15) or ((t3.videodelay>$SGWSvVideodelay or t3.videospeed<$SGWSvVideodownv) and APPTYPE=5) or ((t3.videodelay>$SGWSvlnstantmessagedelay or t3.videospeed<$SGWSvInstantmessagedownv) and APPTYPE=1) then "2"
          |when(((t4.pagedelay>$cellSvBrowsedelay or t4.pagespeed<$cellSvBrowsedownv) and APPTYPE=15) or ((t4.videodelay>$cellSvVideodelay or t4.videospeed<$cellSvVideodownv) and APPTYPE=5) or ((t4.videodelay>$cellSvVideodelay or t4.videospeed<$cellSvVideodownv) and APPTYPE=1)) and
          |(case when (t1.procedurestarttime>unix_timestamp(t5.create_time)*1000 and t1.procedureendtime<unix_timestamp(t5.clear_time)*1000) or
          |(t1.procedurestarttime<unix_timestamp(t5.create_time)*1000 and t1.procedureendtime<unix_timestamp(t5.clear_time)*1000) or
          |(t1.procedurestarttime>unix_timestamp(t5.create_time)*1000 and t1.procedureendtime>unix_timestamp(t5.clear_time)*1000) or
          |(t1.procedurestarttime<unix_timestamp(t5.create_time)*1000 and t1.procedureendtime>unix_timestamp(t5.clear_time)*1000)
          |then 1 else 0 end)>0 then "3"
          |when ((!(t4.pagedelay>$cellSvBrowsedelay or t4.pagespeed<$cellSvBrowsedownv) and APPTYPE=15) and (!(t4.videodelay>$cellSvVideodelay or t4.videospeed<$cellSvVideodownv) and APPTYPE=5) and (!(t4.videodelay>$cellSvVideodelay or t4.videospeed<$cellSvVideodownv) and APPTYPE=1)) and t6.cnt>0 then "4"
          |when t9.cnt<=0 and (t11.ltecover<0.99 or t11.upsinr<-3)  then "4"
          |when t9.cnt<=0 and !(((t4.pagedelay>$cellSvBrowsedelay or t4.pagespeed<$cellSvBrowsedownv) and APPTYPE=15) or ((t4.videodelay>$cellSvVideodelay or t4.videospeed<$cellSvVideodownv) and APPTYPE=5) or ((t4.videodelay>$cellSvVideodelay or t4.videospeed<$cellSvVideodownv) and APPTYPE=1))then
          |(case when ((t8.pagedelay>$tagendSvBrowsedelay or t8.pagespeed<$tagendSvBrowsedownv) and APPTYPE=15) or ((t8.videodelay>$tagendSvVideodelay or t8.videospeed<$tagendSvVideodownv) and APPTYPE=5) or ((t8.videodelay>$tagendSvlnstantmessagedelay or t8.videospeed<$tagendSvInstantmessagedownv) and APPTYPE=1) then "5"
          |when ((t7.pagedelay>$UESvBrowsedelay or t7.pagespeed<$UESvBrowsedownv) and APPTYPE=15) or ((t7.videodelay>$UESvVideodelay or t7.videospeed<$UESvVideodownv) and APPTYPE=5) or ((t7.videodelay>$UESvlnstantmessagedelay or t7.videospeed<$UESvInstantmessagedownv) and APPTYPE=1) then "6"
          |when t1.httpstate>400 then "6"
          |else "7"
          |end
          |)
          |when(t6.cnt<=0 and t9.cnt>0) then
          |(case when ((t8.pagedelay>$tagendSvBrowsedelay or t8.pagespeed<$tagendSvBrowsedownv) and APPTYPE=15) or ((t8.videodelay>$tagendSvVideodelay or t8.videospeed<$tagendSvVideodownv) and APPTYPE=5) or ((t8.videodelay>$tagendSvlnstantmessagedelay or t8.videospeed<$tagendSvInstantmessagedownv) and APPTYPE=1) then "5"
          |when ((t7.pagedelay>$UESvBrowsedelay or t7.pagespeed<$UESvBrowsedownv) and APPTYPE=15) or ((t7.videodelay>$UESvVideodelay or t7.videospeed<$UESvVideodownv) and APPTYPE=5) or ((t7.videodelay>$UESvlnstantmessagedelay or t7.videospeed<$UESvInstantmessagedownv) and APPTYPE=1) then "6"
          |when t1.httpstate>400 then "6"
          |else "7"
          |end
          |)
          |else "7"
          |end)errorcode,
          |(case
          |      when (apptype=15 and appstatus=0 and busrede>${XDRthreshold01} and (dldata*8/(case when (httplastrede-httpfirstrede)<10 then 10 else httplastrede-httpfirstrede end))<${XDRthreshold02}) then "7"
          |      when (apptype=5 and appstatus=0 and busrede>${XDRthreshold03} and (dldata*8/(procedureendtime-procedurestarttime))<${XDRthreshold04}) then "8"
          |      when (apptype=1 and appstatus=0 and busrede>${XDRthreshold05} and (dldata*8/(case when (httplastrede-httpfirstrede)<10 then 10 else httplastrede-httpfirstrede end))<${XDRthreshold06}) then "9"
          |      when (apptype=15 and appstatus=0 and busrede>${XDRthreshold01}) then "1"
          |      when (apptype=15 and appstatus=0 and (dldata*8/(case when (httplastrede-httpfirstrede)<10 then 10 else httplastrede-httpfirstrede end))<${XDRthreshold02}) then "2"
          |      when (apptype=5 and appstatus=0 and busrede>${XDRthreshold03}) then "3"
          |      when (apptype=5 and appstatus=0 and (dldata*8/(procedureendtime-procedurestarttime))<${XDRthreshold04}) then "4"
          |      when (apptype=1 and appstatus=0 and busrede>${XDRthreshold05}) then "5"
          |      when (apptype=1 and appstatus=0 and (dldata*8/(case when httplastrede-httpfirstrede<10 then 10 else httplastrede-httpfirstrede end))<${XDRthreshold06}) then "6"
          |      when httpstate>=400 then "10"
          |      end
          |)etype
          |from (select * from $SDB.tb_xdr_ifc_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" and
          |(httpstate>=400 or
          |(apptype=15 and appstatus=0 and busrede>${XDRthreshold01}) or
          |(apptype=15 and appstatus=0 and (dldata*8/(case when (httplastrede-httpfirstrede)<10 then 10 else httplastrede-httpfirstrede end))<${XDRthreshold02}) or
          |(apptype=5 and appstatus=0 and busrede>${XDRthreshold03}) or
          |(apptype=5 and appstatus=0 and (dldata*8/(procedureendtime-procedurestarttime))<${XDRthreshold04}) or
          |(apptype=1 and appstatus=0 and busrede>${XDRthreshold05}) or
          |(apptype=1 and appstatus=0 and (dldata*8/(case when (httplastrede-httpfirstrede)<10 then 10 else httplastrede-httpfirstrede end))<${XDRthreshold06})
          |)
          |)t1
          |left join (select appserveripipv4 as sp,(ServiceIMTime/ServiceIMTrans)instantdelay,(ServiceIMFlow/ServiceIMTime)instantspeed,(mediaRespTimeall/mediaResp)videodelay,(mediadownflow/mediadowntime)videospeed,(pageshowtimeall/pageshowsucc)pagedelay,(httpdownflow/httpdowntime)pagespeed from sp_hour_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" ) t2
          |on t1.appserveripipv4=t2.sp
          |left join (select sgwipaddr as sgw,(ServiceIMTime/ServiceIMTrans)instantdelay,(ServiceIMFlow/ServiceIMTime)instantspeed,(mediaRespTimeall/mediaResp)videodelay,(mediadownflow/mediadowntime)videospeed,(pageshowtimeall/pageshowsucc)pagedelay,(httpdownflow/httpdowntime)pagespeed from sgw_hour_http where dt="$ANALY_DATE" and h="$ANALY_HOUR") t3
          |on t1.sgwipaddr=t3.sgw
          |left join (select cellid,(ServiceIMTime/ServiceIMTrans)instantdelay,(ServiceIMFlow/ServiceIMTime)instantspeed,(mediaRespTimeall/mediaResp)videodelay,(mediadownflow/mediadowntime)videospeed,(pageshowtimeall/pageshowsucc)pagedelay,(httpdownflow/httpdowntime)pagespeed from cell_hour_http where dt="$ANALY_DATE" and h="$ANALY_HOUR") t4
          |on t1.ecgi=t4.cellid
          |left join warnningtable t5
          |on (cast(t1.ecgi/256 as int)=t5.enb_id)
          |left join (select imsi as im,count(1) cnt from lte_mro_source where dt="$ANALY_DATE" and h="$ANALY_HOUR" and (kpi1<-110 or kpi8<-3)and kpi1 is not null and kpi8 is not null group by imsi) t6
          |on t1.imsi=t6.im
          |left join (select imsi as im,(ServiceIMTime/ServiceIMTrans)instantdelay,(ServiceIMFlow/ServiceIMTime)instantspeed,(mediaRespTimeall/mediaResp)videodelay,(mediadownflow/mediadowntime)videospeed,(pageshowtimeall/pageshowsucc)pagedelay,(httpdownflow/httpdowntime)pagespeed from ue_hour_http where dt="$ANALY_DATE" and h="$ANALY_HOUR") t7
          |on t1.imsi=t7.im
          |left join (select tac,(ServiceIMTime/ServiceIMTrans)instantdelay,(ServiceIMFlow/ServiceIMTime)instantspeed,(mediaRespTimeall/mediaResp)videodelay,(mediadownflow/mediadowntime)videospeed,(pageshowtimeall/pageshowsucc)pagedelay,(httpdownflow/httpdowntime)pagespeed from tac_hour_http where dt="$ANALY_DATE" and h="$ANALY_HOUR" ) t8
          |on substring(t1.tac,1,8)=t8.tac
          |left join (select imsi as im,count(1) cnt from lte_mro_source where dt="$ANALY_DATE" and h="$ANALY_HOUR" group by imsi) t9
          |on t1.imsi=t9.im
          |left join ltecell t10
          |on t1.ecgi=t10.cellid
          |left join (select cellid,(ltecoverratex/commy)ltecover,(upsigrateavgx/upsigrateavgy)upsinr from mr_gt_cell_ana_base60 where dt="$ANALY_DATE" and h="$ANALY_HOUR") t11
          |on t1.ecgi=t11.cellid
          |) t100

       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/t_xdr_event_msg/dt=$ANALY_DATE/h=$ANALY_HOUR")


     sql(
       s"""alter table zc_city_data add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
        """.stripMargin)
     sql(
       s"""
          |select "$CAL_DATE",t2.city,t2.cellid,t2.businessdelay,t2.pageDownKps,t2.etype from
          |(select t.city,t.cellid,t.etype,
          |avg((s.pageresptimeall+s.SERVICEIMTIME+ s.mediaRespTimeall)/(s.pageresp+s.SERVICEIMTRANS+s.mediaResp))as businessdelay,
          |avg((s.SERVICEIMFLOW + s.mediadownflow +s.httpdownflow) /(s.SERVICEIMTIME +s.mediadowntime + s.httpdowntime)) as pageDownKps,
          |(sum(case when t.etype in (2,4,6,7,8,9) and t.apptype in (1, 5, 15) then 1 else 0 end) / sum(s.browsedownloadvisits + s.videoservicevisits +s.instantmessagevisits))speed,
          |(sum(case when t.etype in (1,3,5,7,8,9) and t.apptype in (1, 5, 15) then 1 else 0 end)/sum(s.browsedownloadvisits+s.videoservicevisits+s.instantmessagevisits))delay ,
          |(sum(case when t.etype in (7,8,9) and t.apptype in (1, 5, 15) then 1 else 0 end)/sum(s.browsedownloadvisits+s.videoservicevisits+s.instantmessagevisits))inst
          |from t_xdr_event_msg t
          |inner join (select * from cell_hour_http where dt=$ANALY_DATE and h=$ANALY_HOUR) s
          |on t.cellid = s.cellid
          |where t.dt=$ANALY_DATE and t.h=$ANALY_HOUR
          |group by t.cellid,t.etype,t.city)t2
          |where t2.delay >$tiemdelay/100 or t2.speed>$vsdelay/100 or t2.inst>$timeandvsdelay/100
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/zc_city_data/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }


}
