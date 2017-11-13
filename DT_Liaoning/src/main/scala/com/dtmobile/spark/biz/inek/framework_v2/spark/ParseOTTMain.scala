package com.dtmobile.spark.biz.inek.framework_v2.spark

/**
  * Created by zhoudehu on 2017/10/17.
  */



import java.util.UUID


import com.dtmobile.spark.biz.inek.model.Geometry
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import scala.util.Try
import scala.util.control.Breaks._

case class S1UClass(OID: String, CellID: Long, BeginTime: Long, EndTime: Long, UserIP: String, /* GgsnDataTEId: Double, SgsnDataTEId: Double,*/
                    IMSI: String, IMEI: String, MSISDN: String, Host: String, Uri: String, Radius: String, PositionType: String, DataType: String, Longitude: Double, Latitude: Double)
case class s1_u_inner_s1_mme_lnglatOffset(t11_host: String, host: String, s1_uoid: String, enodebid: Int, reporttime: Long,
                                          objectid: Int, imsi: String, imei: String, msisdn: String, starttime: Long, endtime: Long,
                                          mmes1apueid: String, radius: String, positiontype: String, datatype: String, longitude: Double, latitude: Double, uri: String)
case class InitS1MMEClass(OID: String, procedurestarttime: Long, procedureendtime: Long, /*ENodeBID: Int, */ mmeues1apid: Long, UEipV4: String, /*EarbDLteId: Double, EarbULteId: Double,*/
                          IMSI: String, IMEI: String, MSISDN: Int, cellid: Int)


object ParseOTTMain {
  def main(args: Array[String]): Unit = {



    //TOD : 暂时写死
    val city = "liaoning"
    val begin_p_time = "20170921"
    val end_p_time = "11"


    /* val beginTime = "2017-07-24 11:00:00"
    val endTime = "2017-07-24 12:00:00"*/


    val conf = new SparkConf().setAppName("ParserOTT").setMaster("spark://datanode01:7077")
    val hiveContext = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()


    import hiveContext.sql
    import hiveContext.implicits._
    /*
        hiveContext.sql("set hive.mapred.supports.subdirectories=true")
        hiveContext.sql("set mapreduce.input.fileinputformat.input.dir.recursive=true")
        hiveContext.sql("set mapred.max.split.size=256000000")
        hiveContext.sql("set mapred.min.split.size.per.node=128000000")
        hiveContext.sql("set mapred.min.split.size.per.rack=128000000")
        hiveContext.sql("set hive.hadoop.supports.splittable.combineinputformat=true")
        hiveContext.sql("set hive.exec.compress.output=true")
        hiveContext.sql("set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec")
        hiveContext.sql("set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")

        hiveContext.sql("set hive.merge.mapfiles=true")
        hiveContext.sql("set hive.merge.mapredfiles=true")
        hiveContext.sql("set hive.merge.size.per.task=256000000")
        hiveContext.sql("set hive.merge.smallfiles.avgsize=256000000")

        hiveContext.sql("set hive.groupby.skewindata=true")
        */


    sql("use liaoning")
    sql("create table if not exists d_ens_http_4g_(oid string,cellid bigint,begintime bigint,endtime bigint,userip string,imsi string,imei string,msisdn string,host string,uri string,radius string,positiontype string,datatype string,longitude double,latitude double)")
    //    sql("create table if not exists d_ens_s1_mme_(oid string,begintime bigint,endtime bigint,mmes1apueid string,ueipv4 string,imsi string,imei string,msisdn string,eci int)")
    sql("create table if not exists S1_U_Inner_S1_MME_Offset__s1u(s1_uoid string,enodebid int,reporttime string,objectid int,imsi string,imei string,msisdn string,starttime string,endtime string,mmes1apueid string,host string,radius string,positiontype string,datatype string,longitude double,latitude double,olng string,olat string,lngoffset string,latoffset string)")

    sql("create table if not exists ott_baidu_lnglatItems_s1u(t11_host string,host string,s1_uoid string,enodebid string," +
      "reporttime string,objectid string,imsi string,imei string,msisdn string,starttime string,endtime string," +
      "mmes1apueid string,radius string,positiontype string,datatype string,longitude string,latitude string,uri string)")

    sql("create table if not exists ott_gcj02_lnglatItems_s1u(t11_host string,host string,s1_uoid string,enodebid string," +
      "reporttime string,objectid string,imsi string,imei string,msisdn string,starttime string,endtime string," +
      "mmes1apueid string,radius string,positiontype string,datatype string,longitude string,latitude string,uri string)")

    sql("create table if not exists ott_gps_lnglatItems_s1u(t11_host string,host string,s1_uoid string,enodebid string," +
      "reporttime string,objectid string,imsi string,imei string,msisdn string,starttime string,endtime string," +
      "mmes1apueid string,radius string,positiontype string,datatype string,longitude string,latitude string,uri string)")

    sql("create table if not exists ott_baidu_lnglatItems(t11_host string,host string,s1_uoid string,enodebid string," +
      "reporttime string,objectid string,imsi string,imei string,msisdn string,starttime string,endtime string," +
      "mmes1apueid string,radius string,positiontype string,datatype string,longitude string,latitude string,uri string)")

    sql("create table if not exists ott_gcj02_lnglatItems(t11_host string,host string,s1_uoid string,enodebid string," +
      "reporttime string,objectid string,imsi string,imei string,msisdn string,starttime string,endtime string," +
      "mmes1apueid string,radius string,positiontype string,datatype string,longitude string,latitude string,uri string)")

    sql("create table if not exists ott_gps_lnglatItems(t11_host string,host string,s1_uoid string,enodebid string," +
      "reporttime string,objectid string,imsi string,imei string,msisdn string,starttime string,endtime string," +
      "mmes1apueid string,radius string,positiontype string,datatype string,longitude string,latitude string,uri string)")



    sql("create table if not exists temp_S1_U_JOIN_S1_MME_FIRST_RESULT_Temp10_(s1_uoid string,cellid int ,s1_u_begin bigint,s1_u_end bigint,imsi string,imei string,msisdn string,latitude double,longitude double,uri string,host string,radius string,positiontype string,datatype string,s1_mmeoid string,s1_mme_begin bigint,mmes1apueid string,s1_u_diff_s1_mme bigint)")
    sql("create table if not exists temp_S1_U_JOIN_S1_MME_SECOND_RESULT_Temp11_(s1_uoid string,min_s1_u_diff_s1_mme bigint)")
    sql("create table if not exists res_cell_liaoning(oid int,objectid int,siteoid int,cgi string,cellname string,longitude string,latitude string,pci string,earfcn string,horizonangletoeast string,verticalangletohorizontal string,eangletohorizontal string,angletohorizontal string,antheight string,rstxpower string,tac string) row format delimited fields terminated by ',' stored as textfile ")
    sql("create table if not exists temp_S1_U_JOIN_S1_MME_Third_RESULT_Temp12_(s1_uoid string,cellid int ,s1_u_begin bigint,s1_u_end bigint,imsi string,imei string,msisdn string,latitude double,longitude double,uri string,s1_mmeoid string,s1_mme_begin bigint,mmes1apueid string,host string,radius string,positiontype string,datatype string)")
    sql("create table if not exists temp_S1_U_JOIN_S1_MME_FORTH_RESULT_Temp13_(s1_uoid string,hashkey string)")
    sql("create table if not exists temp_S1_U_JOIN_S1_MME_FifTH_RESULT_Temp14_(s1_uoid string)")
    sql("create table if not exists temp_S1_U_JOIN_S1_MME_SixTH_RESULT_Temp15_(s1_uoid string,enodebid int,reporttime bigint,objectid int,imsi string,imei string,msisdn string,starttime bigint,endtime bigint,mmes1apueid string,latitude double,longitude double,uri string,host string,radius string,positiontype string,datatype string)")
    sql("create table if not exists S1_U_Inner_S1_MME_Offset_(s1_uoid string,enodebid int,reporttime bigint,objectid int,imsi string,imei string,msisdn string,starttime bigint,endtime bigint,mmes1apueid string,host string,radius string,positiontype string,datatype string,longitude double,latitude double,olng string,olat string,lngoffset string,latoffset string)")
    sql("create table if not exists mr_join_signal_server_re_result_(mro_group_key string,s1_uoid string,msisdn string,enodebid int, ecellid int, objectid int, city string, longitude string, latitude string,ltescrsrp double, ltescrsrq double, ltescsinrul double, ltesctadv double,time_stamp timestamp ,imsi bigint,starttime bigint,ltencoid int,ltencrsrp bigint)")
    sql("create table if not exists mr_join_signal_neighbour_re_result_(mro_group_key string,ltencoid int,enodebid int,ecellid int,ltescrsrp double)")
    sql("create table if not exists mr_join_signal_server_real_result(key string,city string,objectid int,enodebid int,ecellid int,longitude string,latitude string,ltesctadv double, imsi bigint,starttime bigint,ltencoid int,ltencrsrp bigint,AverageLteScRSRP double,AverageLteScRSRQ double,AverageLteScSinrUL double,SameCount int)")


    sql("truncate table d_ens_http_4g_")
    sql("truncate table S1_U_Inner_S1_MME_Offset__s1u")
    sql("truncate table S1_U_Inner_S1_MME_Offset_")
    sql("truncate table ott_baidu_lnglatItems_s1u")
    sql("truncate table ott_gcj02_lnglatItems_s1u")
    sql("truncate table ott_gps_lnglatItems_s1u")
    sql("truncate table ott_baidu_lnglatItems")
    sql("truncate table ott_gcj02_lnglatItems")
    sql("truncate table ott_gps_lnglatItems")


    sql("truncate table temp_S1_U_JOIN_S1_MME_FIRST_RESULT_Temp10_")
    sql("truncate table temp_S1_U_JOIN_S1_MME_SECOND_RESULT_Temp11_")
    sql("truncate table temp_S1_U_JOIN_S1_MME_Third_RESULT_Temp12_")
    sql("truncate table temp_S1_U_JOIN_S1_MME_FORTH_RESULT_Temp13_")
    sql("truncate table temp_S1_U_JOIN_S1_MME_FifTH_RESULT_Temp14_")
    sql("truncate table temp_S1_U_JOIN_S1_MME_SixTH_RESULT_Temp15_")
    sql("truncate table mr_join_signal_server_re_result_")
    sql("truncate table mr_join_signal_neighbour_re_result_")
    sql("truncate table mr_join_signal_server_real_result")


    val s1_u_rdd = hiveContext.sql(
      s"""
         |select   cast (cast(t10.PROCEDURESTARTTIME/1000 as bigint)  as string) as procedure_start_time
         |, cast (cast(t10.PROCEDUREENDTIME/1000 as bigint) as string) as procedure_end_time
         |,cast(t10.imsi as string) as imsi
         |,cast(t10.imei as string) as imei
         |,cast(t10.msisdn as string) as msisdn
         |,cast(t10.ECGI as string) as cell_id
         |,t10.USERIPV4 as user_ipv4
         |,t10.host
         |,t10.uri
         |,'N' as http_content
         | from liaoning.tb_xdr_ifc_http t10  where dt="$begin_p_time" and h="$end_p_time"
        """.stripMargin)


    // content  字段没有，暂时没数据
    val s1u_http_content = s1_u_rdd.rdd.map(s => (parse_http_context(s.getAs[String]("host"), s.getAs[String]("http_content")), s)).filter(s => Try(s._1._1).isSuccess
      && Try(s._1._3.toDouble).isSuccess
      && Try(s._1._4.toDouble).isSuccess
      && Try(s._2.getAs[String]("cell_id").length > 0).isSuccess
      && Try(s._2.getAs[String]("http_content").length > 0).isSuccess
      && Try(s._2.getAs[String]("procedure_start_time").length > 0).isSuccess
      && Try(s._2.getAs[String]("procedure_end_time").length > 0).isSuccess).map(s => S1UClass(UUID.randomUUID().toString,
      s._2.getAs[String]("cell_id").toLong,
      s._2.getAs[String]("procedure_start_time").toLong,
      s._2.getAs[String]("procedure_end_time").toLong,
      s._2.getAs[String]("user_ipv4"),
      s._2.getAs[String]("imsi"),
      s._2.getAs[String]("imei"),
      s._2.getAs[String]("msisdn"),
      s._2.getAs[String]("host"),
      s._2.getAs[String]("http_content"),
      s._1._5,
      s._1._6,
      "http_content",
      s._1._3.toDouble,
      s._1._4.toDouble))
      .toDF()


    val s1u_http_uri = s1_u_rdd.rdd.map(s => (parse_uri(s.getAs[String]("uri")), s)).filter(s =>
      Try(s._1.head._1.replace("%", "").toDouble).isSuccess
        && Try(s._1.head._2.replace("%", "").toDouble).isSuccess
        && Try(s._2.getAs[String]("cell_id").length > 0).isSuccess
        && Try(s._2.getAs[String]("http_content").length > 0).isSuccess
        && Try(s._2.getAs[String]("procedure_start_time").length > 0).isSuccess
        && Try(s._2.getAs[String]("procedure_end_time").length > 0).isSuccess).map(s => {
      S1UClass(UUID.randomUUID.toString,
        s._2.getAs[String]("cell_id").toLong,
        s._2.getAs[String]("procedure_start_time").toLong,
        s._2.getAs[String]("procedure_end_time").toLong,
        s._2.getAs[String]("user_ipv4"),
        s._2.getAs[String]("imsi"),
        s._2.getAs[String]("imei"),
        s._2.getAs[String]("msisdn"),
        s._2.getAs[String]("host"),
        s._2.getAs[String]("uri"),
        "",
        "",
        "uri",
        s._1.head._2.replace("%", "").toDouble,
        s._1.head._1.replace("%", "").toDouble)
    }
    ).toDF() /*.createOrReplaceTempView("http")*/

    //  sql("select  * from http where host='loc.map.baidu.com' or host='m5.amap.com' limit 30").show()


    val s1u_df = s1u_http_content.union(s1u_http_uri).repartition(100).persist()

    s1u_df.createOrReplaceTempView("temp_d_ens_uri_http_content_4g_")


    sql("insert into d_ens_http_4g_  select * from temp_d_ens_uri_http_content_4g_")

    val _all_lnglatItems_s1u = hiveContext.sql("select t11.host as t11_host,t10.host,'' as s1_uoid,t10.cellid as enodebid," +
      "t10.begintime reporttime,t10.cellid as objectid,t10.imsi,t10.imei,t10.msisdn," +
      "t10.begintime as starttime,t10.endtime,'' as mmes1apueid,t10.radius,t10.positiontype,t10.datatype,t10.longitude,t10.latitude,t10.uri " +
      "from d_ens_http_4g_ as t10 " +
      "left outer join global_baidu_host as t11 on t10.host=t11.host ")
      .map(s => s1_u_inner_s1_mme_lnglatOffset(
        s.getAs[String]("t11_host"),
        s.getAs[String]("host"),
        s.getAs[String]("s1_uoid"),
        s.getAs[Int]("enodebid"),
        s.getAs[Long]("reporttime"),
        s.getAs[Int]("objectid"),
        s.getAs[String]("imsi"),
        s.getAs[String]("imei"),
        s.getAs[String]("msisdn"),
        s.getAs[Long]("starttime"),
        s.getAs[Long]("endtime"),
        s.getAs[String]("mmes1apueid"),
        s.getAs[String]("radius"),
        s.getAs[String]("positiontype"),
        s.getAs[String]("datatype"),
        s.getAs[Double]("longitude"),
        s.getAs[Double]("latitude"),
        s.getAs[String]("uri"))
      )


    val _baidu_lnglatItems_s1u = _all_lnglatItems_s1u.map(s => {
      var result: s1_u_inner_s1_mme_lnglatOffset = null
      if (s.t11_host != null) {
        if (s.host == "loc.map.baidu.com") {
          if (s.datatype == "http_content" && s.uri.indexOf("\"bldg\":\"\",\"floor\":\"\",\"indoor\":") != -1) {
            result = s
          }
        } else {
          result = s
        }
      }
      result
    }).filter(s => s != null)


    val _gcj02_lnglatItems_s1u = _all_lnglatItems_s1u.map(s => {
      var result: s1_u_inner_s1_mme_lnglatOffset = null
      if (s.t11_host == null) {
        if (s.datatype == "http_content" && s.host == "m5.amap.com") {

        } else {
          result = s
        }
      } else {
        if (s.host == "loc.map.baidu.com") {
          if (s.datatype == "http_content" && s.uri.indexOf("\"bldg\":\"\",\"floor\":\"\",\"indoor\":") != -1) {} else {
            result = s
          }
        }
      }
      result
    }).filter(s => s != null)


    val _gps_lnglatItems_s1u = _all_lnglatItems_s1u.map(s => {
      var result: s1_u_inner_s1_mme_lnglatOffset = null
      if (s.t11_host == null) {
        if (s.datatype == "http_content" && s.host == "m5.amap.com") {
          result = s
        }
      }
      result
    }).filter(s => s != null)


    _baidu_lnglatItems_s1u.createOrReplaceTempView("ott_temp_baidu_lnglatItems_s1u")
    _gcj02_lnglatItems_s1u.createOrReplaceTempView("ott_temp_gcj02_lnglatItems_s1u")
    _gps_lnglatItems_s1u.createOrReplaceTempView("ott_temp_gps_lnglatItems_s1u")


    sql("insert into ott_baidu_lnglatItems_s1u select * from ott_temp_baidu_lnglatItems_s1u")
    sql("insert into ott_gcj02_lnglatItems_s1u select * from ott_temp_gcj02_lnglatItems_s1u")
    sql("insert into ott_gps_lnglatItems_s1u select * from ott_temp_gps_lnglatItems_s1u")

    sql("insert into S1_U_Inner_S1_MME_Offset__s1u " +
      "select t10.s1_uoid,t10.enodebid,t10.reporttime,t10.objectid,t10.imsi,t10.imei,t10.msisdn,t10.starttime,t10.endtime,t10.mmes1apueid,t10.host,t10.radius,t10.positiontype,t10.datatype,(case when isnotnull(t11.lngoffset) then (t10.longitude-t11.lngoffset) else t10.longitude end)as longitude,(case when isnotnull(t11.latoffset) then (t10.latitude-t11.latoffset) else t10.latitude end) as latitude,t10.longitude olng,t10.latitude olat,t11.lngoffset,t11.latoffset " +
      "from ott_baidu_lnglatItems_s1u t10 " +
      "inner join global_baidu_lnglatoffset t11 on rpad(t10.longitude,8,'0')=rpad(t11.baidulng,8,'0') and rpad(t10.latitude,7,'0')=t11.baidulat")

    sql("insert into S1_U_Inner_S1_MME_Offset__s1u " +
      "select t10.s1_uoid,t10.enodebid,t10.reporttime,t10.objectid,t10.imsi,t10.imei,t10.msisdn,t10.starttime,t10.endtime,t10.mmes1apueid,t10.host,t10.radius,t10.positiontype,t10.datatype,(case when isnotnull(t11.lngoffset) then (t10.longitude-t11.lngoffset) else t10.longitude end)as longitude,(case when isnotnull(t11.latoffset) then (t10.latitude-t11.latoffset) else t10.latitude end) as latitude,t10.longitude olng,t10.latitude olat,t11.lngoffset,t11.latoffset " +
      "from ott_gcj02_lnglatItems_s1u t10 " +
      "inner join global_gcj02_lnglatoffset t11 on rpad(t10.longitude,8,'0')=rpad(t11.gcj02lng,8,'0') and rpad(t10.latitude,7,'0')=rpad(t11.gcj02lat,7,'0')")

    sql("insert into S1_U_Inner_S1_MME_Offset__s1u " +
      "select t10.s1_uoid,t10.enodebid,t10.reporttime,t10.objectid,t10.imsi,t10.imei,t10.msisdn,t10.starttime,t10.endtime,t10.mmes1apueid,t10.host,t10.radius,t10.positiontype,t10.datatype,t10.longitude,t10.latitude,t10.longitude olng,t10.latitude olat,'' lngoffset,'' latoffset " +
      "from ott_gps_lnglatItems_s1u t10 ")


    //融合段加俊代码


    val s1_mme_rdd = sql(
      s"""
         |select t10.procedurestarttime
         |,t10.procedureendtime
         |,t10.imsi
         |,t10.imei
         |,cast (t10.cellid/256 as int) msisdn
         |,t10.mmeues1apid
         |,t10.cellid
         |,t10.useripv4
         |from liaoning.tb_xdr_ifc_s1mme t10
         |inner join res_cell_$city t11
         |on t10.cellid=t11.objectid
         |where t10.dt= $begin_p_time and t10.h= $end_p_time
         |
       """.stripMargin)

    /* 已经在数据过滤中完成进行校验   and t10.procedurestarttime>= $beginTime and t10.procedureendtime< $endTime*/


    //.rdd.filter(s =>Try(s.getAs[String]("imsi").toLong).isSuccess&& Try(s.getAs[String]("imei").toLong).isSuccess&& Try(s.getAs[String]("msisdn").toLong).isSuccess&& Try(s.getAs[String]("cell_id").length > 0).isSuccess&& Try(s.getAs[String]("procedure_start_time").length > 0).isSuccess&& Try(s.getAs[String]("procedure_end_time").length > 0).isSuccess)
    s1_mme_rdd
      .map(s => InitS1MMEClass(UUID.randomUUID.toString,
        s.getAs[Long]("procedurestarttime"),
        s.getAs[Long]("procedureendtime"),
        s.getAs[Long]("mmeues1apid"),
        s.getAs[String]("useripv4"),
        s.getAs[String]("imsi"),
        s.getAs[String]("imei"),
        s.getAs[Int]("msisdn"),
        Integer.parseInt(s.getAs[Long]("cellid").toString)))
      .toDF().createOrReplaceTempView("tb_xdr_ifc_s1mme_temp")


    /*s1_mme.repartition(500).persist().createOrReplaceTempView("temp_d_ens_s1_mme_")
    hiveContext.sql("insert into d_ens_s1_mme_ select * from temp_d_ens_s1_mme_")*/

    // do s1_u inner join s1_mme and insert result to hive table's S1_U_Inner_S1_MME_20161115_20161115_
    //"-- 75370750 "
    // 71475692
    //   val t = sql("insert into temp_S1_U_JOIN_S1_MME_FIRST_RESULT_Temp10_ " +
    //      "select T10.OID as S1_UOID,T10.CellID,T10.BeginTime as S1_U_Begin,T10.EndTime as S1_U_End " +
    //      "	  ,T10.IMSI,T10.IMEI,T10.MSISDN,T10.Latitude,T10.Longitude " +
    //      "	  ,T10.Uri,T10.Host,T10.Radius,T10.PositionType,T10.DataType " +
    //      "	  ,T11.OID as S1_MMEOID,T11.BeginTime as S1_MME_Begin,T11.Mmes1apUEId " +
    //      "	  ,(unix_timestamp( cast(t10.begintime as string),'yyyy-MM-dd HH:mm:ss')-unix_timestamp(cast(t11.begintime as string),'yyyy-MM-dd HH:mm:ss')) as S1_U_Diff_S1_MME " +
    //      "from d_ens_http_4g_ T10 " +
    //      "inner join d_ens_s1_mme_ T11 on T10.IMSI=T11.IMSI AND T10.IMEI=T11.IMEI AND T10.CellID=T11.Eci " +
    //      "where (unix_timestamp( cast(t10.endtime as string),'yyyy-MM-dd HH:mm:ss')-unix_timestamp(cast(t10.begintime as string),'yyyy-MM-dd HH:mm:ss'))<=10 and T10.BeginTime>T11.BeginTime")
    //oid cellid/256 取整


    sql(
      s"""
         |insert into temp_S1_U_JOIN_S1_MME_FIRST_RESULT_Temp10_
         |select T10.OID as S1_UOID
         |,T10.CellID
         |,T10.BeginTime as S1_U_Begin
         |,T10.EndTime as S1_U_End
         |,T10.IMSI
         |,T10.IMEI
         |,T10.MSISDN
         |,T10.Latitude
         |,T10.Longitude
         |,T10.Uri
         |,T10.Host
         |,T10.Radius
         |,T10.PositionType
         |,T10.DataType
         |, (cast (T11.cellid/256 as int )) as S1_MMEOID
         |,T11.procedurestarttime as S1_MME_Begin
         |,T11.mmeues1apid
         |,(T10.begintime-t11.procedurestarttime )as S1_U_Diff_S1_MME
         |from d_ens_http_4g_ T10
         |inner join tb_xdr_ifc_s1mme_temp T11
         |on T10.IMSI=T11.IMSI AND T10.IMEI=T11.IMEI AND T10.CellID=T11.cellid
         |where (t10.endtime-T10.begintime)<=1000 and T10.BeginTime>T11.procedurestarttime

       """.stripMargin)


    sql(
      s"""
         |insert into temp_S1_U_JOIN_S1_MME_SECOND_RESULT_Temp11_
         |select S1_UOID
         |,MIN(S1_U_Diff_S1_MME) as MIN_S1_U_Diff_S1_MME
         | from temp_S1_U_JOIN_S1_MME_FIRST_RESULT_Temp10_
         | group by S1_UOID
       """.stripMargin)




    sql(
      s"""
         |insert into temp_S1_U_JOIN_S1_MME_Third_RESULT_Temp12_
         |select T11.S1_UOID
         |,CellID
         |,S1_U_Begin
         |,S1_U_End
         |,T10.IMSI
         |,T10.IMEI
         |,T10.MSISDN
         |,T10.Latitude
         |,T10.Longitude
         |,T10.Uri
         |,S1_MMEOID
         |,S1_MME_Begin
         |,Mmes1apUEId
         |,T10.Host
         |,T10.Radius
         |,T10.PositionType
         |,T10.DataType
         |from temp_S1_U_JOIN_S1_MME_SECOND_RESULT_Temp11_ as T11
         |inner join temp_S1_U_JOIN_S1_MME_FIRST_RESULT_Temp10_ as T10
         |on T10.S1_UOID=T11.S1_UOID and T11.MIN_S1_U_Diff_S1_MME=T10.S1_U_Diff_S1_MME
         |
       """.stripMargin)


    sql(
      s"""
         |insert into temp_S1_U_JOIN_S1_MME_FORTH_RESULT_Temp13_
         |      select T12.S1_UOID,concat(T13.IMEI,'_',T13.IMSI,'_',T13.MSISDN) as HashKey
         |      from temp_S1_U_JOIN_S1_MME_Third_RESULT_Temp12_ as T12
         |      inner join tb_xdr_ifc_s1mme_temp as T13 on ((T12.CellID=T13.cellid) and (T12.Mmes1apUEId=T13.mmeues1apid ))
         |      where (T13.procedurestarttime>=T12.s1_mme_begin) and (T13.procedurestarttime<=T12.s1_u_begin)
       """.stripMargin)

    sql(
      s"""
         |insert into temp_S1_U_JOIN_S1_MME_FifTH_RESULT_Temp14_
         |select S1_UOID FROM (
         |select S1_UOID,HashKey
         |from temp_S1_U_JOIN_S1_MME_FORTH_RESULT_Temp13_
         |group By S1_UOID,HashKey
         |) as T15
         |group By S1_UOID having(COUNT(HashKey)=1)
         |
       """.stripMargin)



    sql(
      s"""
         |insert into temp_S1_U_JOIN_S1_MME_SixTH_RESULT_Temp15_
         |select distinct t12.S1_UOID
         |,cast(t12.CellID/256 as int) as ENodeBID
         |,T12.S1_U_Begin as ReportTime
         |,t12.CellID as ObjectID
         |,T12.IMSI
         |,T12.IMEI
         |,T12.MSISDN
         |,T12.S1_U_Begin-60 StartTime
         |,T12.S1_U_Begin+60 EndTime
         |,T12.Mmes1apUEId
         |,T12.Latitude
         |,T12.Longitude
         |,T12.Uri
         |,T12.Host
         |,T12.Radius
         |,T12.PositionType
         |,T12.DataType
         |from temp_S1_U_JOIN_S1_MME_Third_RESULT_Temp12_ as T12
         |inner join temp_S1_U_JOIN_S1_MME_FifTH_RESULT_Temp14_ AS T14
         | ON T12.S1_UOID=T14.S1_UOID
         |
       """.stripMargin)


    val all_lnglatItems = sql(s"""select t11.host as t11_host
                                          ,t10.host
                                          ,t10.s1_uoid
                                          ,t10.enodebid
                                          ,t10.reporttime
                                          ,t10.objectid
                                          ,t10.imsi
                                          ,t10.imei
                                          ,t10.msisdn
                                          ,t10.starttime
                                          ,t10.endtime
                                          ,t10.mmes1apueid
                                          ,t10.radius
                                          ,t10.positiontype
                                          ,t10.datatype
                                          ,t10.longitude
                                          ,t10.latitude
                                          ,t10.uri
                                        from temp_S1_U_JOIN_S1_MME_SixTH_RESULT_Temp15_ as t10
                                        left outer join global_baidu_host as t11 on t10.host=t11.host  """)
      .map(s => s1_u_inner_s1_mme_lnglatOffset(
        s.getAs[String]("t11_host")
        , s.getAs[String]("host")
        , s.getAs[String]("s1_uoid")
        , s.getAs[Int]("enodebid")
        , s.getAs[Long]("reporttime")
        , s.getAs[Int]("objectid")
        , s.getAs[String]("imsi")
        , s.getAs[String]("imei")
        , s.getAs[String]("msisdn")
        , s.getAs[Long]("starttime")
        , s.getAs[Long]("endtime")
        , s.getAs[String]("mmes1apueid")
        , s.getAs[String]("radius")
        , s.getAs[String]("positiontype")
        , s.getAs[String]("datatype")
        , s.getAs[Double]("longitude")
        , s.getAs[Double]("latitude")
        , s.getAs[String]("uri"))
      )





    val baidu_lnglatItems = all_lnglatItems.map(s => {
      var result: s1_u_inner_s1_mme_lnglatOffset = null
      if (s.t11_host != null) {
        if (s.host == "loc.map.baidu.com") {
          if (s.datatype == "http_content" && s.uri.indexOf("\"bldg\":\"\",\"floor\":\"\",\"indoor\":") != -1) {
            result = s
          }
        } else {
          result = s
        }
      }
      result
    }).filter(s => s != null)
    val gcj02_lnglatItems = all_lnglatItems.map(s => {
      var result: s1_u_inner_s1_mme_lnglatOffset = null
      if (s.t11_host == null) {
        if (s.datatype == "http_content" && s.host == "m5.amap.com") {} else {
          result = s
        }
      } else {
        if (s.host == "loc.map.baidu.com") {
          if (s.datatype == "http_content" && s.uri.indexOf("\"bldg\":\"\",\"floor\":\"\",\"indoor\":") != -1) {} else {
            result = s
          }
        }
      }
      result
    }).filter(s => s != null)
    val gps_lnglatItems = all_lnglatItems.map(s => {
      var result: s1_u_inner_s1_mme_lnglatOffset = null
      if (s.t11_host == null) {
        if (s.datatype == "http_content" && s.host == "m5.amap.com") {
          result = s
        }
      }
      result
    }).filter(s => s != null)


    baidu_lnglatItems.toDF().createOrReplaceTempView("ott_temp_baidu_lnglatItems")
    gcj02_lnglatItems.toDF().createOrReplaceTempView("ott_temp_gcj02_lnglatItems")
    gps_lnglatItems.toDF().createOrReplaceTempView("ott_temp_gps_lnglatItems")
    sql("insert into ott_baidu_lnglatItems select * from ott_temp_baidu_lnglatItems")
    sql("insert into ott_gcj02_lnglatItems select * from ott_temp_gcj02_lnglatItems")
    sql("insert into ott_gps_lnglatItems select * from ott_temp_gps_lnglatItems")

    sql("insert into S1_U_Inner_S1_MME_Offset_ " +
      "select t10.s1_uoid,t10.enodebid,t10.reporttime,t10.objectid,t10.imsi,t10.imei,t10.msisdn,t10.starttime,t10.endtime,t10.mmes1apueid,t10.host,t10.radius,t10.positiontype,t10.datatype,(case when isnotnull(t11.lngoffset) then (t10.longitude-t11.lngoffset) else t10.longitude end)as longitude,(case when isnotnull(t11.latoffset) then (t10.latitude-t11.latoffset) else t10.latitude end) as latitude,t10.longitude olng,t10.latitude olat,t11.lngoffset,t11.latoffset " +
      "from ott_baidu_lnglatItems t10 " +
      "inner join global_baidu_lnglatoffset t11 on rpad(t10.longitude,8,'0')=rpad(t11.baidulng,8,'0') and rpad(t10.latitude,7,'0')=t11.baidulat")

    sql("insert into S1_U_Inner_S1_MME_Offset_ " +
      "select t10.s1_uoid,t10.enodebid,t10.reporttime,t10.objectid,t10.imsi,t10.imei,t10.msisdn,t10.starttime,t10.endtime,t10.mmes1apueid,t10.host,t10.radius,t10.positiontype,t10.datatype,(case when isnotnull(t11.lngoffset) then (t10.longitude-t11.lngoffset) else t10.longitude end)as longitude,(case when isnotnull(t11.latoffset) then (t10.latitude-t11.latoffset) else t10.latitude end) as latitude,t10.longitude olng,t10.latitude olat,t11.lngoffset,t11.latoffset " +
      "from ott_gcj02_lnglatItems t10 " +
      "inner join global_gcj02_lnglatoffset t11 on rpad(t10.longitude,8,'0')=rpad(t11.gcj02lng,8,'0') and rpad(t10.latitude,7,'0')=rpad(t11.gcj02lat,7,'0')")

    sql("insert into S1_U_Inner_S1_MME_Offset_ " +
      "select t10.s1_uoid,t10.enodebid,t10.reporttime,t10.objectid,t10.imsi,t10.imei,t10.msisdn,t10.starttime,t10.endtime,t10.mmes1apueid,t10.host,t10.radius,t10.positiontype,t10.datatype,t10.longitude,t10.latitude,t10.longitude olng,t10.latitude olat,'' lngoffset,'' latoffset " +
      "from ott_gps_lnglatItems t10 ")




    val mro = sql(
      s"""
         |
         |select concat(t10.time_stamp,'_',t10.objectid,'_',t10.mmeues1apid,'_',t10.ltescpci) as mro_group_key
         |,t10.*
         |from res_cell_liaoning t11
         |inner join(
         |  select t10.cellid objectid
         |  ,cast(t10.cellid/256 as int)  as enodebid
         |  ,t10.mrtime time_stamp
         |  ,t10.mmeues1apid
         |  ,(t10.kpi1-141) as ltescrsrp
         |  ,(t10.kpi3*0.5-20) as ltescrsrq
         |  ,(case when t10.kpi5>1200 then null else t10.kpi5 end) as ltesctadv
         |  ,(t10.kpi8-11) as ltescsinrul
         |  ,t10.kpi10 ltescpci
         |  ,t10.kpi25 as ltencoid
         |  ,t10.kpi2 as ltencrsrp
         |  from result.lte_mro_source t10
         |  where  t10.dt=$begin_p_time and t10.h= $end_p_time
         |  ) t10
         |  on t10.objectid=t11.objectid
         |
         |
       """.stripMargin)

      mro.createOrReplaceTempView("mro")



    val res_cell_liaoning = sql(
      s"""
         |select t10.objectid
         |,t10.longitude
         |,t10.latitude
         |,t11.city
         |,t10.HorizonAngleToEast
         |from res_cell_liaoning  t10
         |inner join res_site t11
         |on t10.siteoid=t11.oid
         |
       """.stripMargin)


    val s1u_inner_s1mme = sql(
      s"""
         |select s1_uoid
         |,enodebid
         |,reporttime
         |,objectid
         |,imsi
         |,imei
         |,msisdn
         |,starttime
         |,endtime
         |,mmes1apueid
         |,latitude
         |,longitude
         |,host
         |,radius
         |,positiontype
         |,datatype
         |from S1_U_Inner_S1_MME_Offset_
         """.stripMargin)

    s1u_inner_s1mme.createOrReplaceTempView("s1u_inner_s1mme")




    val isS1PointInCellUdf = udf((latitude: Double, longitude: Double, siteLat: Double, siteLon: Double, lteScRTTD: Double) => {
      // A=坐标点(server.Longitude,server.Latitude)与坐标点(signalItem.Longitude,signalItem.Latitude)之间距离
      val distance = getDistanceByLatAndLon(latitude, longitude, siteLat, siteLon)
      // B=(TADV+2)*78.125
      val rttdValue = (lteScRTTD + 2) * 78.125
      // B减去A是否>=0米,true:满足,false:不满足
      rttdValue - distance >= 0
    })

    /*val TimestampDiff = udf((timestamp1: BigInt, timestamp2: BigInt) => {
      timestamp2  - timestamp1
    })*/


    val mr_join_signal = s1u_inner_s1mme.join(mro, mro("objectid") === s1u_inner_s1mme("objectid")
      && mro("mmeues1apid") === s1u_inner_s1mme("mmes1apueid")
      && mro("time_stamp") >= s1u_inner_s1mme("starttime")
      && mro("time_stamp") <= s1u_inner_s1mme("starttime")+120000)
      .join(res_cell_liaoning, mro("objectid") === res_cell_liaoning("objectid"))
      .where( mro("ltesctadv").isNotNull && isS1PointInCellUdf(s1u_inner_s1mme("latitude")
        , s1u_inner_s1mme("longitude")
        , res_cell_liaoning("latitude")
        , res_cell_liaoning("longitude"), mro("ltesctadv")))
      .select(mro("time_stamp")
        , mro("mro_group_key")
        , s1u_inner_s1mme("s1_uoid")
        , s1u_inner_s1mme("msisdn")
        , res_cell_liaoning("city")
        , s1u_inner_s1mme("latitude")
        , s1u_inner_s1mme("longitude")
        , s1u_inner_s1mme("imsi")
        , s1u_inner_s1mme("starttime")
        , mro("enodebid")
        , mro("objectid")
        , mro("ltescrsrp")
        , mro("ltescrsrq")
        , mro("ltescsinrul")
        , mro("ltesctadv")
        , abs((mro("time_stamp") - s1u_inner_s1mme("starttime"))).alias("timediff")
        , mro("ltencoid")
        , mro("ltencrsrp"))


    mr_join_signal.createOrReplaceTempView("ott_result")
    sql(
      """
        |create table if not exists ott_grid_result
        |(
        |time_stamp              bigint,
        |mro_group_key           string,
        |s1_uoid                 string,
        |msisdn                  string,
        |city                    string,
        |latitude                double,
        |longitude               double,
        |imsi                    string,
        |starttime               bigint,
        |enodebid                int,
        |objectid                bigint,
        |ltescrsrp               bigint,
        |ltescrsrq               decimal(23,1),
        |ltescsinrul             bigint,
        |ltesctadv               bigint,
        |timediff                bigint,
        |ltencoid                bigint,
        |ltencrsrp               bigint,
        |gridid                  string,
        |grid_longitude          double,
        |grid_latitude           double
        |)
      """.stripMargin)

    sql(
      """
        |truncate table ott_grid_result
      """.stripMargin)

    sql(
      """ insert into table ott_grid_result
        |select t10.*,t11.gridid,t11.longitude as grid_longitude ,t11.latitude as grid_latitude
        | from ott_result t10  inner join fingerprintdatabase_service_0 as t11
        | on rpad(t10.latitude+0.00005,7,'0')=rpad(t11.latitude+0.00005,7,'0')
        | and rpad(t10.longitude,8,'0')=rpad(t11.longitude,8,'0')
      """.stripMargin)

    sql(
      """ insert into table ott_grid_result
        |select t10.*,t11.gridid,t11.longitude as grid_longitude ,t11.latitude as grid_latitude
        | from ott_result t10  inner join fingerprintdatabase_service_0 as t11
        |on rpad(t10.latitude+0.00005,7,'0')=rpad(t11.latitude+0.00005,7,'0')
        |and rpad(t10.longitude+0.00005,8,'0')=rpad(t11.longitude+0.00005,8,'0')
        |
      """.stripMargin)

    sql(
      """ insert into table ott_grid_result
        |select t10.*,t11.gridid,t11.longitude as grid_longitude ,t11.latitude as grid_latitude
        | from ott_result t10  inner join fingerprintdatabase_service_0  as t11
        |on rpad(t10.latitude,7,'0')=rpad(t11.latitude,7,'0')
        |and rpad(t10.longitude+0.00005,8,'0')=rpad(t11.longitude+0.00005,8,'0')
      """.stripMargin)

    sql(
      """ insert into table ott_grid_result
        |select t10.*,t11.gridid,t11.longitude as grid_longitude ,t11.latitude as grid_latitude
        | from ott_result t10  inner join fingerprintdatabase_service_0 as t11
        |on rpad(t10.latitude,7,'0')=rpad(t11.latitude,7,'0')
        |and rpad(t10.longitude,8,'0')=rpad(t11.longitude,8,'0')
      """.stripMargin)

    hiveContext.stop()



    val server = mr_join_signal
      .select(mr_join_signal("time_stamp")
        , mr_join_signal("mro_group_key")
        , mr_join_signal("s1_uoid")
        , mr_join_signal("msisdn")
        , mr_join_signal("city")
        , mr_join_signal("latitude")
        , mr_join_signal("longitude")
        , mr_join_signal("imsi")
        , mr_join_signal("starttime")
        , mr_join_signal("enodebid")
        , mr_join_signal("objectid")
        , mr_join_signal("ltescrsrp")
        , mr_join_signal("ltescrsrq")
        , mr_join_signal("ltescsinrul")
        , mr_join_signal("ltesctadv")
        , mr_join_signal("timediff")
        , mr_join_signal("ltencoid")
        , mr_join_signal("ltencrsrp"))
      .distinct()

    val minTimeDiff = server.groupBy("s1_uoid")
      .agg(first("s1_uoid").alias("s1_uoid1"), min("timediff").alias("min_timediff"))
      .selectExpr("s1_uoid1 as s1_uoid", "min_timediff")




    val mr_join_signal_server_result = server.join(minTimeDiff, server("s1_uoid") === minTimeDiff("s1_uoid")
      && server("timediff") === minTimeDiff("min_timediff"))
      .select(server("mro_group_key")
        , server("s1_uoid")
        , server("msisdn")
        , server("enodebid")
        , (server("objectid") - (server("enodebid") * 256)).alias("ecellid")
        , server("objectid")
        , server("city")
        , server("longitude")
        , server("latitude")
        , server("ltescrsrp")
        , server("ltescrsrq")
        , server("ltescsinrul")
        , server("ltesctadv")
        , server("time_stamp")
        , server("imsi")
        , server("starttime")
        , server("ltencoid")
        , server("ltencrsrp"))


    // 6135170
    mr_join_signal_server_result.createOrReplaceTempView("temp_mr_join_signal_server_re_result_")
    sql("insert into mr_join_signal_server_re_result_ select * from temp_mr_join_signal_server_re_result_")

    val neighbour = mr_join_signal.select(mr_join_signal("mro_group_key")
      , mr_join_signal("ltencoid")
      , (pow(10, (mr_join_signal("ltescrsrp") / 10))).alias("ltescrsrp1"))
    val mr_join_signal_neighbour_result = neighbour.groupBy(neighbour("mro_group_key")
      , neighbour("ltencoid"))
      .agg(first("mro_group_key").alias("mro_group_key1")
        , first("ltencoid").alias("ltencoid1")
        , (log10(avg("ltescrsrp1")) * 10).alias("ltescrsrp"))
      .selectExpr("mro_group_key1 as mro_group_key"
        , "ltencoid1 as ltencoid"
        , "cast(ltencoid1/256 as int) as enodebid"
        , "cast(ltencoid1%256 as int) as ecellid"
        , "ltescrsrp")


    mr_join_signal_neighbour_result.createOrReplaceTempView("temp_mr_join_signal_neighbour_re_result_")
    sql("insert into mr_join_signal_neighbour_re_result_ select * from temp_mr_join_signal_neighbour_re_result_")



    val server_re_result = hiveContext.sql(
      s"""select concat(objectid,'_',longitude,'_',latitude) as key
         |,mro_group_key
         |,city
         |,objectid
         |,longitude
         |,latitude
         |,enodebid
         |,ecellid
         |,ltesctadv
         |,imsi
         |,starttime
         |,ltencoid
         |,ltencrsrp
         |,pow(10
         |,(ltescrsrp/10)) as ltescrsrp
         |,pow(10,(ltescrsrq/10)) as ltescrsrq
         |,pow(10,(ltescsinrul/10)) as ltescsinrul
         |from mr_join_signal_server_re_result_  """
        .stripMargin)

    val server_real_result = server_re_result.groupBy(server_re_result("key"))
      .agg(first("key").alias("key1")
        , first("city").alias("city1")
        , first("objectid").alias("objectid1")
        , first("longitude").alias("longitude1")
        , first("latitude").alias("latitude1")
        , first("enodebid").alias("enodebid1")
        , first("ecellid").alias("ecellid1")
        , first("ltesctadv").alias("ltesctadv1")
        ,first("imsi").alias("imsi1")
        ,first("starttime").alias("starttime1")
        ,first("ltencoid").alias("ltencoid1")
        ,first("ltencrsrp").alias("ltencrsrp1")
        , (log10(avg("ltescrsrp")) * 10).alias("AverageLteScRSRP")
        , (log10(avg("ltescrsrq")) * 10).alias("AverageLteScRSRQ")
        , (log10(avg("ltescsinrul")) * 10).alias("AverageLteScSinrUL")
        , count("mro_group_key").alias("SameCount"))
      .selectExpr("key1 as key"
        , "city1 as city"
        , "objectid1 as objectid"
        , "enodebid1 as enodebid"
        , "ecellid1 as ecellid"
        , "longitude1 as longitude"
        , "latitude1 as latitude"
        , "ltesctadv1 as ltesctadv"
        , "imsi1 as imsi"
        , "starttime1 as starttime"
        , "ltencoid1 as ltencoid"
        , "ltencrsrp1 as ltencrsrp"
        , "AverageLteScRSRP"
        , "AverageLteScRSRQ"
        , "AverageLteScSinrUL"
        , "SameCount")


    // 4032429
    server_real_result.createOrReplaceTempView("temp_mr_join_signal_server_real_result_")
    hiveContext.sql("insert into mr_join_signal_server_real_result select * from temp_mr_join_signal_server_real_result_")



    val ott_fingerlibrary_update_server_re_df00 = sql(
      s"""
         |select t11.gridid
         |,t10.key
         |,t10.objectid
         |,t10.longitude
         |,t10.latitude
         |,t10.imsi
         |,t10.starttime
         |,t10.ltencoid
         |,t10.ltencrsrp
         |,cast(((t10.longitude-t11.longitude)*(t10.longitude-t11.longitude)+(t10.latitude-t11.latitude)*(t10.latitude-t11.latitude))*10000000000000 as decimal(38,5)) distans
         |,t10.averageltescrsrp as rsrp
         |,t10.samecount
         |,t11.longitude as gridlongitude
         |,t11.latitude as gridlatitude
         |from mr_join_signal_server_real_result t10
         |inner join fingerprintdatabase_service_0 as t11
         | on rpad(t10.latitude+0.00005,7,'0')=rpad(t11.latitude+0.00005,7,'0')
         | and rpad(t10.longitude,8,'0')=rpad(t11.longitude,8,'0')
       """.stripMargin).repartition(200).persist()






    ott_fingerlibrary_update_server_re_df00.createOrReplaceTempView(s"""temp_ott_fingerlibrary_update_server_re_df00$city""")


    sql(s"""create table fingerlib_join_ott_server_re$city as  select * from temp_ott_fingerlibrary_update_server_re_df00$city """)


    val ott_fingerlibrary_update_server_re_df01 = sql(
      s"""
         |select t11.gridid
         |,t10.key
         |,t10.objectid
         |,t10.longitude
         |,t10.latitude
         |,t10.imsi
         |,t10.starttime
         |,t10.ltencoid
         |,t10.ltencrsrp
         |,cast(((t10.longitude-t11.longitude)*(t10.longitude-t11.longitude)+(t10.latitude-t11.latitude)*(t10.latitude-t11.latitude))*10000000000000 as decimal(38,5)) distans
         |,t10.averageltescrsrp as rsrp
         |,t10.samecount
         |,t11.longitude as gridlongitude
         |,t11.latitude as gridlatitude
         |from mr_join_signal_server_real_result as t10
         |inner join fingerprintdatabase_service_0 as t11
         |on rpad(t10.latitude+0.00005,7,'0')=rpad(t11.latitude+0.00005,7,'0')
         |and rpad(t10.longitude+0.00005,8,'0')=rpad(t11.longitude+0.00005,8,'0')
         |
       """.stripMargin).repartition(200).persist()


      ott_fingerlibrary_update_server_re_df01.createOrReplaceTempView(s"""temp_ott_fingerlibrary_update_server_re_df01$city """)


      sql(s""" insert into fingerlib_join_ott_server_re$city   select * from temp_ott_fingerlibrary_update_server_re_df01$city """)


    val ott_fingerlibrary_update_server_re_df02 = sql(
      s"""
         |select t11.gridid
         |,t10.key
         |,t10.objectid
         |,t10.longitude
         |,t10.latitude
         |,t10.imsi
         |,t10.starttime
         |,t10.ltencoid
         |,t10.ltencrsrp
         |,cast(((t10.longitude-t11.longitude)*(t10.longitude-t11.longitude)+(t10.latitude-t11.latitude)*(t10.latitude-t11.latitude))*10000000000000 as decimal(38,5)) distans
         |,t10.averageltescrsrp as rsrp
         |,t10.samecount
         |,t11.longitude as gridlongitude
         |,t11.latitude as gridlatitude
         |from mr_join_signal_server_real_result  as t10
         |inner join fingerprintdatabase_service_0  as t11
         |on rpad(t10.latitude,7,'0')=rpad(t11.latitude,7,'0')
         |and rpad(t10.longitude+0.00005,8,'0')=rpad(t11.longitude+0.00005,8,'0')
       """.stripMargin).repartition(200).persist()




    ott_fingerlibrary_update_server_re_df02.createOrReplaceTempView(s""" temp_ott_fingerlibrary_update_server_re_df10$city """)



    sql(s""" insert into fingerlib_join_ott_server_re$city  select * from temp_ott_fingerlibrary_update_server_re_df10$city  """)


    val ott_fingerlibrary_update_server_re_df10 = sql(
      s"""
         |select t11.gridid
         |,t10.key
         |,t10.objectid
         |,t10.longitude
         |,t10.latitude
         |,t10.imsi
         |,t10.starttime
         |,t10.ltencoid
         |,t10.ltencrsrp
         |,cast(((t10.longitude-t11.longitude)*(t10.longitude-t11.longitude)+(t10.latitude-t11.latitude)*(t10.latitude-t11.latitude))*10000000000000 as decimal(38,5)) distans
         |,t10.averageltescrsrp as rsrp
         |,t10.samecount
         |,t11.longitude as gridlongitude
         |,t11.latitude as gridlatitude
         |from mr_join_signal_server_real_result  as t10
         |inner join fingerprintdatabase_service_0 as t11
         |on rpad(t10.latitude,7,'0')=rpad(t11.latitude,7,'0')
         |and rpad(t10.longitude,8,'0')=rpad(t11.longitude,8,'0')
         |
       """.stripMargin) .repartition(200).persist()


    ott_fingerlibrary_update_server_re_df10.createOrReplaceTempView(s""" temp_ott_fingerlibrary_update_server_re_df11$city """)


//    hiveContext.stop()
    sql(s""" insert into fingerlib_join_ott_server_re$city  select * from temp_ott_fingerlibrary_update_server_re_df11$city """)






    //oid string,cellid int,begintime bigint,endtime bigint,userip string,imsi string,imei string,msisdn string,host string,uri string,radius string,positiontype string,datatype string,longitude double,latitude double
    // 对于MRO数据，解析时，部分字段需要进行计算，包含：MR.LteScRSRP、MR.LteScRSRQ、MR.LteScRTTD、MR.LteScPHR、MR.LteScAOA、MR.LteScSinrUL、MR.ltescrsrp、MR.LteNcRSRQ八项。
    // 计算公式如下：
    //? MR.LteScRSRP    入库值=原始值-141
    //? MR.LteScRSRQ    入库值=原始值*0.5-20
    //? MR.LteScRTTD    入库值的RTTD为： 其中  是指XML数据中的原始值
    //? MR.LteScPHR     入库值=原始值-23
    //? MR.LteScAOA     入库值=原始值*0.5
    //? MR.LteScSinrUL  入库值=原始值-11
    //? MR.ltescrsrp    入库值=原始值-141
    //? MR.LteNcRSRQ    入库值=原始值*0.5-20
    //    0: jdbc:hive2://10.78.152.52:21066/> select time_stamp from default.d_enl_mr_h limit 10;
    //    +--------------------------+--+
    //    |        time_stamp        |
    //    +--------------------------+--+
    //    | 2017-02-23 14:08:50.057  |
    /*
    val mro = hiveContext.sql("create table testOTT as select t10.*,t11.longitude,latitude " +
      "from liaoning.lte_mro_source t10 " +
      "inner join d_ens_http_4g_Rectification t11 " +
      "on (t10.imsi=t11.imsi and abs(t10.starttime-t11.procedure_start_time)<5 and abs(t10.endtime-t11.procedure_end_time)<5)")


    val isS1PointInCellUdf = udf((latitude: Double, longitude: Double, siteLat: Double, siteLon: Double, lteScRTTD: Double) => {
      // A=坐标点(server.Longitude,server.Latitude)与坐标点(signalItem.Longitude,signalItem.Latitude)之间距离
      val distance = getDistanceByLatAndLon(latitude, longitude, siteLat, siteLon)
      // B=(TADV+2)*78.125
      val rttdValue = (lteScRTTD + 2) * 78.125;
      // B减去A是否>=0米,true:满足,false:不满足
      rttdValue - distance >= 0
    })
    */


  }

   /* def getDistanceByLatAndLon(lat: Double, lon: Double, lat1: Double, lon1: Double): Double = {
      val mercator = lonLat2Mercator(lon, lat)
      val mercator1 = lonLat2Mercator(lon1, lat1)
      var doubleResult = math.sqrt((mercator._1 - mercator1._1) * (mercator._1 - mercator1._1) + (mercator._2 - mercator1._2) * (mercator._2 - mercator1._2))
      if (doubleResult < 0.0 || doubleResult.toString.toUpperCase() == "NAN") {
        // println(doubleResult)
        doubleResult = 3001.0
      }
      doubleResult
    }*/






  def parse_Rectification(datatype:String,Lon:Double,Lat:Double): (Boolean, Double, Double)= {
    val sLon = 124.54208
    val sLat = 40.43059

    val pi = 3.1415926535897932384626
    val a = 6378245.0
    val ee = 0.00669342162296594323

    val bflag = true
    var r_lon = Lon
    var r_lat = Lat

    if (datatype=="BD")
    {
      //datatype = "GCJ02"
      val x = Lon - 0.0065
      val y = Lat - 0.006
      val z = Math.sqrt(x * x + y * y) - 0.00002 * Math.sin(y * pi)
      val theta = Math.atan2(y, x) - 0.000003 * Math.cos(x * pi)
      r_lon = z * Math.cos(theta)
      r_lat = z * Math.sin(theta)
    }

    if (datatype=="GCJ02")
    {
      if(Lon > 72.004 && Lon < 137.8347 && Lat > 0.8293 && Lat < 55.8271)
      {
        var lontitude = Lon * 2 - sLon
        var latitude = Lat * 2 - sLat
        var dLat = transformLat(Lon - 105.0, Lat - 35.0)
        var dLon = transformLon(Lon - 105.0, Lat - 35.0)
        val radLat = Lat / 180.0 * pi
        var magic = Math.sin(radLat)
        magic = 1 - ee * magic * magic
        val sqrtMagic = Math.sqrt(magic)
        dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * pi)
        dLon = (dLon * 180.0) / (a / sqrtMagic * Math.cos(radLat) * pi)
        r_lat = Lat + dLat
        r_lon = Lon + dLon
      }
    }

    (bflag,r_lon,r_lat)
  }

  def transformLat(x: Double, y: Double) = {
    val pi = 3.1415926535897932384626
//    val a = 6378245.0
//    val ee = 0.00669342162296594323
    var ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y  + 0.2 * Math.sqrt(Math.abs(x))
    ret += (20.0 * Math.sin(6.0 * x * pi) + 20.0 * Math.sin(2.0 * x * pi)) * 2.0 / 3.0
    ret += (20.0 * Math.sin(y * pi) + 40.0 * Math.sin(y / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * Math.sin(y / 12.0 * pi) + 320 * Math.sin(y * pi / 30.0)) * 2.0 / 3.0
    ret
  }

  def transformLon(x: Double, y: Double) = {
    val pi = 3.1415926535897932384626
    val a = 6378245.0
    val ee = 0.00669342162296594323
    var ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * Math.sqrt(Math.abs(x))
    ret += (20.0 * Math.sin(6.0 * x * pi) + 20.0 * Math.sin(2.0 * x * pi)) * 2.0 / 3.0
    ret += (20.0 * Math.sin(x * pi) + 40.0 * Math.sin(x / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * Math.sin(x / 12.0 * pi) + 300.0 * Math.sin(x / 30.0 * pi)) * 2.0 / 3.0
    ret
  }

  /**
    * @ summary: 解析http_context字段信息
    * @ param host 参数信息
    * @ param http_context 参数信息
    * @ result._1:是否匹配成功；
    * @ result._2:匹配出的是什么经纬度的格式：
    * @ result._3:经度；
    * @ result._4:纬度,
    * @ result._5:radius
    * @ result._6:positionType
    **/
  def parse_http_context(host: String, http_context: String): (Boolean, String, String, String, String, String) = {
    if (host == null || http_context == null) {
      return (false, "", "", "", "", "")
    }

    var positionType: String = ""
    var success = false
    var lnglatType = ""
    var longitude = ""
    var latitude = ""
    var radius = ""
    val lowerCaseHost = host.toLowerCase().trim()
    val lowerCaseHttp_Content = http_context.toLowerCase()
    //    1. alipay.apilocate.amap.com
    //    apilocate.amap.com
    //    kdtaxi.apilocate.amap.com
    //    m5.amap.com
    //    taobao.apilocate.amap.com
    //    <cenx>120.2084196</cenx><ceny>30.207982</ceny><radius>25</radius>
    //    --<?xml version="1.0" encoding="UTF-8" ?><Cell_Req Ver="4.2.0"><BIZ></BIZ><HDA Version="4.2.0" SuccessCode="1"></HDA><DRA><apiTime>1476963850376</apiTime><coord>1</coord><retype>-5</retype><citycode>0571</citycode><adcode>330108</adcode><cenx>120.2084196</cenx><ceny>30.207982</ceny><radius>25</radius><desc><![CDATA[????????? ????????? ????????? ????????? ??????DQ????????????(?????????????????????)]]></desc><revergeo><country><![CDATA[??????]]></country><province><![CDATA[?????????]]></province><city><![CDATA[?????????]]></city><district><![CDATA[?????????]]></district><road><![CDATA[?????????]]>
    //    //取经纬度及其后radius字段。
    //    //火星坐标
    if (lowerCaseHost.equals("alipay.apilocate.amap.com")
      || lowerCaseHost.equals("apilocate.amap.com")
      || lowerCaseHost.equals("kdtaxi.apilocate.amap.com")
      || lowerCaseHost.equals("m5.amap.com")
      || lowerCaseHost.equals("taobao.apilocate.amap.com")) {
      val indexCenxBegin = lowerCaseHttp_Content.indexOf("<cenx>")
      val indexCenxEnd = lowerCaseHttp_Content.indexOf("</cenx>")
      val indexCenyBegin = lowerCaseHttp_Content.indexOf("<ceny>")
      val indexCenyEnd = lowerCaseHttp_Content.indexOf("</ceny>")
      val indexRadiusBegin = lowerCaseHttp_Content.indexOf("<radius>")
      val indexRadiusEnd = lowerCaseHttp_Content.indexOf("</radius>")
      if (indexCenxBegin != -1 && indexCenxEnd != -1
        && indexCenyBegin != -1 && indexCenyEnd != -1) {
        success = true
        longitude = lowerCaseHttp_Content.substring(indexCenxBegin + "<cenx>".length, indexCenxEnd).trim()
        latitude = lowerCaseHttp_Content.substring(indexCenyBegin + "<ceny>".length, indexCenyEnd).trim()
        if (indexRadiusBegin != -1 && indexRadiusEnd != -1) {
          radius = lowerCaseHttp_Content.substring(indexRadiusBegin + "<radius>".length, indexRadiusEnd).trim()
        }
        lnglatType = "BD"
      }
      //7. m5.amap.com
      //    "y": "30.206281", "x": "120.145655",
      //    --{"code": "1", "timestamp": "1476963846.31", "tip_list": [{"tip": {"category": "140100", "poi_tag": "<font color=#666666>?????????</font>", "name": "????????????????????????", "district": "???????????????????????????", "ignore_district": "0", "adcode": "330102", "column": "3", "rank": "13500782.025024", "datatype_spec": "0", "f_nona": "other", "datatype": "0", "child_nodes": [{"category": "991401", "name": "????????????????????????(?????????)", "datatype": "0", "adcode": "330102", "datatype_spec": "0", "y": "30.206281", "x": "120.145655", "shortname": "?????????", "poiid": "B0FFGAQZD8"}], "x_
      //    火星坐标
      if (success == false && lowerCaseHost.equals("m5.amap.com")) {
        val indexLng = lowerCaseHttp_Content.indexOf("\"x\"")
        val indexLat = lowerCaseHttp_Content.indexOf("\"y\"")
        if (indexLng != -1 && indexLat != -1) {
          val splitstr: String = "\\,|\\{|\\}"
          val uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
          var tempItem: String = ""
          lnglatType = "GCJ02"
          success = true
          for (uriItem <- uriItems) {
            tempItem = uriItem.trim()
            if (tempItem.startsWith("\"x\":")) {
              longitude = tempItem.replace("\"x\":", "").trim()
            } else if (tempItem.startsWith("\"y\":")) {
              latitude = tempItem.replace("\"y\":", "").trim()
            } else if (tempItem.startsWith("\"radius\":")) {
              radius = tempItem.replace("\"radius\":", "").trim()
            }
          }
        }
      }
    }
    //    2. api.map.baidu.com
    //    "result":{"location":{"lng":120.25088311933617,"lat":30.310684375444877},
    //    "confidence":25
    //     --renderReverse&&renderReverse({"status":0,"result":{"location":{"lng":120.25088311933617,"lat":30.310684375444877},"formatted_address":"???????????????????????????????????????","business":"","addressComponent":{"country":"??????","country_code":0,"province":"?????????","city":"?????????","district":"?????????","adcode":"330104","street":"????????????","street_number":"","direction":"","distance":""},"pois":[{"addr":"????????????5277???","cp":" ","direction":"???","distance":"68","name":"????????????????????????????????????","poiType":"????????????","point":{"x":120.25084961536486,"y":30.3112150
    //    如果一个CONTENT中包含如上特征的规则，则取location，放弃后面的point或者其他类型可能存在的经纬度；
    //    第二个特征值：confidence为可信度，表示经纬度坐标的准确度即等效radius字段，一般不包含该字段，但如果检测包含该字段，则需要将confidence字段保存。
    //    前者LOCATION为定位位置，后者POINT为搜索周边POI信息所在位置。
    //    百度坐标
    //3. ***********api.map.baidu.com******************
    //    "point":{"x":120.20848914102,"y":30.327836489696}
    //    --{"content":{"address":"????????????????????????????????????","address_detail":{"adcode":330104,"city":"?????????","city_code":179,"country":"??????","country_code":0,"direction":"","distance":"","district":"?????????","province":"?????????","street":"?????????","street_number":""},"business":"","poi_desc":"????????????(??????)???????????????65???","poi_region":[],"point":{"x":120.34921887098,"y":30.284539279398},"surround_poi":[{"addr":"???????????????????????????","cp":" ","direction":"???","distance":"65","name":"????????????(??????)????????????","poiType":"????????????","point":{"x":120.349
    //    如果一个CONTENT中仅包含point一个经纬度，则取point
    //    需要注意，一个CONTENT字段中可能包含多个该POINT经纬度，取第一个，后面的DROP
    //    单独的point多为POI检索，可信度一般
    //    百度坐标
    //4. api.map.baidu.com
    //    "location":{                "lat":30.331446,                "lng":120.347053            },
    //    --{    "status":0,    "message":"ok",    "total":0,    "results":[        {            "name":"?????????558???",            "location":{                "lat":30.331145,                "lng":120.345378            },            "address":"?????????"        },        {            "name":"?????????518",            "location":{                "lat":30.331446,                "lng":120.347053            },            "address":"?????????"        },        {            "name":"?????????768???",            "location":{                "lat":30.329564,                "lng":120.3365
    //    为一些列地址对应的位置点（地图检索周边小吃等等类似场景），可信度低
    //    取第一个，后面的DROP
    //    百度坐标
    else if (lowerCaseHost.equals("api.map.baidu.com")) {
      val indexLng = lowerCaseHttp_Content.indexOf("\"lng\"")
      val indexLat = lowerCaseHttp_Content.indexOf("\"lat\"")
      if (lowerCaseHttp_Content.indexOf("\"location\"") != -1 && indexLng != -1 && indexLat != -1) {
        val splitstr: String = "\\,|\\{|\\}"
        val uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
        var tempItem: String = ""
        lnglatType = "BD"
        success = true
        for (uriItem <- uriItems) {
          tempItem = uriItem.trim()
          if (tempItem.startsWith("\"lng\":")) {
            longitude = tempItem.replace("\"lng\":", "").trim()
          } else if (tempItem.startsWith("\"lat\":")) {
            latitude = tempItem.replace("\"lat\":", "").trim()
          } else if (tempItem.startsWith("\"confidence\":")) {
            radius = tempItem.replace("\"confidence\":", "").trim()
          }
        }
      }
    }
    //5. loc.map.baidu.com
    //    rd.go.10086.cn
    //    "point":{"x":"120.268353","y":"30.375310"},"radius":"66.474369"},
    //    --{"content":{"bldg":"","clf":"120.275640(30.365383(2000.000000","floor":"","indoor":"0","point":{"x":"120.268353","y":"30.375310"},"radius":"66.474369"},"result":{"error":"161","time":"2016-10-20 19:43:33"}}
    //    CLF坐标放弃，取POINT坐标及radius字段。
    //    百度坐标
    else if (lowerCaseHost.equals("loc.map.baidu.com") || lowerCaseHost.equals("rd.go.10086.cn")) {
      val indexLng = lowerCaseHttp_Content.indexOf("\"x\"")
      val indexLat = lowerCaseHttp_Content.indexOf("\"y\"")
      if (lowerCaseHttp_Content.indexOf("\"point\"") != -1 && indexLng != -1 && indexLat != -1) {
        val splitstr: String = "\\,|\\{|\\}"
        val uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
        var tempItem: String = ""
        lnglatType = "BD"
        success = true
        for (uriItem <- uriItems) {
          tempItem = uriItem.trim()
          if (tempItem.startsWith("\"x\":")) {
            longitude = tempItem.replace("\"x\":", "").trim()
          } else if (tempItem.startsWith("\"y\":")) {
            latitude = tempItem.replace("\"y\":", "").trim()
          } else if (tempItem.startsWith("\"radius\":")) {
            radius = tempItem.replace("\"radius\":", "").trim()
          }
        }

        // CLF坐标放弃，取POINT坐标及radius字段。
        // 仅针对该规则中，结果中增加一个定位方式字段，在符合本规则中，寻找“wf”、“cl”、“ll”特征（注：其中“cl”不能为“clf”）
        // 记录三个值：wf、cl、ll
        // Schema for type java.util.UUID is not supported
        if (lowerCaseHttp_Content.indexOf("\"wf\"") != -1)
          positionType = "wf"
        else if (lowerCaseHttp_Content.indexOf("\"cl\"") != -1)
          positionType = "cl"
        else if (lowerCaseHttp_Content.indexOf("\"ll\"") != -1)
          positionType = "ll"
      }
    }
    //6. ******m5.amap.com******
    //    {"distance": "68.4773", "direction": "North", "name": "\u6e56\u5885\u5357\u8def--\u738b\u5b50\u8857", "weight": "130", "level": "44000, 45000", "longitude": "120.1555667", "crossid": "0571H51F0210021101--0571H51F021002340197", "width": "16, 8", "latitude": "30.27709667"}
    //    --{"province": "\u6d59\u6c5f\u7701", "cross_list": [{"distance": "68.4773", "direction": "North", "name": "\u6e56\u5885\u5357\u8def--\u738b\u5b50\u8857", "weight": "130", "level": "44000, 45000", "longitude": "120.1555667", "crossid": "0571H51F0210021101--0571H51F021002340197", "width": "16, 8", "latitude": "30.27709667"}, {"distance": "133.997", "direction": "SouthEast", "name": "\u6e56\u5885\u5357\u8def--\u6587\u6656\u8def", "weight": "140", "level": "44000, 44000", "longitude": "120.1548272", "crossid": "0571H51F0210021101--0571H51F021002688", "width": "16, 28", "latitude": "30.2787175"}, {"d
    //    地址经纬度，可信度较低
    //    火星坐标
    //8. ******m5.amap.com******
    //    "view_region": "120.193464558,30.216298163,120.202591442,30.202607837",
    //    --{"bus_list": [], "codepoint": 0, "code": "1", "suggestion": {}, "busline_count": "0", "timestamp": "1476963849.32", "lqii": {"suggestionview": "1", "cache_directive": {"cache_all": {"flag": "0", "expires": "24"}}, "utd_sceneid": "101000", "call_taxi": "0", "car_icon_flag": "0", "is_current_city": "1", "slayer": "0", "querytype": "5", "slayer_type": "none", "specialclassify": "0", "view_region": "120.193464558,30.216298163,120.202591442,30.202607837", "suggest_query": {"data": [], "col": "", "row": ""}, "render_name_flag": "1", "is_view_city": "1", "is_tupu_sug": "0"}, "is_general_search": "0",
    //    地图可视范围，经纬度不可信，放弃。
    //9. restapi.amap.com
    //    "origin":"120.162699,30.134971",
    //    --{"status":"1","info":"ok","infocode":"10000","count":"1","route":{"origin":"120.162699,30.134971","destination":"120.160917,30.137208","paths":[{"distance":"294","duration":"210","steps":[{"instruction":"??????????????????????????????294??????????????????","orientation":"??????","road":"????????????","distance":"294","duration":"210","polyline":"120.162727,30.134983;120.162575,30.1353;120.162399,30.135603;120.162178,30.135933;120.16201,30.136141;120.161911,30.136242;120.161842,30.136311;120.161789,30.136362;120.161568,30.136555;120.161308,30.136749;120.160889,30.13707","action":[],"assistant_a
    //    导航开始及目的位置，和中间折现点位置。取开始位置。
    //    火星坐标
    else if (lowerCaseHost.equals("restapi.amap.com")) {
      val indexOrigin = lowerCaseHttp_Content.indexOf("\"origin\"")
      if (indexOrigin != -1) {
        val splitstr: String = "\\\"\\,\\\"|\\{|\\}"
        val uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
        var tempItem: String = ""
        lnglatType = "GCJ02"
        for (uriItem <- uriItems) {
          tempItem = uriItem.trim()
          if (tempItem.startsWith("\"origin\":")) {
            val lngLatItems = tempItem.replace("\"origin\":", "").trim().split("\\,")
            if (lngLatItems.length == 2) {
              success = true
              longitude = lngLatItems(0)
              latitude = lngLatItems(1)
            }
          } else if (tempItem.startsWith("\"radius\":")) {
            radius = tempItem.replace("\"radius\":", "")
          }
        }
      }
    }
    //10. ******restapi.amap.com******
    //    "location":"120.134568,30.1772719",
    //    --{"status":"1","info":"OK","infocode":"10000","regeocode":{"formatted_address":"????????????????????????????????????????????????1669???","addressComponent":{"country":"??????","province":"?????????","city":"?????????","citycode":"0571","district":"?????????","adcode":"330108","township":"????????????","towncode":"330108003000","neighborhood":{"name":[],"type":[]},"building":{"name":[],"type":[]},"streetNumber":{"street":"?????????","number":"1669???","location":"120.134568,30.1772719","direction":"???","distance":"19.4482"},"businessAreas":[{"location":"120.15294029081633,30.164365186224465","n
    //    查询地址对应位置坐标，可信度低，放弃
    //11. route.map.baidu.com
    //    "location":{            "lng":120.16870999682,            "lat":30.179330126914        },
    //    --{    "status":0,    "result":{        "location":{            "lng":120.16870999682,            "lat":30.179330126914        },        "formatted_address":"????????????????????????????????????470",        "business":"?????????,??????",        "addressComponent":{            "adcode":330108,            "city":"?????????",            "country":"??????",            "country_code":0,            "direction":"???",            "distance":"97",            "district":"?????????",            "province":"?????????",            "street":"?????????",            "street_number":"470"
    //    百度坐标
    else if (lowerCaseHost.equals("route.map.baidu.com")) {
      val indexLng = lowerCaseHttp_Content.indexOf("\"lng\"")
      val indexLat = lowerCaseHttp_Content.indexOf("\"lat\"")
      if (lowerCaseHttp_Content.indexOf("\"location\"") != -1 && indexLng != -1 && indexLat != -1) {
        val splitstr: String = "\\,|\\{|\\}"
        val uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
        var tempItem: String = ""
        lnglatType = "BD"
        success = true
        for (uriItem <- uriItems) {
          tempItem = uriItem.trim()
          if (tempItem.startsWith("\"lng\":")) {
            longitude = tempItem.replace("\"lng\":", "").trim()
          } else if (tempItem.startsWith("\"lat\":")) {
            latitude = tempItem.replace("\"lat\":", "").trim()
          } else if (tempItem.startsWith("\"confidence\":")) {
            radius = tempItem.replace("\"confidence\":", "").trim()
          }
        }
      }
    }
    //12. ******route.map.baidu.com******
    //    "display":{                "lat":30.261124,                "lng":120.168691            },
    //    --{    "status":0,    "total":3,    "results":[        {            "uid":"1008c832eaaa556ca2d23045",            "name":"??????????????????-???????????????",            "addr":"??????????????????124???",            "street_id":"1008c832eaaa556ca2d23045",            "display":{                "lat":30.261124,                "lng":120.168691            },            "areaid":2835,            "dis":192,            "price":"5???/??????",            "total_num":287,            "left_num":76        },        {            "uid":"524b138d3fb77fbac056ac09",            "name":"???????
    //    查询地址对应位置坐标，可信度低，放弃
    //******13. sns.amap.com******
    //    "fence_center":"120.15991,30.25411"
    //    --{"fencing_event_list":[{"fence_info":{"fence_center":"120.15991,30.25411","fence_gid":"567fcb37-2e90-4ea9-9cb4-cee52f160d35","fence_name":"??????????????????0302","is_in_alerttime":"false"}}],"msg":{"code":"1","data":{"next_request_time":1440.0,"status":1},"message":"Successful.","result":"true","timestamp":"1476963832.87","version":"2.0-2.0.6287.1606"},"nearest_fence_distance":"2000.0","status":"0"}
    //    高德地图围栏，可信度取决于围栏大小，可信度一般
    //    火星坐标
    //14. trafficapp.autonavi.com:8888
    //    <lon>120.1768039</lon><lat>30.28316083</lat>
    //    --<?xml version="1.0" encoding="gbk"?> <response type="trafficinfo" msgtype="Incident" detailType="2"> <status>0</status> <timestamp>20161020194415</timestamp> <updatetime>194408</updatetime> <front> <updatetime>194408</updatetime> <description><![CDATA[????2??????????????????????????????????]]></description> <signature nearby="0" dist="-1" class="6"><event><type>201</type><layer>1065</layer><layertag>11040</layertag><id>114886501</id><lon>120.1768039</lon><lat>30.28316083</lat><sourcedesc>????????????????</sourcedesc><brief>????????????</brief></event></signature> </front> </response>
    //    火星坐标
    else if (lowerCaseHost.equals("trafficapp.autonavi.com:8888")) {
      val indexCenxBegin = lowerCaseHttp_Content.indexOf("<lon>")
      val indexCenxEnd = lowerCaseHttp_Content.indexOf("</lon>")
      val indexCenyBegin = lowerCaseHttp_Content.indexOf("<lat>")
      val indexCenyEnd = lowerCaseHttp_Content.indexOf("</lat>")
      val indexRadiusBegin = lowerCaseHttp_Content.indexOf("<radius>")
      val indexRadiusEnd = lowerCaseHttp_Content.indexOf("</radius>")

      if (indexCenxBegin != -1 && indexCenxEnd != -1
        && indexCenyBegin != -1 && indexCenyEnd != -1) {
        success = true
        longitude = lowerCaseHttp_Content.substring(indexCenxBegin + "<lon>".length, indexCenxEnd).trim()
        latitude = lowerCaseHttp_Content.substring(indexCenyBegin + "<lat>".length, indexCenyEnd).trim()
        if (indexRadiusBegin != -1 && indexRadiusEnd != -1) {
          radius = lowerCaseHttp_Content.substring(indexRadiusBegin + "<radius>".length, indexRadiusEnd).trim()
        }
        lnglatType = "GCJ02"
      }
    }

    longitude = longitude.replace("\"", "")
    latitude = latitude.replace("\"", "")
    radius = radius.replace("\"", "")

    (success, lnglatType, longitude, latitude, radius, positionType)
  }

  def parse_uri(uri: String): Array[(String, String)] = {
    var latitude: String = null
    var longitude: String = null
    if (uri == null) {
      return Array((latitude, longitude))
    }

    if (parseXYWithEqualsChar(uri, "s_y=", "s_x=").head._1) {
      parseXYWithEqualsChar(uri, "s_y=", "s_x=").head._2
    }
    else if (parseXY(uri, "x=", "y=").head._1) {
      parseXY(uri, "x=", "y=").head._2
    }
    else if (parseXYWithEqualsChar(uri, "lng=", "lat=").head._1) {
      parseXYWithEqualsChar(uri, "lng=", "lat=").head._2
    }
    //http://common.diditaxi.com.cn/passenger/getredpoint?_t=1479167539&appVersion=4.3.12&appversion=4.3.12&channel=102&clientType=1&datatype=101&imei=8787db686f34ec8ae0aec68899e1bdf2
    // &imsi=&lat=30.31243923611111&lng=120.2174****49653&maptype=soso&mobileType=iPhone&model=iPhone&networkType=UNKOWN&os=10.0.2&osType=1&osVersion=10.0.2&sig=46fe82b23734c651b6d349f6d1f3376004301856&timestamp=1479167539259&token=KdHuAAgfNTlnIw_OKVnM74OU-tR2gpoERfEhgzoTIo1UjLsOQjEMQ__Fc4Y8Sprmb3jDgJComK7674TxbraPdTYckQDhhNSQ1tsI82belXBByi
    else if (parseXY(uri, "lng=", "lat=").head._1) {
      parseXY(uri, "lng=", "lat=").head._2
    }
    else if (parseXY(uri, "lng%3d", "lat%3d").head._1) {
      parseXY(uri, "lng%3d", "lat%3d").head._2
    }
    else if (parseXY(uri, "lng%3A", "lat%3A").head._1) {
      parseXY(uri, "lng%3A", "lat%3A").head._2
    }
    else if (parseXY(uri, "lng%22%3a%22", "lat%22%3a%22").head._1) {
      parseXY(uri, "lng%22%3a%22", "lat%22%3a%22").head._2
    }
    else if (parseXY(uri, "lon=", "lat=").head._1) {
      parseXY(uri, "lon=", "lat=").head._2
    }
    else if (parseXY(uri, "lon%3D", "lat%3D").head._1) {
      parseXY(uri, "lon%3D", "lat%3D").head._2
    }
    else if (parseXY(uri, "don=", "lat=").head._1) {
      parseXY(uri, "don=", "lat=").head._2
    }
    else if (parseXY(uri, "lgt=", "lat=").head._1) {
      parseXY(uri, "lgt=", "lat=").head._2
    }
    else if (parseXY(uri, "longitude%22%3a%22", "latitude%22%3a%22").head._1) {
      parseXY(uri, "longitude%22%3a%22", "latitude%22%3a%22").head._2
    }
    else if (parseXY(uri, "longitude=", "latitude=").head._1) {
      parseXY(uri, "longitude=", "latitude=").head._2
    }
    else if (parseXY(uri, "long=", "lat=").head._1) {
      parseXY(uri, "long=", "lat=").head._2
    }
    else if (parseXY(uri, "MyPosx=", "MyPosy=").head._1) {
      parseXY(uri, "MyPosx=", "MyPosy=").head._2
    }
    else if (parseXY(uri, "pointx=", "pointy=").head._1) {
      parseXY(uri, "pointx=", "pointy=").head._2
    }
    else if (parseXY(uri, "lou%22%20:%20%22", "lau%22%20:%20%22").head._1) {
      parseXY(uri, "lou%22%20:%20%22", "lau%22%20:%20%22").head._2
    }
    else if (parseXY(uri, "logi=", "lati=").head._1) {
      parseXY(uri, "logi=", "lati=").head._2
    }
    else if (parseXYWithEqualsChar(uri, "m=", 0, 1).head._1) {
      parseXYWithEqualsChar(uri, "m=", 0, 1).head._2
    }
    else if (parseXY4(uri, "xy%22%3A%22", "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%22%7d%5d|%22%7D%5D|%7b%22|%3B|%3b|%257C", "%2C|%2c|,", 0, 1)._1) {
      parseXY4(uri, "xy%22%3A%22", "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%22%7d%5d|%22%7D%5D|%7b%22|%3B|%3b|%257C", "%2C|%2c|,", 0, 1)._2.toArray
    }
    else if (parseXY2(uri, "action_gps=")._1) {
      parseXY2(uri, "action_gps=")._2.toArray
    }
    else if (parseXY2(uri, "cur_pt=")._1) {
      parseXY2(uri, "cur_pt=")._2.toArray
    }
    else if (parseXY2(uri, "center=")._1) {
      parseXY2(uri, "center=")._2.toArray
    }
    else if (parseXY2(uri, "cll=")._1) {
      parseXY2(uri, "cll=")._2.toArray
    }
    else if (parseXY2(uri, "geoinfo=")._1) {
      parseXY2(uri, "geoinfo=")._2.toArray
    }
    else if (parseXY2(uri, "gps=", 1, 0)._1) {
      parseXY2(uri, "gps=", 1, 0)._2.toArray
    }
    else if (parseXY2(uri, "loc=")._1) {
      parseXY2(uri, "loc=")._2.toArray
    }
    else if (parseXY2(uri, "location=", 1, 0, "http://restapi.amap.com")._1) {
      parseXY2(uri, "location=", 1, 0, "http://restapi.amap.com")._2.toArray
    }
    else if (parseXY2(uri, "location=")._1) {
      parseXY2(uri, "location=")._2.toArray
    }
    else if (parseXY2(uri, "location=", 1, 0)._1) {
      parseXY2(uri, "location=", 1, 0)._2.toArray
    }
    else if (parseXY2(uri, "latlng=")._1) {
      parseXY2(uri, "latlng=")._2.toArray
    }
    else if (parseXY2(uri, "mypos=")._1) {
      parseXY2(uri, "mypos=")._2.toArray
    }
    else if (parseXY2(uri, "origin=")._1) {
      parseXY2(uri, "origin=")._2.toArray
    }
    else if (parseXY2(uri, "points=")._1) {
      parseXY2(uri, "points=")._2.toArray
    }
    else if (parseXY2(uri, "point=")._1) {
      parseXY2(uri, "point=")._2.toArray
    }
    else if (parseXY2(uri, "position=")._1) {
      parseXY2(uri, "position=")._2.toArray
    }
    else if (parseXY2(uri, "q=")._1) {
      parseXY2(uri, "q=")._2.toArray
    }
    else if (parseXY2(uri, "xyr=")._1) {
      parseXY2(uri, "xyr=")._2.toArray
    }
    else if (parseXY4(uri, "coords:", "%2C|%2c|,", "_", 0, 1)._1) {
      parseXY4(uri, "coords:", "%2C|%2c|,", "_", 0, 1)._2.toArray
    }
    else if (parseXY4(uri, "coords%3D", "%2C|%2c|,", "_", 0, 1)._1) {
      parseXY4(uri, "coords%3D", "%2C|%2c|,", "_", 0, 1)._2.toArray
    }
    else if (parseXY4(uri, "click=", "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C", "%2C|%2c|,", 1, 0)._1) {
      parseXY4(uri, "click=", "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C", "%2C|%2c|,", 1, 0)._2.toArray
    }
    else if (parseXY4(uri, "start=", "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C", "%2C|%2c|,", 1, 0)._1) {
      parseXY4(uri, "start=", "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C", "%2C|%2c|,", 1, 0)._2.toArray
    }
    else if (parseXY4(uri, "JW%3A", "%2C|%2c,", "_|\\|", 1, 0) _1) {
      parseXY4(uri, "JW%3A", "%2C|%2c", ",_|\\|", 1, 0)._2.toArray
    }
    else if (parseXY4(uri, "JW:", "%2C|%2c|,", "_|\\|", 1, 0)._1) {
      parseXY4(uri, "JW:", "%2C|%2c|,", "_|\\|", 1, 0)._2.toArray
    }
    else if (parseXY3(uri, "ps.map.baidu.com", "sessid=", "&", "_|\\|", "%2C|%2c|,", 0, 1)._1) {
      parseXY3(uri, "ps.map.baidu.com", "sessid=", "&", "_|\\|", "%2C|%2c|,", 0, 1)._2.toArray
    }
    else if (parseXY(uri, "lt%22%3A%22", "ltt%22%3A%22").head._1) {
      parseXY(uri, "lt%22%3A%22", "ltt%22%3A%22").head._2
    }
    else if (parseXY(uri, "lo=", "la=").head._1) {
      parseXY(uri, "lo=", "la=").head._2
    }
    else if (parseXY(uri, "d=", "l=").head._1) {
      parseXY(uri, "d=", "l=").head._2
    }
    else if (parseXY(uri, "l=", "x=").head._1) {
      parseXY(uri, "l=", "x=").head._2
    }
    else if (parseXY(uri, "px=", "py=").head._1) {
      parseXY(uri, "px=", "py=").head._2
    }
    else {
      breakable {
        if (uri.indexOf("-lat") != -1 && uri.indexOf("-lng") != -1) {
          val uriArr: Array[String] = uri.split("-")
          for (uriPartial: String <- uriArr) {
            if (uriPartial.indexOf("lat") != -1) {
              latitude = uriPartial.replace("lat", "")
              if (longitude != null) {
                break
              }
            }
            if (uriPartial.indexOf("lng") != -1) {
              longitude = uriPartial.replace("lng", "")
              if (latitude != null) {
                break
              }
            }
          }
        }
      }

      Array((latitude, longitude))
    }
  }

  /// <summary>
  /// &from_lat=%28null%29&from_lng=%28null%29&...&lat=30.325195312500&lng=120.099913465712& http://common.diditaxi.com.cn/poiservice/addrrecommend?_t=1472265437&acckey=T7JNA-HRGLG-4N2KY-XX8QE-0RDGW-122J3&appVersion=4.4.4&appversion=4.4.4&channel=102&clientType=1&datatype=101&debugKey=1472265437440%2B02328198adf58916beb31f1fd9acd5a9&departure_time=1472265437&from_lat=%28null%29&from_lng=%28null%29&imei=02328198adf58916beb31f1fd9acd5a9&imsi=&lat=30.325195312500&lng=120.099913465712&maptype=soso&mobileType=iPhone%206%20Plus&model=iPhone&networkType=4G&os=9.3.4&osType=1&osVersion=9.3.4&passengerid=283
  /// </summary>
  /// <returns></returns>
  def parseXYWithEqualsChar(uri: String, lngEqualsChar: String, latEqualsChar: String): Map[Boolean, Array[(String, String)]] = {
    var lng: String = null
    var lat: String = null

    if (uri.toLowerCase().indexOf(lngEqualsChar.toLowerCase()) == -1 | uri.toLowerCase().indexOf(latEqualsChar.toLowerCase()) == -1) {
      Map(false -> Array((lat, lng)))
    }
    else {
      val splitstr: String = "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C"
      val uriItems: Array[String] = uri.split(splitstr)
//      var uriItem: String = ""
      breakable {
        for (uriItem <- uriItems) {
          val uriItemLowerCase: String = uriItem.toLowerCase()
          if (uriItemLowerCase.indexOf(lngEqualsChar.toLowerCase()) != -1) {
            lng = uriItem.substring(uriItemLowerCase.indexOf(lngEqualsChar.toLowerCase()) + lngEqualsChar.length)
            if (lat != null)
              break
          }
          if (uriItemLowerCase.indexOf(latEqualsChar.toLowerCase()) != -1) {
            lat = uriItemLowerCase.substring(uriItem.indexOf(latEqualsChar.toLowerCase()) + latEqualsChar.length)
            if (lng != null)
              break
          }
        }
      }
      Map((lat != null && lng != null) -> Array((lat, lng)))
    }
  }

  def parseXYWithEqualsChar(uri: String, lngEqualsChar: String, latIndex: Int, lngIndex: Int): Map[Boolean, Array[(String, String)]] = {
    var lng: String = null
    var lat: String = null

    if (uri.toLowerCase().indexOf(lngEqualsChar.toLowerCase()) == -1) {
      Map(false -> Array((lat, lng)))
    }
    else {
      val splitstr: String = "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C"
      val uriItems: Array[String] = uri.split(splitstr)
//      var uriItem: String = ""
      breakable {
        for (uriItem <- uriItems) {
          val uriItemLowerCase: String = uriItem.toLowerCase()
          if (uriItemLowerCase.startsWith(lngEqualsChar.toLowerCase())) {
//            var tempValue: String = uriItemLowerCase.substring(uriItem.toLowerCase().indexOf(lngEqualsChar.toLowerCase()) + lngEqualsChar.length)
            val tempArray: Array[String] = uri.split("%2C|%2c|,")
            if (tempArray != null && tempArray.length >= 2) {
              lat = tempArray(latIndex) //tempArray[latIndex]
              lng = tempArray(lngIndex)

              if (lng.indexOf("(") != -1) {
                lng = lng.substring(0, lng.indexOf("("))
              }
            }
            break
          }
        }
      }
      Map((lat != null && lng != null) -> Array((lat, lng)))
    }
  }

  def parseXY(uri: String, lngChar: String, latChar: String): Map[Boolean, Array[(String, String)]] = {
    var lng: String = null
    var lat: String = null

    // &imsi=&lat=30.31243923611111&lng=120.2174****49653&
    if (uri.toLowerCase().indexOf(lngChar.toLowerCase()) == -1 | uri.toLowerCase().indexOf(latChar.toLowerCase()) == -1) {
      Map(false -> Array((lat, lng)))
    }
    else {
      var splitstr: String = "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C"
      var uriItems: Array[String] = uri.split(splitstr)
      var uriItem: String = ""
      breakable {
        for (uriItem <- uriItems) {
          val uriItemLowerCase: String = uriItem.toLowerCase()
          if (uriItemLowerCase.indexOf(lngChar.toLowerCase()) != -1) {
            lng = uriItem.substring(uriItemLowerCase.indexOf(lngChar.toLowerCase()) + lngChar.length)
            if (lat != null)
              break
          }
          if (uriItemLowerCase.indexOf(latChar.toLowerCase()) != -1) {
            lat = uriItem.substring(uriItemLowerCase.indexOf(latChar.toLowerCase()) + latChar.length)
            if (lng != null)
              break
          }
        }
      }
      Map((lat != null && lng != null) -> Array((lat, lng)))
    }
  }

  def parseXY2(uri: String, lngLatChar: String, latIndex: Int = 0, lngIndex: Int = 1, specialHost: String = ""): Tuple2[Boolean, List[Tuple2[String, String]]] = {
    var lng: String = null
    var lat: String = null

    if (specialHost != "" && uri.toLowerCase().indexOf(specialHost.toLowerCase()) == -1) {
      Tuple2(false, List(Tuple2(lat, lng)))
    }
    else if (uri.toLowerCase().indexOf(lngLatChar.toLowerCase()) == -1) {
      Tuple2(false, List(Tuple2(lat, lng)))
    }
    else {
      var splitstr: String = "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C"
      var uriItems: Array[String] = uri.split(splitstr)
      var uriItem: String = ""
      breakable {
        for (uriItem <- uriItems) {
          val uriItemLowerCase: String = uriItem.toLowerCase()
          if (uriItemLowerCase.indexOf(lngLatChar.toLowerCase()) != -1) {
            var tempValue: String = uriItem.substring(uriItemLowerCase.indexOf(lngLatChar.toLowerCase()) + lngLatChar.length)
            var tempArray: Array[String] = uri.split("%2C|%2c|,")
            if (tempArray != null && tempArray.length == 2) {
              lat = tempArray(latIndex) //tempArray[latIndex]
              lng = tempArray(lngIndex)

              if (lng.indexOf("(") != -1) {
                lng = lng.substring(0, lng.indexOf("("));
              }
            }
            break
          }
        }
      }
      Tuple2(lng != null && lat != null, List(Tuple2(lat, lng)))
    }
  }

  def parseXY3(uri: String, host: String, lngLatChar: String, splitChars: String, beginEndChars: String, lngLatSplitChars: String, latIndex: Int = 0, lngIndex: Int = 0): Tuple2[Boolean, List[Tuple2[String, String]]] = {
    var lng: String = null
    var lat: String = null
    var items: List[Tuple2[String, String]] = List()

    if (uri.toLowerCase().indexOf(host.toLowerCase()) == -1 | uri.toLowerCase().indexOf(lngLatChar.toLowerCase()) == -1) {
      Tuple2(false, List(Tuple2(lat, lng)))
    }
    else {
      var uriItems: Array[String] = uri.split(splitChars)
      breakable {
        for (uriItem: String <- uriItems) {
          if (uriItem.toLowerCase().indexOf(lngLatChar.toLowerCase()) != -1) {
            var tempValue: String = uriItem.substring(uriItem.toLowerCase().indexOf(lngLatChar.toLowerCase()) + lngLatChar.length)
            var tempArray: Array[String] = tempValue.split(beginEndChars)
            for (tempItem: String <- tempArray) {
              var tempItemItems: Array[String] = tempItem.split(lngLatSplitChars)
              if (tempItemItems != null && tempItemItems.length == 2) {
                lat = tempItemItems(latIndex)
                lng = tempItemItems(lngIndex)

                if (lng.indexOf("(") != -1) {
                  lng = lng.substring(0, lng.indexOf("("))
                }

                if (lat != null && lng != null && Try(lat.replace("%", "").toDouble > 0.0).isSuccess && Try(lng.replace("%", "").toDouble > 0.0).isSuccess) {
                  items = List.concat(items, List(Tuple2(lat.replace("%", ""), lng.replace("%", ""))))
                }
              }
            }
            break
          }
        }
      }
      Tuple2(items.size != 0, items)
    }
  }

  def parseXY4(uri: String, lngLatChar: String, splitChars: String, lngLatSplitChars: String, latIndex: Int = 0, lngIndex: Int = 0): Tuple2[Boolean, List[Tuple2[String, String]]] = {
    var lng: String = null
    var lat: String = null
    var items: List[Tuple2[String, String]] = List()

    if (uri.toLowerCase().indexOf(lngLatChar.toLowerCase()) == -1) {
      Tuple2(false, List(Tuple2(lat, lng)))
    }
    else {
      var uriItems: Array[String] = uri.split(splitChars)
      for (uriItem: String <- uriItems) {
        if (uriItem.toLowerCase().indexOf(lngLatChar.toLowerCase()) != -1) {
          var tempValue: String = uriItem.substring(uriItem.toLowerCase().indexOf(lngLatChar.toLowerCase()) + lngLatChar.length);
          var tempArray: Array[String] = tempValue.split(lngLatSplitChars)

          if (tempArray != null && tempArray.length == 2) {
            lat = tempArray(latIndex)
            lng = tempArray(lngIndex)

            if (lng.indexOf("(") != -1) {
              lng = lng.substring(0, lng.indexOf("("));
            }

            if (lat != null && lng != null && Try(lat.replace("%", "").toDouble > 0.0).isSuccess && Try(lng.replace("%", "").toDouble > 0.0).isSuccess) {
              items = List.concat(items, List(Tuple2(lat.replace("%", ""), lng.replace("%", ""))))
            }
          }
        }
      }

      Tuple2(lng != null && lat != null, items)
    }
  }

  def lonLat2Mercator_(lon: Double, lat: Double): Geometry = {
    val x = lon * 20037508.34 / 180;
    var y = Math.log(Math.tan((90 + lat) * Math.PI / 360)) / (Math.PI / 180)
    y = y * 20037508.34 / 180
    new Geometry(x, y)
  }

  def lonLat2Mercator(lon: Double, lat: Double): (Double, Double) = {
    val x = lon * 20037508.34 / 180
    var y = Math.log(Math.tan((90 + lat) * Math.PI / 360)) / (Math.PI / 180)
    y = y * 20037508.34 / 180
    (x, y)
  }

  def getDistanceByLatAndLon(lat: Double, lon: Double, lat1: Double, lon1: Double): Double = {
    val mercator = lonLat2Mercator(lon, lat)
    val mercator1 = lonLat2Mercator(lon1, lat1)
    var doubleResult = math.sqrt((mercator._1 - mercator1._1) * (mercator._1 - mercator1._1) + (mercator._2 - mercator1._2) * (mercator._2 - mercator1._2))
    if (doubleResult < 0.0 || doubleResult.toString.toUpperCase() == "NAN") {
      // println(doubleResult)
      doubleResult = 3001.0
    }
    doubleResult
  }

}
