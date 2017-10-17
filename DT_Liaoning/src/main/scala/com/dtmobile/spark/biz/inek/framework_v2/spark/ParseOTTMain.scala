/**
  * Created by Administrator on 2017/8/4.
  */
import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try
import scala.util.control.Breaks._

case class S1UClass(OID: String, CellID: Long, BeginTime: Long, EndTime: Long, UserIP: String, /* GgsnDataTEId: Double, SgsnDataTEId: Double,*/
                    IMSI: String, IMEI: String, MSISDN: String, Host: String, Uri: String, Radius: String, PositionType: String, DataType: String, Longitude: Double, Latitude: Double)
case class s1_u_inner_s1_mme_lnglatOffset(t11_host: String, host: String, s1_uoid: String, enodebid: Long, reporttime: Long,
                                          objectid: Long, imsi: String, imei: String, msisdn: String, starttime: Long, endtime: Long,
                                          mmes1apueid: String, radius: String, positiontype: String, datatype: String, longitude: Double, latitude: Double, uri: String)
object ParseOTTMain {
  def main(args: Array[String]): Unit = {

    var beginTime_TimeStamp = "1500871293906"
    var endTime_TimeStamp = "1500875854821"

    val conf = new SparkConf().setAppName("ParserOTT")
    //.setMaster("local[1]").setMaster("spark://172.21.7.10:7077").setJars(List("xxx.jar")).set("spark.executor.memory", "10g")
    val sc = new SparkContext(conf)
    // 创建sqlContext用来连接oracle、Hive等，由于HiveContext继承自SQLContext，因此，实例化HiveContext既可以操作Oracle，也可操作Hive
    val hiveContext = new HiveContext(sc)

    // use rc_hive_db;
    hiveContext.sql("use liaoning")

    // toDF() method need this line...
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

    // spark error info:
    hiveContext.sql("create table if not exists d_ens_http_4g_(oid string,cellid bigint,begintime bigint,endtime bigint,userip string,imsi string,imei string,msisdn string,host string,uri string,radius string,positiontype string,datatype string,longitude double,latitude double)")


    hiveContext.sql("truncate table d_ens_http_4g_")


    //hiveContext.sql("create table if not exists d_ens_http_4g_(oid string,cellid int,begintime timestamp,endtime timestamp,userip string,imsi string,imei string,msisdn string,host string,uri string,radius string,positiontype string,datatype string,longitude double,latitude double)")
    hiveContext.sql("create table if not exists d_ens_s1_mme_(oid string,begintime bigint,endtime bigint,mmes1apueid string,ueipv4 string,imsi string,imei string,msisdn string,eci int)")
    hiveContext.sql("create table if not exists S1_U_Inner_S1_MME_Offset_(s1_uoid string,enodebid int,reporttime timestamp,objectid int,imsi string,imei string,msisdn string,starttime timestamp,endtime timestamp,mmes1apueid string,host string,radius string,positiontype string,datatype string,longitude double,latitude double,olng string,olat string,lngoffset string,latoffset string)")
    hiveContext.sql("create table if not exists S1_U_Inner_S1_MME_Offset__s1u(s1_uoid string,enodebid int,reporttime string,objectid int,imsi string,imei string,msisdn string,starttime string,endtime string,mmes1apueid string,host string,radius string,positiontype string,datatype string,longitude double,latitude double,olng string,olat string,lngoffset string,latoffset string)")

    hiveContext.sql("create table if not exists ott_baidu_lnglatItems_s1u(t11_host string,host string,s1_uoid string,enodebid string," +
      "reporttime string,objectid string,imsi string,imei string,msisdn string,starttime string,endtime string," +
      "mmes1apueid string,radius string,positiontype string,datatype string,longitude string,latitude string,uri string)")

    hiveContext.sql("create table if not exists ott_gcj02_lnglatItems_s1u(t11_host string,host string,s1_uoid string,enodebid string," +
      "reporttime string,objectid string,imsi string,imei string,msisdn string,starttime string,endtime string," +
      "mmes1apueid string,radius string,positiontype string,datatype string,longitude string,latitude string,uri string)")

    hiveContext.sql("create table if not exists ott_gps_lnglatItems_s1u(t11_host string,host string,s1_uoid string,enodebid string," +
      "reporttime string,objectid string,imsi string,imei string,msisdn string,starttime string,endtime string," +
      "mmes1apueid string,radius string,positiontype string,datatype string,longitude string,latitude string,uri string)")

    hiveContext.sql("create table if not exists ott_baidu_lnglatItems(t11_host string,host string,s1_uoid string,enodebid string," +
      "reporttime string,objectid string,imsi string,imei string,msisdn string,starttime string,endtime string," +
      "mmes1apueid string,radius string,positiontype string,datatype string,longitude string,latitude string,uri string)")

    hiveContext.sql("create table if not exists ott_gcj02_lnglatItems(t11_host string,host string,s1_uoid string,enodebid string," +
      "reporttime string,objectid string,imsi string,imei string,msisdn string,starttime string,endtime string," +
      "mmes1apueid string,radius string,positiontype string,datatype string,longitude string,latitude string,uri string)")

    hiveContext.sql("create table if not exists ott_gps_lnglatItems(t11_host string,host string,s1_uoid string,enodebid string," +
      "reporttime string,objectid string,imsi string,imei string,msisdn string,starttime string,endtime string," +
      "mmes1apueid string,radius string,positiontype string,datatype string,longitude string,latitude string,uri string)")

    //hiveContext.sql("truncate table d_ens_http_4g_")
    hiveContext.sql("truncate table d_ens_s1_mme_")
    hiveContext.sql("truncate table S1_U_Inner_S1_MME_Offset__s1u")
    hiveContext.sql("truncate table S1_U_Inner_S1_MME_Offset_")
    hiveContext.sql("truncate table ott_baidu_lnglatItems_s1u")
    hiveContext.sql("truncate table ott_gcj02_lnglatItems_s1u")
    hiveContext.sql("truncate table ott_gps_lnglatItems_s1u")
    hiveContext.sql("truncate table ott_baidu_lnglatItems")
    hiveContext.sql("truncate table ott_gcj02_lnglatItems")
    hiveContext.sql("truncate table ott_gps_lnglatItems")

    //    0: jdbc:hive2://10.78.152.52:21066/> select procedure_start_time from default.d_ens_http_4g limit 10;
    //    +-----------------------+--+
    //    | procedure_start_time  |
    //    +-----------------------+--+
    //    | 1486536595032         |
    // and from_unixtime(cast(t10.procedure_start_time*0.001 as bigint),'yyyy-MM-dd HH:mm:ss')>'2017-03-06 00:00:00'
    //val s1_u_rdd = hiveContext.sql(
    //  "select t10.procedure_start_time,t10.procedure_end_time,t10.imsi,t10.imei,t10.msisdn,t10.cell_id,t10.user_ipv4,t10.host,t10.uri,t10.http_content " +
    //    "from default.d_ens_http_4g t10 " +
    //    "inner join rc_hive_db.res_cell_" + city + " t11 on t10.cell_id=t11.objectidx16 " +
    //    "where t10.p_hour>='" + begin_p_time + "' and t10.p_hour<'" + end_p_time + "' and t10.procedure_start_time>='" + beginTime_TimeStamp + "' and t10.procedure_start_time<'" + endTime_TimeStamp + "' "
    //)

    val s1_u_rdd = hiveContext.sql(
      "select cast(t10.PROCEDURESTARTTIME as string) as procedure_start_time,cast(t10.PROCEDUREENDTIME as string) as procedure_end_time,cast(t10.imsi as string) as imsi,cast(t10.imei as string) as imei,cast(t10.msisdn as string) as msisdn,cast(t10.ECGI as string) as cell_id,t10.USERIPV4 as user_ipv4,t10.host,t10.uri, 'N' as http_content " +
        "from liaoning.yunnan_s1u t10 " //+
      //"inner join rc_hive_db.res_cell_" + city + " t11 on t10.cell_id=t11.objectidx16 " +
      //"where t10.PROCEDURESTARTTIME>=" + beginTime_TimeStamp + " and t10.PROCEDUREENDTIME<" + endTime_TimeStamp + " and t10.dt=20170724 and h=13"
      //"where t10.dt=20170724 and h=13"
    )

    val s1u_http_content = s1_u_rdd.map(s => (parse_http_context(s.getAs[String]("host"), s.getAs[String]("http_content")), s))
      .filter(s => Try(s._1._1).isSuccess && Try(s._1._3.toDouble).isSuccess && Try(s._1._4.toDouble).isSuccess && Try(s._2.getAs[String]("cell_id").length > 0).isSuccess && Try(s._2.getAs[String]("http_content").length > 0).isSuccess && Try(s._2.getAs[String]("procedure_start_time").length > 0).isSuccess && Try(s._2.getAs[String]("procedure_end_time").length > 0).isSuccess)
      .map(s => S1UClass(UUID.randomUUID().toString,
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
    val s1u_http_uri = s1_u_rdd.map(s => (parse_uri(s.getAs[String]("uri")), s))
      .filter(s => Try(s._1.head._1.replace("%", "").toDouble).isSuccess && Try(s._1.head._2.replace("%", "").toDouble).isSuccess && Try(s._2.getAs[String]("cell_id").length > 0).isSuccess && Try(s._2.getAs[String]("http_content").length > 0).isSuccess && Try(s._2.getAs[String]("procedure_start_time").length > 0).isSuccess && Try(s._2.getAs[String]("procedure_end_time").length > 0).isSuccess)
      .map(s => S1UClass(UUID.randomUUID.toString,
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
        s._1.head._1.replace("%", "").toDouble))
      .toDF()

    // 136403109
    // 当使用默认的partition时，
    // -rwxrwx---+  3 jc_rc jc_rc_group    187.9 K 2017-06-28 17:58 /jc_rc/rc_hive_db/d_ens_http_4g_/part-37944.gz
    // -rwxrwx---+  3 jc_rc jc_rc_group    188.9 K 2017-06-28 17:56 /jc_rc/rc_hive_db/d_ens_http_4g_/part-37945.gz
    // 当设置：repartition(1000)时，
    // -rwxrwx---+  3 jc_rc jc_rc_group     10.9 M 2017-07-01 16:31 /jc_rc/rc_hive_db/d_ens_http_4g_/part-00998.gz
    // -rwxrwx---+  3 jc_rc jc_rc_group     10.9 M 2017-07-01 16:31 /jc_rc/rc_hive_db/d_ens_http_4g_/part-00999.gz
    // 当设置：repartition(100)时，
    // -rwxrwx---+  3 jc_rc jc_rc_group    103.0 M 2017-07-01 17:53 /jc_rc/rc_hive_db/d_ens_http_4g_/part-00098.gz
    // -rwxrwx---+  3 jc_rc jc_rc_group    103.2 M 2017-07-01 17:53 /jc_rc/rc_hive_db/d_ens_http_4g_/part-00099.gz
    val s1u_df = s1u_http_content.unionAll(s1u_http_uri).repartition(1000).persist()
    s1u_df.registerTempTable("temp_d_ens_uri_http_content_4g_")
    hiveContext.sql("insert into d_ens_http_4g_ select * from temp_d_ens_uri_http_content_4g_")

    var _all_lnglatItems_s1u = hiveContext.sql("select t11.host as t11_host,t10.host,'' as s1_uoid,t10.cellid as enodebid," +
      "t10.begintime reporttime,t10.cellid as objectid,t10.imsi,t10.imei,t10.msisdn," +
      "t10.begintime as starttime,t10.endtime,'' as mmes1apueid,t10.radius,t10.positiontype,t10.datatype,t10.longitude,t10.latitude,t10.uri " +
      "from d_ens_http_4g_ as t10 " +
      "left outer join global_baidu_host as t11 on t10.host=t11.host ")
      .map(s => new s1_u_inner_s1_mme_lnglatOffset(
        s.getAs[String]("t11_host"),
        s.getAs[String]("host"),
        s.getAs[String]("s1_uoid"),
        s.getAs[Long]("enodebid"),
        s.getAs[Long]("reporttime"),
        s.getAs[Long]("objectid"),
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
    var _baidu_lnglatItems_s1u = _all_lnglatItems_s1u.map(s => {
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
    var _gcj02_lnglatItems_s1u = _all_lnglatItems_s1u.map(s => {
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
    var _gps_lnglatItems_s1u = _all_lnglatItems_s1u.map(s => {
      var result: s1_u_inner_s1_mme_lnglatOffset = null
      if (s.t11_host == null) {
        if (s.datatype == "http_content" && s.host == "m5.amap.com") {
          result = s
        }
      }
      result
    }).filter(s => s != null)

    _baidu_lnglatItems_s1u.toDF().registerTempTable("ott_temp_baidu_lnglatItems_s1u")
    _gcj02_lnglatItems_s1u.toDF().registerTempTable("ott_temp_gcj02_lnglatItems_s1u")
    _gps_lnglatItems_s1u.toDF().registerTempTable("ott_temp_gps_lnglatItems_s1u")
    hiveContext.sql("insert into ott_baidu_lnglatItems_s1u select * from ott_temp_baidu_lnglatItems_s1u")
    hiveContext.sql("insert into ott_gcj02_lnglatItems_s1u select * from ott_temp_gcj02_lnglatItems_s1u")
    hiveContext.sql("insert into ott_gps_lnglatItems_s1u select * from ott_temp_gps_lnglatItems_s1u")

    hiveContext.sql("insert into S1_U_Inner_S1_MME_Offset__s1u " +
      "select t10.s1_uoid,t10.enodebid,t10.reporttime,t10.objectid,t10.imsi,t10.imei,t10.msisdn,t10.starttime,t10.endtime,t10.mmes1apueid,t10.host,t10.radius,t10.positiontype,t10.datatype,(case when isnotnull(t11.lngoffset) then (t10.longitude-t11.lngoffset) else t10.longitude end)as longitude,(case when isnotnull(t11.latoffset) then (t10.latitude-t11.latoffset) else t10.latitude end) as latitude,t10.longitude olng,t10.latitude olat,t11.lngoffset,t11.latoffset " +
      "from ott_baidu_lnglatItems_s1u t10 " +
      "inner join global_baidu_lnglatoffset t11 on rpad(t10.longitude,8,'0')=rpad(t11.baidulng,8,'0') and rpad(t10.latitude,7,'0')=t11.baidulat")

    hiveContext.sql("insert into S1_U_Inner_S1_MME_Offset__s1u " +
      "select t10.s1_uoid,t10.enodebid,t10.reporttime,t10.objectid,t10.imsi,t10.imei,t10.msisdn,t10.starttime,t10.endtime,t10.mmes1apueid,t10.host,t10.radius,t10.positiontype,t10.datatype,(case when isnotnull(t11.lngoffset) then (t10.longitude-t11.lngoffset) else t10.longitude end)as longitude,(case when isnotnull(t11.latoffset) then (t10.latitude-t11.latoffset) else t10.latitude end) as latitude,t10.longitude olng,t10.latitude olat,t11.lngoffset,t11.latoffset " +
      "from ott_gcj02_lnglatItems_s1u t10 " +
      "inner join global_gcj02_lnglatoffset t11 on rpad(t10.longitude,8,'0')=rpad(t11.gcj02lng,8,'0') and rpad(t10.latitude,7,'0')=rpad(t11.gcj02lat,7,'0')")

    hiveContext.sql("insert into S1_U_Inner_S1_MME_Offset__s1u " +
      "select t10.s1_uoid,t10.enodebid,t10.reporttime,t10.objectid,t10.imsi,t10.imei,t10.msisdn,t10.starttime,t10.endtime,t10.mmes1apueid,t10.host,t10.radius,t10.positiontype,t10.datatype,t10.longitude,t10.latitude,t10.longitude olng,t10.latitude olat,'' lngoffset,'' latoffset " +
      "from ott_gps_lnglatItems_s1u t10 ")


    //oid string,cellid int,begintime bigint,endtime bigint,userip string,imsi string,imei string,msisdn string,host string,uri string,radius string,positiontype string,datatype string,longitude double,latitude double
    // 对于MRO数据，解析时，部分字段需要进行计算，包含：MR.LteScRSRP、MR.LteScRSRQ、MR.LteScRTTD、MR.LteScPHR、MR.LteScAOA、MR.LteScSinrUL、MR.LteNcRSRP、MR.LteNcRSRQ八项。
    // 计算公式如下：
    //? MR.LteScRSRP    入库值=原始值-141
    //? MR.LteScRSRQ    入库值=原始值*0.5-20
    //? MR.LteScRTTD    入库值的RTTD为： 其中  是指XML数据中的原始值
    //? MR.LteScPHR     入库值=原始值-23
    //? MR.LteScAOA     入库值=原始值*0.5
    //? MR.LteScSinrUL  入库值=原始值-11
    //? MR.LteNcRSRP    入库值=原始值-141
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

    sc.stop()
  }
  /*
    def getDistanceByLatAndLon(lat: Double, lon: Double, lat1: Double, lon1: Double): Double = {
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

    var bflag = true
    var r_lon = Lon
    var r_lat = Lat

    if (datatype=="BD")
    {
      //datatype = "GCJ02"
      var x = Lon - 0.0065
      var y = Lat - 0.006
      var z = Math.sqrt(x * x + y * y) - 0.00002 * Math.sin(y * pi)
      var theta = Math.atan2(y, x) - 0.000003 * Math.cos(x * pi)
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
        var radLat = Lat / 180.0 * pi
        var magic = Math.sin(radLat)
        magic = 1 - ee * magic * magic
        var sqrtMagic = Math.sqrt(magic)
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
    val a = 6378245.0
    val ee = 0.00669342162296594323
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
    var lowerCaseHost = host.toLowerCase().trim()
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
          var splitstr: String = "\\,|\\{|\\}"
          var uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
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
        var splitstr: String = "\\,|\\{|\\}"
        var uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
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
        var splitstr: String = "\\,|\\{|\\}"
        var uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
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
        var splitstr: String = "\\\"\\,\\\"|\\{|\\}"
        var uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
        var tempItem: String = ""
        lnglatType = "GCJ02"
        for (uriItem <- uriItems) {
          tempItem = uriItem.trim()
          if (tempItem.startsWith("\"origin\":")) {
            var lngLatItems = tempItem.replace("\"origin\":", "").trim().split("\\,")
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
        var splitstr: String = "\\,|\\{|\\}"
        var uriItems: Array[String] = lowerCaseHttp_Content.split(splitstr)
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
      var splitstr: String = "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C"
      var uriItems: Array[String] = uri.split(splitstr)
      var uriItem: String = ""
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
      var splitstr: String = "\\?|%3F|%3f|&|%26|%22%2c%22|%22%2C%22|%22%7d%2c%7b%22|%22%7D%2C%7B%22|%7b%22|%3B|%3b|%257C"
      var uriItems: Array[String] = uri.split(splitstr)
      var uriItem: String = ""
      breakable {
        for (uriItem <- uriItems) {
          val uriItemLowerCase: String = uriItem.toLowerCase()
          if (uriItemLowerCase.startsWith(lngEqualsChar.toLowerCase())) {
            var tempValue: String = uriItemLowerCase.substring(uriItem.toLowerCase().indexOf(lngEqualsChar.toLowerCase()) + lngEqualsChar.length)
            var tempArray: Array[String] = uri.split("%2C|%2c|,")
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

}
