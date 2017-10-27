package com.dtmobile.spark.biz.inek.framework_v2.spark

import java.sql.Timestamp

import com.dtmobile.spark.biz.inek.configuration.{AppSettings, DBConnectionManager, DBHelper}
import com.dtmobile.spark.biz.inek.model.{S1MMEClass, S1UClass}
import com.dtmobile.spark.biz.inek.utils.{geoUtil, httpParser}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import com.dtmobile.spark.biz.inek.utils

import scala.util.Try

/**
 * related hive table:
 * 1. s1_u
 * 2. s1mme
 * 3. mro
 * 4. base_parameter_cell
 * 5. Global_BD09_Host
 * 6. gcj2wgs
 */
object generate_ott_adjust {
 /* def main(args: Array[String]) {
    val time = args(0)
    val day = time.substring(0, 8)
    val hour = time.substring(8, 10)
    val conf = new SparkConf().setAppName("generate_adjust")
      .set("spark.akka.timeout", "10000")
      .set("spark.network.timeout", "10000")
      .set("spark.akka.askTimeout", "10000")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val simpleDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    val commonConnection = DBConnectionManager.getConnection("common")
    val bd09Host = DBHelper.Table(hiveContext, commonConnection.getUrl(), "Global_BD09_Host")
    val hosts = bd09Host.collect().map(s => s.getAs[String](0))

    val hostStringBuilder = new StringBuilder()
    for(host <- hosts) {
      if(hostStringBuilder.isEmpty)
        hostStringBuilder.append("'" + host + "'")
      else
        hostStringBuilder.append(",'" + host + "'")
    }


    val s1u_bd = hiveContext.sql("select s1_u.procedure_start_time, s1_u.procedure_end_time, s1_u.imsi, s1_u.imei, s1_u.cell_id, s1_u.host, s1_u.uri, " +
      "'bd09' as coordinate, s1_u.msisdn from base_parameter_cell cell inner join (select * from s1_u where start_time = '" + time + "' and host in (" + hostStringBuilder.toString() + ")) s1_u " +
      "on cell.objectid = s1_u.cell_id")
    val s1u_gcj = hiveContext.sql("select s1_u.procedure_start_time, s1_u.procedure_end_time, s1_u.imsi, s1_u.imei, s1_u.cell_id, s1_u.host, s1_u.uri, " +
      "'bd09' as coordinate, s1_u.msisdn from base_parameter_cell cell inner join (select * from s1_u where start_time = '" + time + "' and host not in (" + hostStringBuilder.toString() + ")) s1_u " +
      "on cell.objectid = s1_u.cell_id")

    val s1_u_rdd = s1u_bd.union(s1u_gcj)


    //todo add a .rdd
    val s1u_http = s1_u_rdd.rdd.map(s => (httpParser.parse_uri(s.getAs[String](6)), s))
      .filter(s => Try(s._1.head._1.toDouble).isSuccess && Try(s._1.head._2.toDouble).isSuccess)
      .filter(s => !utils.geoUtil.outOfChina(s._1.head._1.toDouble, s._1.head._2.toDouble))
      .zipWithUniqueId().map(s => (s._1._1, s._1._2, s._2))






    val s1u_http_uri_bd09 = s1u_http.filter(s => s._2.getAs[String](7) == "bd09")
      .map(s => (geoUtil.bd_decrypt(s._1.head._1.toDouble, s._1.head._2.toDouble), s._2, s._3))
      .map(s => S1UClass(s._3.toString, s._2.getAs[Int](4), Timestamp.valueOf(simpleDateFormat.format(new java.util.Date(s._2.getAs[Long](0)))),
        Timestamp.valueOf(simpleDateFormat.format(new java.util.Date(s._2.getAs[Long](1)))), s._2.getAs[String](2),
        s._2.getAs[String](3), s._2.getAs[String](8), s._2.getAs[String](5),
        s._2.getAs[String](6), s._1._2.toDouble, s._1._1.toDouble,
        math.floor(s._1._2.toDouble * 100) / 100, math.floor(s._1._1.toDouble * 100) / 100, s._2.getAs(7))
      ).toDF()
    val s1u_http_uri_gcj = s1u_http.filter(s => s._2.getAs[String](7) == "gcj")
      .map(s => S1UClass(s._3.toString, s._2.getAs[Int](4), Timestamp.valueOf(simpleDateFormat.format(new java.util.Date(s._2.getAs[Long](0)))),
        Timestamp.valueOf(simpleDateFormat.format(new java.util.Date(s._2.getAs[Long](1)))), s._2.getAs[String](2),
        s._2.getAs[String](3), s._2.getAs[String](8), s._2.getAs[String](5),
        s._2.getAs[String](6), s._1.head._2.toDouble, s._1.head._1.toDouble,
        math.floor(s._1.head._2.toDouble * 100) / 100, math.floor(s._1.head._1.toDouble * 100) / 100, s._2.getAs(7))
      ).toDF()

    val s1u_http_uri = s1u_http_uri_bd09.unionAll(s1u_http_uri_gcj)

    //val s1u_http_uri = s1u_all.repartition(200)
    //s1u_http_uri.registerTempTable("s1u_http_uri")
    //hiveContext.sql("insert into table d_s1u_http_lonlat partition (start_time = '" + time + "') select * from s1u_http_uri")
    //-----------------------经纬度提取完成----------------------

    //s1u与s1mme关联
    //从hive表中读取S1MME数据
    val s1_mme_rdd = hiveContext.sql(
      "select cast(procedure_start_time as timestamp) as begintime, cast(procedure_end_time as timestamp) as endtime, cast(mmeues1apid as string), imsi, imei, msisdn, cell_id as cellid from s1mme" +
        " inner join base_parameter_cell cell on s1mme.cell_id = cell.objectid where start_time = '" + time + "'")

    val s1_mme = s1_mme_rdd.map(s =>
      S1MMEClass(s.getAs[Timestamp](0), s.getAs[Timestamp](1), s.getAs[String](2),
        s.getAs[String](3), s.getAs[String](4), s.getAs[String](5), s.getAs[Int](6)))
      .toDF()

    val Temp10 = s1u_http_uri.join(s1_mme, s1u_http_uri("imsi") === s1_mme("imsi") && s1u_http_uri("cellid") === s1_mme("cellid"))
      .filter(udfFunctions.TimestampAdd(s1u_http_uri("begintime"), lit(10)) >= s1u_http_uri("endtime") && s1u_http_uri("begintime") > s1_mme("begintime"))
      .select(s1u_http_uri("oid").as("s1_uoid"), s1u_http_uri("cellid"), s1u_http_uri("begintime").as("S1_U_Begin"), s1u_http_uri("endtime").as("S1_U_End"),
        s1u_http_uri("imsi"), s1u_http_uri("imei"), s1_mme("msisdn"), s1u_http_uri("latitude"), s1u_http_uri("longitude"), s1u_http_uri("latitude2"), s1u_http_uri("longitude2"),
        s1u_http_uri("uri"), s1_mme("begintime").as("S1_MME_Begin"), s1_mme("mmeues1apid"),
        udfFunctions.TimestampDiff(s1_mme("begintime"), s1u_http_uri("begintime")).as("S1_U_Diff_S1_MME"))

    val Temp11 = Temp10.groupBy("s1_uoid").min("S1_U_Diff_S1_MME").toDF().withColumnRenamed("min(S1_U_Diff_S1_MME)", "MIN_S1_U_Diff_S1_MME").withColumnRenamed("S1_UOID", "S1_UOID1")

    val Temp12 = Temp11.join(Temp10, Temp11("S1_UOID1") === Temp10("S1_UOID") && Temp11("MIN_S1_U_Diff_S1_MME") === Temp10("S1_U_Diff_S1_MME"))
      .select(Temp10("S1_UOID"), Temp10("CellID"), Temp10("S1_U_Begin"), Temp10("S1_U_End"), Temp10("IMSI"), Temp10("IMEI"), Temp10("MSISDN"), Temp10("Latitude"), Temp10("Longitude"), Temp10("latitude2"), Temp10("longitude2"),  Temp10("Uri"),
        Temp10("S1_MME_Begin"), Temp10("mmeues1apid").as("mmeues1apid1"))

    val s1u_join_s1mme = Temp12.select(Temp12("S1_UOID"), Temp12("CellID"), Temp12("Latitude"), Temp12("Longitude"), Temp12("Latitude2"), Temp12("Longitude2"), Temp12("mmeues1apid1").as("mmeues1apid"), Temp12("Uri"), Temp12("S1_U_Begin").as("BeginTime"), Temp12("IMSI"))

    //与mro进行关联
    val mro = hiveContext.sql("select cast(regexp_replace(timestamp, 'T', ' ') as timestamp) timestamp, enbid as ENodeBID, mro.objectid, cell.latitude as cell_latitude, cell.longitude as cell_longitude, mmeues1apid, ltescpci, ltescrsrp-141 as ltescrsrp, ltescrsrq/2-20 as ltescrsrq, ltesctadv, ltescaoa/2 as ltescaoa," +
      "ltescsinrul-11 as ltescsinrul, ltencobjectid, ltencrsrp-141 as ltencrsrp, cell.city from base_parameter_cell cell inner join (select * from mro " +
      " where day = '" + day + "' and houre = '" + hour + "') mro on mro.objectid = cell.objectid")

    val mr_join_signal_original = s1u_join_s1mme.join(mro, mro("objectid") === s1u_join_s1mme("CellID") && mro("mmeues1apid") === s1u_join_s1mme("mmeues1apid")
      && mro("timestamp") >= udfFunctions.TimestampAdd(s1u_join_s1mme("BeginTime"), lit(AppSettings.SignalTimeStampSubtractValue * (-1) + AppSettings.MRTimeStampLaterThanSignalTimeStampValue))
      && mro("timestamp") <= udfFunctions.TimestampAdd(s1u_join_s1mme("BeginTime"), lit(AppSettings.SignalTimeStampSubtractValue + AppSettings.MRTimeStampLaterThanSignalTimeStampValue)))
      .where(udfFunctions.isS1PointInCellUdf(s1u_join_s1mme("Latitude"), s1u_join_s1mme("Longitude"), mro("cell_latitude"), mro("cell_longitude"), mro("ltesctadv")))
      .select(s1u_join_s1mme("S1_UOID"), s1u_join_s1mme("Latitude"),
        s1u_join_s1mme("Longitude"), s1u_join_s1mme("Latitude2"), s1u_join_s1mme("Longitude2"), mro("ENodeBID"),
        mro("objectid"), mro("ltescrsrp"), mro("ltescrsrq"), mro("ltescsinrul"),s1u_join_s1mme("BeginTime"),
        mro("ltesctadv"), mro("ltencobjectid"), mro("ltencrsrp"), abs(udfFunctions.TimestampDiff(mro("timestamp"), s1u_join_s1mme("BeginTime"))).as("TimeDiff"),
        mro("city"), s1u_join_s1mme("IMSI"))
    //.select(s1u_join_s1mme("Latitude"), s1u_join_s1mme("Longitude"), mro("cell_latitude"), mro("cell_longitude"), mro("ltesctadv"))

    mr_join_signal_original.registerTempTable("mr_join_signal_original")
    val mr_join_signal = hiveContext.sql("select * from mr_join_signal_original m inner join (select s1_uoid as s1_uoid1, min(timediff) as min_timediff from mr_join_signal_original group by s1_uoid) t" +
      " on (m.s1_uoid = t.s1_uoid1 and m.timediff = t.min_timediff)")

    //火星转为gps
    mr_join_signal.registerTempTable("mr_join_signal")

    val gcj2wgs = DBHelper.Table(hiveContext, commonConnection.getUrl(), "GCJ2WGS")
    gcj2wgs.registerTempTable("gcj2wgs")

    val mr_join_signal_gps = hiveContext.sql("select /*+ MAPJOIN(g) */ s1_uoid, city, m.latitude + g.LatDvalue as latitude," +
      "m.longitude + g.LngDvalue as longitude, m.enodebid, m.objectid, ltescrsrp, ltescrsrq, ltescsinrul, ltencobjectid, ltencrsrp, timediff, imsi, begintime " +
      "from gcj2wgs g right join  mr_join_signal m on (m.Latitude2 = g.Latitude and m.Longitude2 = g.Longitude)")
      .withColumn("Longitude4", udfFunctions.floor($"Longitude", lit(4))).withColumn("Latitude4", udfFunctions.floor($"Latitude", lit(4)))

    val finger = hiveContext.sql("select gridid, gridx, gridy, latitude, longitude, latitude4, longitude4 from finger_total")

    val mr_join_signal_result = mr_join_signal_gps.join(finger, finger("Longitude4") === mr_join_signal_gps("Longitude4")
      && finger("Latitude4") === mr_join_signal_gps("Latitude4"))
      .select(mr_join_signal_gps("city"), mr_join_signal_gps("latitude"), mr_join_signal_gps("longitude"),
        mr_join_signal_gps("enodebid"), mr_join_signal_gps("objectid"), mr_join_signal_gps("ltescrsrp"),mr_join_signal_gps("ltencobjectid"), mr_join_signal_gps("ltencrsrp"),
        mr_join_signal_gps("ltescrsrq"), mr_join_signal_gps("ltescsinrul"),
        finger("gridid"), mr_join_signal_gps("imsi"), mr_join_signal_gps("begintime"), finger("longitude").as("grid_longitude"), finger("latitude").as("grid_latitude"),
        finger("gridx"), finger("gridy"))

    mr_join_signal_result.registerTempTable("temp_mr_join_signal_result")

    hiveContext.sql("create table if not exists ott_grid_result (city string,objectid int, enodebid int," +
      "longitude double, latitude double, grid_longitude double, grid_latitude double, gridx int, gridy int, ltescrsrp double, ltescrsrq double,ltescsinrul double, ltencobjectid int, ltencrsrp double, gridid bigint, imsi string, begintime timestamp) partitioned by (day string)")

    hiveContext.sql("insert into table ott_grid_result partition (day = '" + day + "') select distinct city, objectid, enodebid, longitude, latitude, grid_longitude, grid_latitude, gridx, gridy, " +
      "ltescrsrp, ltescrsrq, ltescsinrul, ltencobjectid, ltencrsrp, gridid, imsi, begintime from temp_mr_join_signal_result")

    println("=======" + time + "===success=======")

    sc.stop()
  }*/
}
