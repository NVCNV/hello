package com.dtmobile.spark.biz.inek.framework_v2.spark

import java.sql.Timestamp

import com.dtmobile.spark.biz.inek.model.S1UClass
import com.dtmobile.spark.biz.inek.utils.{geoUtil, httpParser}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
 * Created by xuximing on 2017/7/26.
 */
object common {
  def s1u_getpoints(s1u_original:DataFrame): RDD[S1UClass] = {
    val simpleDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    val s1u_http = s1u_original.map(s => (httpParser.parse_uri(s.getAs[String](6)), s))
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
      )
    val s1u_http_uri_gcj = s1u_http.filter(s => s._2.getAs[String](7) == "gcj")
      .map(s => S1UClass(s._3.toString, s._2.getAs[Int](4), Timestamp.valueOf(simpleDateFormat.format(new java.util.Date(s._2.getAs[Long](0)))),
        Timestamp.valueOf(simpleDateFormat.format(new java.util.Date(s._2.getAs[Long](1)))), s._2.getAs[String](2),
        s._2.getAs[String](3), s._2.getAs[String](8), s._2.getAs[String](5),
        s._2.getAs[String](6), s._1.head._2.toDouble, s._1.head._1.toDouble,
        math.floor(s._1.head._2.toDouble * 100) / 100, math.floor(s._1.head._1.toDouble * 100) / 100, s._2.getAs(7))
      )

    s1u_http_uri_bd09.union(s1u_http_uri_gcj)

  }
}
