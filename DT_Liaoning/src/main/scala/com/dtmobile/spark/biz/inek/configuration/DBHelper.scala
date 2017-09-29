package com.dtmobile.spark.biz.inek.configuration

import inek.configuration.{DBConnectionManager, DBConnection}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

/**
 * Created by xuximing on 2017/3/10.
 */
object DBHelper {
  def Table(hc:HiveContext, conn:DBConnection, tableName:String) : DataFrame = {
    val url = conn.getUrl()
    Table(hc, url, tableName)
  }

  def Table(hc:HiveContext, url:String, tableName:String) : DataFrame = {
    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    hc.load("jdbc", Map("url" -> url, "driver" -> driver, "dbtable" -> tableName))
  }

  def Table_All(hc:HiveContext, tableName:String):DataFrame = {
    val conns = DBConnectionManager.getAllConnections()

    val firstConn = conns.head
    val db = hc.load("jdbc", Map("url" -> firstConn.getUrl(), "driver" -> firstConn.driver, "dbtable" -> tableName))
    var result = db.withColumn("city", lit(firstConn.key))

    for(i <- 1 until conns.size)
    {
      val conn = conns(i)
      val db = hc.load("jdbc", Map("url" -> conn.getUrl(), "driver" -> conn.driver, "dbtable" -> tableName))
      val part = db.withColumn("city", lit(conn.key))
      result = result.unionAll(part)
    }
    result
  }

}
