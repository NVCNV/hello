/**
 * Created by xuximing on 2017/2/16.
 */
package com.dtmobile.spark.biz.inek.configuration

import java.sql.Connection
import scala.collection.mutable.HashMap

object DBConnectionManager {
  def getConnection(key: String) : DBConnection = {
    val connection =  connections.getDBInfo(key)
    val dbconnection = new DBConnection()
    dbconnection.key = key
    dbconnection.ip = connection._1
    dbconnection.databasename = connection._2
    dbconnection.username = connection._3
    dbconnection.password = connection._4

    dbconnection
  }

  def getAllConnections(): scala.collection.immutable.List[DBConnection] = {
    var conns = List[DBConnection]()
    connections.lists.foreach(item => {
      if(item._1 != "common") {
        val conn = new DBConnection()
        conn.key = item._1
        conn.ip = item._2._1
        conn.databasename = item._2._2
        conn.username = item._2._3
        conn.password = item._2._4
        conns = conn :: conns
      }
    })

    conns.foreach(s => println(s.key + " " + s.getUrl()))
    conns
  }

  val connectionList = new HashMap[String, Connection]()
  def initConnections()={
    for(conn <- connectionList.values)
    {
      if(conn != null)
        conn.close()
    }
    connectionList.clear()
  }

  def getConnectionByCity(city:String):Connection = {
    if(connectionList.contains(city)) {
      connectionList.apply(city)
    }
    else {
      val db =  DBConnectionManager.getConnection(city)
      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      val conn = java.sql.DriverManager.getConnection(db.getUrl(), db.username, db.password)
      connectionList.put(city, conn)
      conn
    }
  }
}
