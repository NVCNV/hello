package com.dtmobile.spark.biz.inek.configuration

/**
 * Created by xuximing on 2016/5/19.
 */
class DBConnection {
  var key = ""
  val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  var ip = "10.101.218.66"
  var databasename = "iNek_Develop_chengdu_S"
  var username = "Netplan_Develop_AppUser"
  var password = "Netplan2012"

  def getUrl() = {
    "jdbc:sqlserver://"+ip+":1433;DatabaseName="+databasename+";user="+username+";password="+password
  }
}
