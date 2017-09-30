package com.dtmobile.spark.biz.inek.framework_v2.spark


import com.dtmobile.spark.biz.inek.configuration.DBHelper
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xuximing on 2017/6/14.
 */
object threed_mrlocate_gridavg {
  def main(args: Array[String]) {

    val maxHeight = 100
    val conf = new SparkConf().setAppName("threed_mrlocate_gridavg")
      .set("spark.akka.timeout", "10000")
      .set("spark.network.timeout", "10000")
      .set("spark.akka.askTimeout", "10000")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    var height = 0
    while(height <= maxHeight) {
      hiveContext.sql("create table if not exists  mrlocate_"+ height +
        " (gridid bigint, objectid bigint, longitude double, latitude double, mrpointnum bigint, weakpointnum bigint, avgrsrp double, avgrsrq double, avgsinrul double) ")
      hiveContext.sql("truncate table mrlocate_"+ height)
      hiveContext.sql("insert into mrlocate_" + height +
        " select m.GridID, m.ObjectID, m.Longitude, m.Latitude, count(1) as mrpointnum, sum(case when ltescrsrp-141 <= -110 then 1 else 0 end), AVG(ltescrsrp-141) as avgrsrp, AVG(ltescrsrq/2-20) as avgrsrq, AVG(ltescsinrul-11) as avgsinrul " +
        " from mrlocate_result m " +
        " where height = '" + height + "' group by m.GridID, m.Objectid, m.longitude, m.latitude ")
      height = height + 5
    }

    hiveContext.sql("create table if not exists building_floor_rsrp (buildingId int, height int, avgrsrp double) ")

    val gridmappingbuilding = DBHelper.Table_All(hiveContext, "gridmappingbuilding")
    gridmappingbuilding.registerTempTable("gridmappingbuilding_db")

    hiveContext.sql("create table if not exists gridmappingbuilding (gridid int, bulidingid int, isinndoor boolean)")
    hiveContext.sql("insert overwrite table gridmappingbuilding select gridid, bulidingid, isinndoor from gridmappingbuilding_db where isinndoor = 1")

    height = 0
    while(height <= maxHeight) {
      hiveContext.sql("insert into building_floor_rsrp" +
        " select g.buildingid, "+height+", avg(avgrsrp) from mrlocate_"+height+" m " +
        " inner join gridmappingbuilding g on m.gridid = g.gridid group by g.buildingid ")
      height = height + 5
    }

  }
}
