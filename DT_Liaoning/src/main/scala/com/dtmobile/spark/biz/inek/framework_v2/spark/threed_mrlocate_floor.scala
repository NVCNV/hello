package com.dtmobile.spark.biz.inek.framework_v2.spark

import com.dtmobile.spark.biz.inek.configuration.{AppSettings, DBHelper}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by xuximing on 2017/6/14.
 * mro
 * finger_total{height}
 * GridMappingBuilding
 *
 */
object threed_mrlocate_floor {
  def main(args: Array[String]) {
    val time = args(0)
    val day = time.substring(0, 8)
    val hour = time.substring(8, 10)
    val maxHeight = 100

    val conf = new SparkConf().setAppName("threed_mrlocate")
      .set("spark.akka.timeout", "10000")
      .set("spark.network.timeout", "10000")
      .set("spark.akka.askTimeout", "10000")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    import hiveContext.sql

    sql("create table if not exists mrlocate_floor (objectid bigint, mmeues1apid string," +
      "time_stamp string, ltescrsrp int, ltescrsrq int, ltescsinrul int, gridid bigint, longitude double, latitude double, height int, distance int) partitioned by (day string, hour string)")


    var height = 0
    while (height <=  AppSettings.maxHeight) {
      val fingerPrint_service = DBHelper.Table_All(hiveContext ,"FingerPrintDatabase_Service_" + height)
      val fingerPrint_neighbor = DBHelper.Table_All(hiveContext, "FingerPrintDatabase_NeighborCell_" + height)

      fingerPrint_service.createOrReplaceTempView("service_" + height)
      fingerPrint_neighbor.createOrReplaceTempView("neighbor_" + height)

      val fingerTotal = hiveContext.sql("select s.GridID, s.Longitude, s.Latitude, s.Objectid, s.RSRP, n.ObjectID n_objectid, n.RSRP n_rsrp " +
        "from service_" + height + " s inner join neighbor_" + height + " n " +
        " on s.GridID = n.ServiceGridID")

      fingerTotal.createOrReplaceTempView("finger" + height)
      sql("create table if not exists finger_total" + height + " (objectid int, gridid bigint, longitude double, latitude double, rsrp double, n_objectid int, n_rsrp int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")

      sql("insert overwrite table finger_total" + height + " select objectid, gridid, longitude, latitude, rsrp, n_objectid, n_rsrp from finger" + height)

/*
      sql(" insert into table mrlocate_floor PARTITION (day = '"+day+"', hour = '"+hour+"') " +
        " select d2.objectid,d2.mmeues1apid,d2.time_stamp,d2.ltescrsrp,d2.ltescrsrq,d2.ltescsinrul,d2.GridID, d2.longitude, d2.latitude, d2.height,d2.distance from " +
        " (" +
        "   select d1.objectid, d1.mmeues1apid,  d1.time_stamp, d1.ltescrsrp, d1.ltescrsrq, d1.ltescsinrul, d1.GridID, d1.longitude, d1.latitude, d1.height, d1.distance, " +
        "   row_number() over (partition by d1.objectid, d1.mmeues1apid,d1.time_stamp order by d1.distance asc ) rn  " +
        "   from (" +
        "         select c1.objectid, c1.mmeues1apid, c1.time_stamp,  c1.ltescrsrp, c1.ltescrsrq,  c1.ltescsinrul, c2.GridID, c2.Longitude, c2.Latitude, c2.height, sum(((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp))*((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp)))/COUNT(1) distance from  " +
        "             (select * from mro_orc where day = '"+day+"' and hour = '"+hour+"')  c1 inner join  " +
        "               (select GridID, Longitude, Latitude, ObjectID,RSRP,n_objectid,n_rsrp, "+height+" height  from finger_total"+height) c2 on (c1.objectid=c2.ObjectID and c1.ncellobjectid=c2.n_objectid) "+"  group by c1.objectid, c1.mmeues1apid,c1.time_stamp,c1.ltescrsrp, c1.ltescrsrq, c1.ltescsinrul,c2.GridID,c2.Longitude, c2.Latitude,c2.height" + "             having count(1) > 1" +
        "       ) d1 " +
        ") d2 where d2.rn=1")*/
      //todo: 改造上方sql
      sql(
        s"""
           | insert into table mrlocate_floor PARTITION (day = $day ,hour = $hour)
           | select d2.objectid,d2.mmeues1apid,d2.time_stamp,d2.ltescrsrp,d2.ltescrsrq,d2.ltescsinrul,d2.GridID, d2.longitude, d2.latitude, d2.height,d2.distance
           | from (
           | select d1.objectid, d1.mmeues1apid,  d1.time_stamp, d1.ltescrsrp, d1.ltescrsrq, d1.ltescsinrul, d1.GridID, d1.longitude, d1.latitude, d1.height, d1.distance,
           | row_number() over (partition by d1.objectid, d1.mmeues1apid,d1.time_stamp order by d1.distance asc ) rn
           |  from (
           |  select c1.objectid, c1.mmeues1apid, c1.time_stamp,  c1.ltescrsrp, c1.ltescrsrq,  c1.ltescsinrul, c2.GridID, c2.Longitude, c2.Latitude, c2.height, sum(((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp))*((c1.ltescrsrp-c1.ltencrsrp)-(c2.RSRP-c2.n_rsrp)))/COUNT(1) distance from
           |              (select * from mro_orc where day = $day and hour = $hour)  c1 inner join
           |           (select GridID, Longitude, Latitude, ObjectID,RSRP,n_objectid,n_rsrp, $height height
           |           from finger_total" $height) c2
           |           on (c1.objectid=c2.ObjectID and c1.ncellobjectid=c2.n_objectid)  group by c1.objectid, c1.mmeues1apid,c1.time_stamp,c1.ltescrsrp, c1.ltescrsrq, c1.ltescsinrul,c2.GridID,c2.Longitude, c2.Latitude,c2.height
           |             having count(1) > 1
           |             )d1
           |            ) d2 where d2.rn=1
           |
         """.stripMargin)





      height = height +  AppSettings.deltaHeight
    }


    //    hiveContext.sql("create table if not exists mrlocate_result (objectid bigint, mmeues1apid string," +
    //      "time_stamp string, ltescrsrp int, ltescrsrq int, ltescsinrul int, gridid bigint, longitude double, latitude double, height int) partitioned by (day string, hour string)")
    //
    //    hiveContext.sql("insert into table mrlocate_result PARTITION (day = '"+day+"', hour = '"+hour+"')" +
    //      " select b.objectid,b.mmeues1apid,b.time_stamp,b.ltescrsrp,b.ltescrsrq,b.ltescsinrul,b.gridid, b.longitude, b.latitude, b.height " +
    //      " from (select a.objectid,a.mmeues1apid,a.time_stamp, a.ltescrsrp,a.ltescrsrq,a.ltescsinrul,a.gridid,a.longitude, a.latitude,a.height,a.distance, " +
    //      " ROW_NUMBER() over (partition by a.objectid,a.mmeues1apid,a.time_stamp order by a.distance asc) rn " +
    //      " from mrlocate_floor a where day = '"+day+"' and hour = '"+hour+"') b where b.rn=1 ")
  }
}
