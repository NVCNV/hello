package com.dtmobile.spark.biz.inek.framework_v2.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by zhoudehu on 2017/11/8/0008.
  */
object Rasterize {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("rasterize")
    val hiveContext = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import hiveContext.sql

    sql(
      """
        |insert overwrite table liaoning.liaoning_lte_mro_source
        |  select t10.*,
        |         t11.lon         as mrlongitude,
        |         t11.lat         as mrlatitude,
        |         t11.grid_height as height
        |    from result.lte_mro_source t10
        |   inner join (select d2.objectid,
        |                      d2.mmeues1apid,
        |                      d2.time_stamp,
        |                      d2.ltescrsrp,
        |                      d2.ltescrsrq,
        |                      d2.ltescsinrul,
        |                      d2.Grid_row_id,
        |                      d2.Grid_col_id,
        |                      d2.lon,
        |                      d2.lat,
        |                      d2.grid_height,
        |                      d2.distance
        |                 from (select d1.objectid,
        |                              d1.mmeues1apid,
        |                              d1.time_stamp,
        |                              d1.ltescrsrp,
        |                              d1.ltescrsrq,
        |                              d1.ltescsinrul,
        |                              d1.Grid_row_id,
        |                              d1.Grid_col_id,
        |                              d1.lon,
        |                              d1.lat,
        |                              d1.grid_height,
        |                              d1.distance,
        |                              row_number() over(partition by d1.objectid, d1.mmeues1apid, d1.time_stamp order by d1.distance asc) rn
        |                         from (select c1.objectid,
        |                                      c1.mmeues1apid,
        |                                      c1.time_stamp,
        |                                      c1.ltescrsrp,
        |                                      c1.ltescrsrq,
        |                                      c1.ltescsinrul,
        |                                      c2.Grid_row_id,
        |                                      c2.Grid_col_id,
        |                                      c2.lon,
        |                                      c2.lat,
        |                                      c2.grid_height,
        |                                      sum(((c1.ltescrsrp - c1.ncrsrp) -
        |                                          (c2.RSRP - c2.n_rsrp)) *
        |                                          ((c1.ltescrsrp - c1.ncrsrp) -
        |                                          (c2.RSRP - c2.n_rsrp))) / COUNT(1) distance
        |                                 from (select cellid      as objectid,
        |                                              mmeues1apid,
        |                                              mrtime      as time_stamp,
        |                                              kpi1        as ltescrsrp,
        |                                              kpi3        as ltescrsrq,
        |                                              kpi8        as ltescsinrul,
        |                                              kpi25       as ncellobjectid,
        |                                              kpi2        as ncrsrp
        |                                         from result.lte_mro_source
        |                                        where dt = 20171108
        |                                          and h = 11) c1
        |                                inner join default.finger c2
        |                                   on (c1.objectid = cast(split(c2.objectid, '_') [ 0 ] as int) * 256 + cast(split(c2.objectid, '_') [ 1 ] as int) and
        |                                      c1.ncellobjectid = cast(split(c2.n_objectid, '_') [ 0 ] as int) * 256 +cast(split(c2.n_objectid, '_') [ 1 ] as int))
        |                                group by c1.objectid,
        |                                         c1.mmeues1apid,
        |                                         c1.time_stamp,
        |                                         c1.ltescrsrp,
        |                                         c1.ltescrsrq,
        |                                         c1.ltescsinrul,
        |                                         c2.Grid_row_id,
        |                                         c2.Grid_col_id,
        |                                         c2.lon,
        |                                         c2.lat,
        |                                         c2.grid_height) d1) d2
        |                where d2.rn = 1) t11
        |      on (t10.cellid = t11.objectid and t10.mrtime = t11.time_stamp and t10.mmeues1apid = t11.mmeues1apid)
        |      where t10.dt=20171108 and t10.h=11
      """.stripMargin)
  }




}
