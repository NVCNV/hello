package com.dtmobile.spark.biz.inek.framework_v2.spark

import java.sql.Timestamp

import com.dtmobile.spark.biz.inek.utils.geoUtil
import org.apache.spark.sql.functions._

/**
 * Created by xuximing on 2017/7/26.
 */
object udfFunctions {
  val isS1PointInCellUdf = udf((latitude: Double, longitude: Double, siteLat: Double, siteLon: Double, lteScRTTD: Double) => {
    val site_gcj = geoUtil.transform(siteLat, siteLon)
    val distance = geoUtil.getDistanceByLatAndLon(latitude, longitude, site_gcj._1, site_gcj._2)
    val rttdValue = (lteScRTTD + 4) * 78.125
    rttdValue >= distance
  })

  val TimestampDiff = udf((timestamp1: Timestamp, timestamp2: Timestamp) => {
    timestamp2.getTime - timestamp1.getTime
  })

  val TimestampAdd = udf((timestamp : Timestamp, second: Int) => {
    val time = timestamp.getTime + second * 1000

    new Timestamp(time)
  })

  val floor = udf((d:Double, i:Int) => {
    math.floor(d * math.pow(10, i)) / math.pow(10, i)
  })

  val log10 = udf((d:Double) => {
    math.log10(d)
  })

  val getCalibrateGridId = udf((point_lon_mercator:Double, point_lat_mercator:Double, cell_lon_mercator:Double, cell_lat_mercator:Double,
                                cellHorizonAngleToEast:Int, angle_grid_count:Int, distance_grid_count:Int, grid_step:Int) => {
    // b) 计算 栅格坐标 与小区方向的夹角
    /* 计算栅格坐标和小区方向的夹角
     *   1、弧度 = arctan(y1-y1/x1-x1) )                           值在-180—180之间
     *   2、angleHoritonToEast=[30-(angleHoritonToEast-90)]%360
     *   3、角度1 = 角度0-angleHoritonToEast                        值在 -540—180之间
     *   4、角度2=（角度1+720）%360
    */
    // 1、弧度 = arctan(y1-y1/x1-x1) )    值在-180—180之间
    val radian: Double = math.atan2(point_lon_mercator - cell_lon_mercator, point_lat_mercator - cell_lat_mercator)
    // 将 弧度转换为角度 弧度转角度= 180/PI*弧度
    val angle0: Int = (180 / math.Pi * radian).toInt
    // 2、angleHoritonToEast=[30-(angleHoritonToEast-90)]%360
    val angleHoritonToEast: Int = (360 - (cellHorizonAngleToEast - 90)) % 360
    // 3、角度1 = 角度0-angleHoritonToEast
    val angle1: Int = angle0 - angleHoritonToEast
    // 4、角度2=（角度1+720）%360
    val angle2: Int = (angle1 + 720) % 360

    // c) 计算夹角的索引
    /*
     * 1、角度3=（角度2+180/a）%360;                             0-360
     * 2、tempA = Math.Cell(角度3*a/360)                         0-19
     */
    // 1、角度3=（角度2+180/a）%360;
    val angle3: Int = (angle2 + 180 / angle_grid_count) % 360
    // 2、计算角度索引 tempA = Math.Cell(角度3*a/360)
    val angleIndex: Int = math.floor(angle3 * angle_grid_count / 360d).toInt

    // 小区和栅格的中心点的距离
    val d: Double = geoUtil.getDistanceByMercator(point_lon_mercator, point_lat_mercator, cell_lon_mercator, cell_lat_mercator)
    // d) 计算距离索引
    // b = min(Math.Celling())
    val tempB: Double = math.min(math.floor(d / grid_step), distance_grid_count - 1)
    // e) 计算校准的栅格编号
    (angleIndex * distance_grid_count + tempB).toInt
  })

  val parseCalibrateGridId2Angle = udf((calibrateGridId:Int, distanceGridCount:Int, angleGridCount:Int) => {
    // 计算 角度索引=校准格ID/距离格数
    //int angleIndex = item.Key / parameter.DistanceGridCount;
    // 计算 距离索引=Math.Floor(校准格ID%距离格数)
    //int distanceIndex = item.Key % parameter.DistanceGridCount;
    // 计算 校准格 与小区扇区方位角的夹角
    //int angle = angleIndex * 360 / parameter.AngleGridCount;
    // 计算 校准格与小区扇区方位角
    //double distance = (distanceIndex + 0.5) * parameter.DistanceGridStep;
    val angleIndex = calibrateGridId / distanceGridCount
    //val distanceIndex = calibrateGridId % distanceGridCount
    val angle = angleIndex * 360 / angleGridCount
    //val distance = (distanceIndex + 0.5) * distanceGridStep

    angle
  })
  val parseCalibrateGridId2Distance = udf((calibrateGridId:Int, distanceGridCount:Int, distanceGridStep:Int) => {
    // 计算 角度索引=校准格ID/距离格数
    //int angleIndex = item.Key / parameter.DistanceGridCount;
    // 计算 距离索引=Math.Floor(校准格ID%距离格数)
    //int distanceIndex = item.Key % parameter.DistanceGridCount;
    // 计算 校准格 与小区扇区方位角的夹角
    //int angle = angleIndex * 360 / parameter.AngleGridCount;
    // 计算 校准格与小区扇区方位角
    //double distance = (distanceIndex + 0.5) * parameter.DistanceGridStep;
    //val angleIndex = calibrateGridId / distanceGridCount
    val distanceIndex = calibrateGridId % distanceGridCount
    //val angle = angleIndex * 360 / angleGridCount
    val distance = (distanceIndex + 0.5) * distanceGridStep

    distance
  })
}
