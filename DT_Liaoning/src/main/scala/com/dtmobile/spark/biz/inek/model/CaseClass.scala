/**
 * Created by xuximing on 2017/1/15.
 */
package com.dtmobile.spark.biz.inek.model

case class S1UClass(OID: String, CellID: Int, BeginTime: java.sql.Timestamp, EndTime: java.sql.Timestamp,
                    IMSI: String, IMEI: String, MSISDN: String, Host: String, Uri: String, Longitude: Double, Latitude: Double,
                    Longitude2:Double, Latitude2:Double, Coordinate:String)

case class S1MMEClass(BeginTime: java.sql.Timestamp, EndTime: java.sql.Timestamp, MmeUeS1apId: String,
                      IMSI: String, IMEI: String, MSISDN: String, cellid: Int)

case class MROClass(TimeStamp: java.sql.Timestamp, ObjectID:Int, SiteOID:Int, MmeUeS1apId:String, LteScRSRP:Double, LteScRSRQ:Double, LteScRTTD:Option[Double],
                    LteScSinrUL: Option[Double])
case class MROClass3(TimeStamp:java.sql.Timestamp, ObjectID:Int, SiteOID:Int, MmeUeS1apId:String, LteScRSRP:Double, LteScRSRQ:Double, LteScRTTD:Option[Double],
                     LteScSinrUL:Option[Double], LteCellOID:Int, LteNcRSRP:Double, LteNcRSRQ:Double)



case class S1UClass2(OID:Long, CellID:Int, BeginTime: java.sql.Timestamp, EndTime: java.sql.Timestamp, UserIP:String, GgsnDataTEId:Double, SgsnDataTEId:Double,
                     IMSI:String, IMEI:String, MSISDN:String, Uri:String, Longitude:Double, Latitude:Double)


case class InitS1MMEClass(BeginTime: java.sql.Timestamp, EndTime: java.sql.Timestamp, /*ENodeBID: Int, */ Mmes1apUEId: String, UEipV4: String, /*EarbDLteId: Double, EarbULteId: Double,*/
                          IMSI: String, IMEI: String, MSISDN: String, Eci: Int)
case class S1MMEClass2(BeginTime:  java.sql.Timestamp, EndTime: java.sql.Timestamp,ENodeBID:Int, Mmes1apUEId:String, UEipV4:String,EarbDLteId:Double,EarbULteId:Double,
                       IMSI:String, IMEI:String, MSISDN:String, Eci:Int)

case class fingerprint_info (gridlength:Int, columncount:Int, minX:Double, minY:Double, maxX:Double, maxY:Double, city:String)

case class grid_lonlat(gridid:Long, longitude:Double, latitude:Double)