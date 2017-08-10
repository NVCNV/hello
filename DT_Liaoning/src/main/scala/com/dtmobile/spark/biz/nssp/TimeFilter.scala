package com.dtmobile.spark.biz.nssp

import java.text.SimpleDateFormat

import com.dtmobile.spark.Analyse
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by shenkaili on 17-7-4.
  */
class TimeFilters(ANALY_DATE: String, ANALY_HOUR: String,SDB: String,DDB: String,localStr:String,warhouseDir:String){
  val AL_DATE="${ANALY_DATE:0:(4)}-${ANALY_DATE:4:(2)}-${ANALY_DATE:6:(2)} ${ANALY_HOUR}:00:00"
  //    date -d "$AL_DATE" +%
  val sdf =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val TTIME=sdf.parse(AL_DATE)
  def analyse(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.sql
    sql(s"use $SDB")
    sql(
      s"""
         |alter table $SDB.lte_mro_source add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         |location "$localStr/LTE_MRO_SOURCE/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)
    sql(
      s"""
         |alter table $SDB.tb_xdr_ifc_uu add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "$localStr/TB_XDR_IFC_UU/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)
    sql(
      s"""
         |alter table $SDB.tb_xdr_ifc_x2 add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "$localStr/TB_XDR_IFC_X2/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)
    sql(
      s"""
         |alter table $DDB.lte_mro_source add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         |location "$warhouseDir/LTE_MRO_SOURCE/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)
    sql(
      s"""
         |alter table $DDB.tb_xdr_ifc_uu add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "$warhouseDir/TB_XDR_IFC_UU/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)
    sql(
      s"""
         |alter table $DDB.tb_xdr_ifc_x2 add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "$warhouseDir/TB_XDR_IFC_X2/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)
    sql(
      s"""
         |select
         |objectid,
         |vid,
         |fileformatversion,
         |starttime,
         |endtime,
         |period,
         |enbid,
         |userlabel,
         |mrname,
         |cellid,
         |earfcn,
         |subframenbr,
         |prbnbr,
         |mmeues1apid,
         |mmegroupid,
         |mmecode,
         |meatime,
         |eventtype,
         |gridcenterlongitude,
         |gridcenterlatitude,
         |kpi1,
         |kpi2,
         |kpi3,
         |kpi4,
         |kpi5,
         |kpi6,
         |kpi7,
         |kpi8,
         |kpi9,
         |kpi10,
         |kpi11,
         |kpi12,
         |kpi13,
         |kpi14,
         |kpi15,
         |kpi16,
         |kpi17,
         |kpi18,
         |kpi19,
         |kpi20,
         |kpi21,
         |kpi22,
         |kpi23,
         |kpi24,
         |kpi25,
         |kpi26,
         |kpi27,
         |kpi28,
         |kpi29,
         |kpi30,
         |kpi31,
         |kpi32,
         |kpi33,
         |kpi34,
         |kpi35,
         |kpi36,
         |kpi37,
         |kpi38,
         |kpi39,
         |kpi40,
         |kpi41,
         |kpi42,
         |kpi43,
         |kpi44,
         |kpi45,
         |kpi46,
         |kpi47,
         |kpi48,
         |kpi49,
         |kpi50,
         |kpi51,
         |kpi52,
         |kpi53,
         |kpi54,
         |kpi55,
         |kpi56,
         |kpi57,
         |kpi58,
         |kpi59,
         |kpi60,
         |kpi61,
         |kpi62,
         |kpi63,
         |kpi64,
         |kpi65,
         |kpi66,
         |kpi67,
         |kpi68,
         |kpi69,
         |kpi70,
         |kpi71,
         |length,
         |city,
         |xdrtype,
         |interface,
         |xdrid,
         |rat,
         |imsi,
         |imei,
         |msisdn,
         |mrtype,
         |neighborcellnumber,
         |gsmneighborcellnumber,
         |tdsneighborcellnumber,
         |v_enb,
         |mrtime
         |from ${SDB}.lte_mro_source where procedurestarttime>$TTIME and procedurestarttime<($TTIME+60*60*1000)
         |and dt=$ANALY_DATE
         |and h=$ANALY_HOUR
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/LTE_MRO_SOURCE/dt=$ANALY_DATE/h=$ANALY_HOUR")
    sql(
      s"""select
         |parentid,
         |length,
         |city,
         |interface,
         |xdrid,
         |rat,
         |imsi,
         |imei,
         |msisdn,
         |proceduretype,
         |procedurestarttime,
         |procedureendtime,
         |keyword1,
         |keyword2,
         |procedurestatus,
         |plmnid,
         |enbid,
         |cellid,
         |crnti,
         |targetenbid,
         |targetcellid,
         |targetcrnti,
         |mmeues1apid,
         |mmegroupid,
         |mmecode,
         |mtmsi,
         |csfbindication,
         |redirectednetwork,
         |epsbearernumber,
         |bearer0id,
         |bearer0status,
         |bearer1id,
         |bearer1status,
         |bearer2id,
         |bearer2status,
         |bearer3id,
         |bearer3status,
         |bearer4id,
         |bearer4status,
         |bearer5id,
         |bearer5status,
         |bearer6id,
         |bearer6status,
         |bearer7id,
         |bearer7status,
         |bearer8id,
         |bearer8status,
         |bearer9id,
         |bearer9status,
         |bearer10id,
         |bearer10status,
         |bearer11id,
         |bearer11status,
         |bearer12id,
         |bearer12status,
         |bearer13id,
         |bearer13status,
         |bearer14id,
         |bearer14status,
         |bearer15id,
         |bearer15status,
         |rangetime
         |from ${SDB}.tb_xdr_ifc_uu where procedurestarttime>$TTIME and procedurestarttime<($TTIME+60*60*1000) and dt=$ANALY_DATE
         |and h=$ANALY_HOUR
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/tb_xdr_ifc_uu/dt=$ANALY_DATE/h=$ANALY_HOUR")

    sql(
      s"""
         |select
         |parentid,
         |length,
         |city,
         |interface,
         |xdrid,
         |rat,
         |imsi,
         |imei,
         |msisdn,
         |proceduretype,
         |procedurestarttime,
         |procedureendtime,
         |procedurestatus,
         |cellid,
         |targetcellid,
         |enbid,
         |targetenbid,
         |mmeues1apid,
         |mmegroupid,
         |mmecode,
         |requestcause,
         |failurecause,
         |epsbearernumber,
         |bearer0id,
         |bearer0status,
         |bearer1id,
         |bearer1status,
         |bearer2id,
         |bearer2status,
         |bearer3id,
         |bearer3status,
         |bearer4id,
         |bearer4status,
         |bearer5id,
         |bearer5status,
         |bearer6id,
         |bearer6status,
         |bearer7id,
         |bearer7status,
         |bearer8id,
         |bearer8status,
         |bearer9id,
         |bearer9status,
         |bearer10id,
         |bearer10status,
         |bearer11id,
         |bearer11status,
         |bearer12id,
         |bearer12status,
         |bearer13id,
         |bearer13status,
         |bearer14id,
         |bearer14status,
         |bearer15id,
         |bearer15status,
         |rangetime
         |from $SDB.tb_xdr_ifc_x2 where procedurestarttime>$TTIME and procedurestarttime<($TTIME+60*60*1000) and dt=$ANALY_DATE
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/tb_xdr_ifc_x2/dt=$ANALY_DATE/h=$ANALY_HOUR")

  }
  }

