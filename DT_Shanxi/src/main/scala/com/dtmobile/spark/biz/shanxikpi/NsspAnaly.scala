package com.dtmobile.spark.biz.shanxikpi

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * NsspAnaly
  *
  * @author heyongjin
  * @create 2017/03/02 10:36
  *
  **/
class NsspAnaly(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, localStr:String, warhouseDir:String) {
  def analyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    //原始表初始化
    sql(s"use $SDB")
//    sql(
//      s"""
//         |alter table lte_mro_source add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
//         |location "/$localStr/LTE_MRO_SOURCE/${ANALY_DATE}/${ANALY_HOUR}"
//       """.stripMargin)
//    sql(
//      s"""
//         |alter table tb_xdr_ifc_uu add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
//         |location "/$localStr/TB_XDR_IFC_UU/${ANALY_DATE}/${ANALY_HOUR}"
//       """.stripMargin)
//    sql(
//      s"""
//         |alter table tb_xdr_ifc_x2 add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
//         |location "/$localStr/TB_XDR_IFC_X2/${ANALY_DATE}/${ANALY_HOUR}"
//       """.stripMargin)

//    sql(
//      s"""
//         |alter table tb_xdr_ifc_gxrx add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
//         |location "/$localStr/volte_rx/${ANALY_DATE}/${ANALY_HOUR}"
//       """.stripMargin)
    sql(
      s"""
         |alter table tb_xdr_ifc_http add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "/$localStr/s1u_http_orgn/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)
    sql(
      s"""
         |alter table tb_xdr_ifc_dns add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "/$localStr/s1u_dns_orgn/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)
//    sql(
//      s"""
//         |alter table tb_xdr_ifc_sv add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
//         |location "/$localStr/volte_sv/${ANALY_DATE}/${ANALY_HOUR}"
//       """.stripMargin)
//    sql(
//      s"""
//         |alter table tb_xdr_ifc_s1mme add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
//         |location "/$localStr/s1mme_orgn/${ANALY_DATE}/${ANALY_HOUR}"
//       """.stripMargin)
//    sql(
//      s"""
//         |alter table tb_xdr_ifc_mw add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
//         |location "/$localStr/volte_orgn/${ANALY_DATE}/${ANALY_HOUR}"
//       """.stripMargin)

    sql(s"use $DDB")
//    sql(s"alter table tb_xdr_ifc_uu add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
//    sql(s"alter table tb_xdr_ifc_x2 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
//    sql(s"alter table cell_mr add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
//    sql(s"alter table lte_mro_source add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")

//    sql(
//      s"""
//         |alter table tb_xdr_ifc_http add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
//         |location "/$localStr/s1u_http_orgn/${ANALY_DATE}/${ANALY_HOUR}"
//       """.stripMargin)


  }
}

