package com.dtmobile.spark.biz.inek.configuration

import scala.collection.mutable

/**
 * Created by xuximing on 2017/2/16.
 */
object connections {

  val lists = Map (
//    "common" -> ("10.113.251.98", "iNek_GCJToWGS", "sa", "new.1234"),
//    "chengdu" -> ("10.113.251.98", "iNek_ChengDu_5", "sa", "new.1234"),
//    "mianyang" -> ("10.113.251.98", "iNek_SiChuan_MianYang20170122", "sa", "new.1234"),
//    "aba" -> ("10.113.251.98", "iNek_aba_5", "sa", "new.1234"),
//    "bazhong" -> ("10.113.251.98", "iNek_bazhong_5", "sa", "new.1234"),
//    "dazhou" -> ("10.113.251.98", "iNek_dazhou_5", "sa", "new.1234"),
//    "ganzi" -> ("10.113.251.98", "iNek_ganzi_5", "sa", "new.1234"),
//    "guangan" -> ("10.113.251.98", "iNek_guangan_5", "sa", "new.1234"),
//    "guangyuan" -> ("10.113.251.98", "iNek_guangyuan_5", "sa", "new.1234"),
//    "liangshan" -> ("10.113.251.98", "iNek_liangshan_5", "sa", "new.1234"),
//    "meishang" -> ("10.113.251.98", "iNek_meishang_5", "sa", "new.1234"),
//    "nanchong" -> ("10.113.251.98", "iNek_nanchong_5", "sa", "new.1234"),
//    "neijiang" -> ("10.113.251.98", "iNek_neijiang_5", "sa", "new.1234"),
//    "panzhihua" -> ("10.113.251.98", "iNek_panzhihua_5", "sa", "new.1234"),
//    "suining" -> ("10.113.251.98", "iNek_suining_5", "sa", "new.1234"),
//    "yaan" -> ("10.113.251.98", "iNek_yaan_5", "sa", "new.1234"),
//    "yibin" -> ("10.113.251.98", "iNek_yibin_5", "sa", "new.1234"),
//    "zigong" -> ("10.113.251.98", "iNek_zigong_5", "sa", "new.1234"),
//    "ziyang" -> ("10.113.251.98", "iNek_ziyang_5", "sa", "new.1234")
      "shanghai" -> ("10.254.188.160", "iNek_Shanghai_test", "sa", "new.1234" )
  )


  def getDBInfo(key:String): (String, String, String, String) =
  {
    lists.apply(key)
  }
}
