package com.dtmobile.spark.biz.gridanalyse

import com.dtmobile.spark.SparkSessionSingleton
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoudehu on 2017/5/10/0010.
  */
object Test {

  def main(args: Array[String]) {
//   val pci = new PCIOptimize("","","","","")
////    val t = sc.sql("select operator,value from ltepci_degree_condition where field = 'cellrsrpcoverth'").collectAsList()
//    val sRsrpLap = t.get(0).getString(0)+" "+t.get(0).getDecimal(1)
//    val t2 = sc.sql("select operator,value from ltepci_degree_condition where field = 'adjcellrsrpeffectiveth'").collectAsList()
//    val adjRsrpthresh = t2.get(0).getString(0)+" "+t2.get(0).getDecimal(1)
//
//print(sRsrpLap)
//    print(adjRsrpthresh)
/*
  val conf= new SparkConf().setAppName("PCI").setMaster("spark://172.30.4.189:7077");
//    val ssc = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
    val sc= SparkSessionSingleton.getInstance(conf)
    val i =new Init("","","","","","172.30.4.187:1521/morpho0307")
    i.InitLteCell(sc)
   /* i.TableUtil(sc)*/
val warhouseDir: String = "/user/hive/warehouse/" + "morpho"+ ".db"
    val p = new PCIOptimize("","","morpho2","morpho2","warhouseDir")

      p.analyse(sc)
*/




  }



}
