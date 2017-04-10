package com.dtmobile.spark.biz.businessexception

import  org.apache.spark.sql.{SaveMode, SparkSession}
/**
  * Created by shenkaili on 17-4-1.
  */
class businessexception (ANALY_DATE: String,ANALY_HOUR: String,SDB: String, DDB: String, warhouseDir: String,
                         XDRthreshold01:Int,XDRthreshold02:Int,XDRthreshold03:Int,XDRthreshold04:Int,
                         XDRthreshold05:Int,XDRthreshold06:Int,XDRthreshold07:Int,
                         threshold01:Int,threshold02:Int,threshold03:Int,threshold04:Int,threshold05:Int,
  threshold06:Int,threshold07:Int){


    def analyse(implicit sparkSession: SparkSession): Unit = {
      exceptionAnalyse(sparkSession)
      
    }
   def exceptionAnalyse(implicit sparkSession: SparkSession): Unit = {
     import sparkSession.sql
     sql(s"use $DDB")
     sql(s"alter table t_xdr_event_msg add if not exists partition(dt=$ANALY_DATE,,h=$ANALY_HOUR))")
     sql(
       s"""

       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/t_xdr_event_msg/dt=$ANALY_DATE/$ANALY_HOUR")


  }

}
