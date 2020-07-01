package com.oujian

import org.apache.spark.rdd.RDD

class Search(query:String) extends Serializable {
    def getMatche2(rdd:RDD[String]):RDD[String]={
      rdd.filter(r=>r.contains(query))
    }
}
