package com.xf.util

import org.apache.spark.sql.SparkSession

/**
  * Created by wei.wang on 2018/9/13 0013.
  */
class SparkConfig {

  def getSpark(): SparkSession = {
   val spark = SparkSession.builder()
      .master("local")
      .appName("name")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()
     spark
  }

}
