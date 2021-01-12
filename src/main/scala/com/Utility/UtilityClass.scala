package com.Utility

import org.apache.spark.sql.SparkSession

object UtilityClass {

  /***
    * Creates SparkSession object
    * @param appName String
    * @return SparkSession
    */
  def createSparkSessionObj(appName: String): SparkSession = {
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(appName)
      .config("spark.streaming.stopGracefullyOnShutdown", value = true)
      .getOrCreate()
    sparkSession
  }
}
