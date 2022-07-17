package org.news

import org.apache.spark.sql.SparkSession

trait BaseSharedSparkSession {

  @transient protected val _sparkSession: SparkSession = getSpark("Unit Tests for streaming ")

  private def getSpark(appName: String): SparkSession = {
    val s = SparkSession.builder()
      .appName(appName)
      .master("local[2]") // .enableHiveSupport()
      .config("spark.driver.host", "localhost")
      .getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    s
  }
}
