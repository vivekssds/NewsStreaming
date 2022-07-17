package org.news

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.SparkSession

trait InitSpark {

  def getSpark(config:ApplicationConfig): SparkSession = SparkSession.builder()
    .appName(config.AppName)
    .config("spark.sql.hive.convertMetastoreParquet","false")
    .config("spark.sql.sources.bucketing.enabled", "true")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.sql.parquet.writeLegacyFormat", "true")
    .config("spark.cassandra.connection.host",config.cassandrahost)
    .config("spark.cassandra.auth.username",config.cassandrauser)
    .config("spark.cassandra.auth.password",config.cassandrapwd)
    .enableHiveSupport()
    .getOrCreate()
}
