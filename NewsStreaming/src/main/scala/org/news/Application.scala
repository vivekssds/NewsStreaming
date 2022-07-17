package org.news

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

object Application extends App {

  val applicationName = sys.env.getOrElse("APP_NAME", "newsstreaming(default)")

  private val parser = new scopt.OptionParser[ApplicationConfig](applicationName) {
    head(applicationName, "1.0")

    opt[String]("applicationDisplayName").action( (x, c) =>
      c.copy( applicationDisplayName = x) ).text("name to show in spark UI")

    opt[String]("sparkMaster").action( (x, c) =>
      c.copy( sparkMaster = x) ).text("sparkMaster is loaded from env SPARK_MASTER but can be overwritten from command line")

    opt[String]("streamSource").action( (x, c) =>
      c.copy( streamSource = x) ).text("source for streaming can be KAFKA or DELTA")

    opt[String]("streamFromTableVersion").action( (x, c) =>
      c.copy( streamFromTableVersion = x) ).text("Delta table version to start streaming from")

    opt[String]("kafkaConfigId").action( (x, c) =>
      c.copy( kafkaConfigId = x) ).text("Kafka configuration to use")

    opt[String]("kafkaStartingOffsets").action( (x, c) =>
      c.copy( inputkafkaStartingOffsets = x) ).text("Kafka streaming starting offset")

    opt[String]("streamSink").action( (x, c) =>
      c.copy( streamSink = x) ).text("Target Cassandra")



  }

  private val config = parser.parse(args, ApplicationConfig(AppName = applicationName)) match {
    case Some(c) => c

    case None =>
      sys.exit(0)
  }

  val newConfig = config
    .copy(cassandrahost = config.cassandrahost)
    .copy(cassandrauser = config.cassandrauser)
    .copy(cassandrapwd = config.cassandrapwd)
    .copy(kafkabroker = config.kafkabroker)
    .copy(subscribertopic = config.subscribertopic)
    .copy(inputkafkaStartingOffsets = config.inputkafkaStartingOffsets)
    .copy(checkpointlocation = config.checkpointlocation)
    .copy(streamFailOnDataLoss = config.streamFailOnDataLoss)
    .copy(maxBytesPerTrigger = config.maxBytesPerTrigger)
    .copy(eventAction=config.eventAction)

    println(newConfig)
//  logger.info("loadIndex :  Creating worker object")

  // The job driver
  val controller: Controller = new Controller(newConfig)
  controller.start()

//  logger.info("loadIndex :  Exiting the application: " + this.getClass.getPackage)


}
