package org.news.processors
import org.news.{ApplicationConfig, InitSpark}
import com.typesafe.scalalogging._
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, _}
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.news.constants.constants


class NewsProcessor(config:ApplicationConfig)extends InitSpark {

  private val isDebug = config.debug
  private val spark = getSpark(config)
  private val streamSource=config.streamSource
  private val streamSink=config.streamSink

  def start(): Unit = {
    val queryListener = addStreamListener()
    if(isDebug) {
      println(s"streaming query listener ${queryListener.toString} added..")
    }

//    KAFKA--constants
    println(streamSource,streamSink)
    try {
      if (streamSource == constants.KAKFA && streamSink == constants.CASSANDRA){
        streamKafkatoCassandra()
      }

    }
    catch {
      case e: Exception =>
        e.printStackTrace()
//        logger.error(e.getMessage)
        throw e
    }

  }


  def addStreamListener(): StreamingQueryListener = {

    // callback will be executed every time the micro batch completes.
    val queryListener = new StreamingQueryListener() {

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        val progress = queryProgress.progress
        val inputRowsPerSecond = progress.inputRowsPerSecond
        val processedRowsPerSecond = progress.processedRowsPerSecond
        val numInputRows = progress.numInputRows
        val durationMs = progress.durationMs.toString
        val triggerExecutionDurationMs = progress.durationMs.get("triggerExecution")
        val queryPlanningDurationMs = progress.durationMs.get("queryPlanning")
        val getBatchDurationMs = progress.durationMs.get("getBatch")
        val addBatchDurationMs = progress.durationMs.get("addBatch")
        val walCommitDurationMs = progress.durationMs.get("walCommit")

        val progressJson = progress.prettyJson
        val batchTimestamp = progress.timestamp
        val source = progress.sources(0).description
        val name = progress.name
        val batchId = progress.batchId
        val id = progress.id.toString
        val runId = progress.runId.toString

        import spark.implicits._
        val df = Seq((name, source, batchTimestamp, durationMs, numInputRows, inputRowsPerSecond, processedRowsPerSecond,
          triggerExecutionDurationMs, queryPlanningDurationMs, getBatchDurationMs, addBatchDurationMs, walCommitDurationMs, progressJson, batchId, id, runId))
          .toDF("name", "source", "batchTimestamp", "durationMs", "numInputRows", "inputRowsPerSecond", "processedRowsPerSecond",
            "triggerExecutionDurationMs", "queryPlanningDurationMs", "getBatchDurationMs", "addBatchDurationMs", "walCommitDurationMs", "progressJson", "batchId", "id", "runId")

//
//        persist when some rows were processed in microbatch
//        if (numInputRows > 0) {
//          df.write.format("delta").mode("append").saveAsTable("sme.streamstats_tbl")
//        }
      }
      // We don't want to do anything with start or termination,
      // but we have to override them anyway
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = { }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = { }
    }

    spark.streams.addListener(queryListener)

    queryListener
  }

  private def streamKafkatoCassandra(): Unit ={

    val subscriberkafkatopic=config.subscribertopic
    val kafkabroker=config.kafkabroker
    val checkpointLocation=config.checkpointlocation

    val inputkafkaStartingOffsets=config.inputkafkaStartingOffsets
    val streamFailOnDataLoss=config.streamFailOnDataLoss


    val newsstreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkabroker)
      .option("subscribe",subscriberkafkatopic)
      .option("startingOffsets", inputkafkaStartingOffsets)
      .option("failOnDataLoss",streamFailOnDataLoss)
      .option( "maxBytesPerTrigger", "200m")
      .load()

    val newsjsonDF = newsstreamDF.selectExpr("CAST(value AS STRING)","CAST(partition as INTEGER)","CAST(offset as LONG)")

    val newsSchemaDF = new StructType()
      .add("articleid",IntegerType)
      .add("headline",StringType)
      .add("newscategory",StringType)
      .add("newssubcategory",StringType)
      .add("imageurl",StringType)
      .add("pagenumber",IntegerType)
      .add("pageorder",IntegerType)
      .add("userid",IntegerType)
      .add("newspublishedtime",StringType)
      .add("userviewtime",StringType)
      .add("hashtag",StringType)
      .add("hashtaggedtime",StringType)
      .add("messageid",StringType)
      .add("recid",IntegerType)


    val newsDF = newsjsonDF.select(from_json(col("value"), newsSchemaDF).as("data"),col("partition") ,col("offset"))
      .select("data.*")
      .filter("data.articleid is not null")


    var sqlnewsStartTime = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").format(LocalDateTime.now)
    sqlnewsStartTime = (sqlnewsStartTime+"Z")

    val newstobePublishedDF=newsDF.distinct().withColumn("rawzonetime",lit(sqlnewsStartTime).cast("timestamp"))

    val query = newstobePublishedDF
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch(saveToKCassandraDeltaSink _)
      .option("checkpointLocation",checkpointLocation)
      .outputMode("update")
      .start()

    query.awaitTermination()

  }

  def saveToKCassandraDeltaSink(newstobePublishedDF: DataFrame, batchId: Long) : Unit = {

    val newseventrawzoneDF = DeltaTable.forName(config.newseventRawzone).toDF
    val newseventfeedaggDF = DeltaTable.forName(config.newseventConsumerzone).toDF

    val newseventrawzone = DeltaTable.forName(config.newseventRawzone)
    val newseventfeedagg = DeltaTable.forName(config.newseventConsumerzone)

    val Targetaggtable=config.Targetaggtable
    val keySpace=config.Keyspace


    //merge into raw zone(new data)
    newseventrawzone.as("target").merge(newstobePublishedDF.as("source"),s"target.recid = source.recid")
      .whenMatched()
      .updateAll
      .whenNotMatched
      .insertAll
      .execute()

    var newstobeaggDF=newseventrawzoneDF.alias("final").join(newstobePublishedDF,Seq("articleid"),"inner")
      .groupBy(newseventrawzoneDF("articleid"),newseventrawzoneDF("headline"),newseventrawzoneDF("newscategory")
        ,newseventrawzoneDF("newssubcategory"),newseventrawzoneDF("newspublishedtime")).count()
      .withColumnRenamed("count","numofhits")

    newstobeaggDF.show()

    var consumerzoneingestTime = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").format(LocalDateTime.now)
    consumerzoneingestTime = (consumerzoneingestTime+"Z")

    newstobeaggDF=newstobeaggDF.withColumn("feedaggregatedtime",lit(consumerzoneingestTime).cast("timestamp")).withColumn("feedaggregatedhour",lit(1))

    newstobeaggDF.persist()
    //merge to consumer zone
    newseventfeedagg.as("target").merge(newstobeaggDF.as("source"),s"target.articleid = source.articleid")
      .whenMatched()
      .updateAll
      .whenNotMatched
      .insertAll
      .execute()

    //merge to cassandra

    //   println("update cassandra")
    newstobeaggDF.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> Targetaggtable, "keyspace" -> keySpace))
      .mode("append")
      .save()

    newstobeaggDF.unpersist()

  }



}
