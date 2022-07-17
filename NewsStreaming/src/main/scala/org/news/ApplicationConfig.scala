package org.news

case class ApplicationConfig(
                              AppName: String
                              , applicationDisplayName: String = "CreditSafe Streaming Application"
                              , sparkMaster: String = sys.env.getOrElse("SPARK_MASTER", "local[*]")
                              , streamSource: String = "KAFKA"
                              , streamSink: String = "CASSANDRA"
                              , streamFromTableVersion: String = "0"
                              , streamFailOnDataLoss: String = "false"
                              , kafkaConfigId: String = "1"
                              , cassandrahost: String = "cassandra-node1.midevcld.spglobal.com,cassandra-node2.midevcld.spglobal.com,cassandra-node3.midevcld.spglobal.com,cassandra-node4.midevcld.spglobal.com,cassandra-node5.midevcld.spglobal.com,cassandra-node6.midevcld.spglobal.com"
                              , subscribertopic: String = "news-streaming"
                              , kafkabroker: String = "avsoakfkd-1.dev.mktint.global:9092,avsoakfkd-2.dev.mktint.global:9092,avsoakfkd-3.dev.mktint.global:9092,avsoakfkd-4.dev.mktint.global:9092,avsoakfkd-5.dev.mktint.global:9092"
                              , checkpointlocation: String = "s3://spgmi-use1-databricks-dev-sme/checkpoint/kafka_to_delta_kafka_news_1234/"
                              , cassandrauser: String = "vivek_s"
                              , cassandrapwd: String = "VS_MiKeyDev"
                              , Keyspace: String = "keyspace1"
                              , Targetaggtable: String = "newsfeedaggconsumerzone_tbl"
                              , eventAction: Int = 1
                              , debug: Boolean = false
                              ,inputkafkaStartingOffsets:String ="latest"
                              ,newseventRawzone:String="sme.newseventrawzonedata_tbl"
                              ,newseventConsumerzone :String="sme.newsfeedaggconsumerzone_tbl"
                              ,maxBytesPerTrigger:String="200m"

                            )


