package Broker

import org.apache.spark.sql.{ForeachWriter, SparkSession}

object KafkaSparkConsumer {

  def runSparkConsumer(topic: String): Unit = {

    // create or get SparkSession
    val spark = SparkSession
      .builder
      .appName("KafkaStructuredStreamTester")
      .getOrCreate()
    import spark.implicits._

    // Subscribe to one topic with spark kafka connector
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.21.5:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val writer = new ForeachWriter[(String, String)] {
      override def open(partitionId: Long, version: Long) = true
      override def process( value :(String, String)) = println("key=" + value._1 + "\n" + value._2)
      override def close(errorOrNull: Throwable) = {}
    }

    // With writer, verify consumer data is received...
    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .writeStream
      .foreach(writer)
      .start()
    query.awaitTermination(10000)
    query.stop()
  }

}
