package Broker

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.bson.Document
import scala.collection.JavaConverters._
import scala.collection.mutable

object KafkaSparkConsumer {

  // write records to MongoDB after every mongoBatch messages are consumed
  val mongoBatch = 10
  val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://xxxx:yyyy@192.168.21.5:27017/", "database" -> "zzzz", "collection" -> "epd01"))

  def runSparkConsumer(topic: String): Unit = {
    // create or get SparkSession
    val spark = SparkSession
      .builder
      .master("local[4]")
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
      // ForeachWriter prints consumed records to console and writes records to MongoDB
      var mongoConnector: MongoConnector = _
      var epdRecords: mutable.ArrayBuffer[SimulatedRecord] = _

      override def open(partitionId: Long, version: Long) = {
        // open connection to MongoDB
        mongoConnector = MongoConnector(writeConfig.asOptions)
        epdRecords = new mutable.ArrayBuffer[SimulatedRecord]()
        true
      }

      override def process(value: (String, String)) = {
        // print consumed records to console and save to MongoDB
        epdRecords.append(SimulatedRecord(value._1, value._2))
        if (epdRecords.size >= mongoBatch) {
          mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
            collection.insertMany(epdRecords.map(record => {
              Document.parse(record.value)
            }).asJava)
          })
          println(s"$mongoBatch records written to MongoDB...")
          epdRecords.clear()
        }
        println(s"Consumer record: key=${value._1}\n${value._2}")
      }

      override def close(errorOrNull: Throwable)= {
        // save all records to MongoDB (not previously saved)
        if (epdRecords.nonEmpty) {
          mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
            collection.insertMany(epdRecords.map(record => {
              Document.parse(record.value)
            }).asJava)
          })
          println(s"${epdRecords.size} records written to MongoDB -- Spark ForeachWriter is closed...")
          epdRecords.clear()
        }
      }
    }

    // With writer, verify consumer data is received and save data to MongoDB...
    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .writeStream
      .foreach(writer)
      .start()

    query.awaitTermination(10000)
    query.stop()
  }

  case class SimulatedRecord(key: String, value: String)
}

