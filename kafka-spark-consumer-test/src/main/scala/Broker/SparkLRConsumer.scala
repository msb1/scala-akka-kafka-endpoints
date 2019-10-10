package Broker

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

import scala.collection.immutable.HashMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object SparkLRConsumer {

  val lrBatch = 10

  // case class for simulated data
  case class EpdData(CurrentTime: String, Topic: String, Categories: HashMap[String, Int], Sensors: HashMap[String, Double], Result: Int)

  // Spark SQL struct for simulated data
  val epdSchema = new StructType()
    .add("CurrentTime", StringType)
    .add("Topic", StringType)
    .add("Result", IntegerType)
    .add("Categories",
      new StructType()
        .add("cat0", IntegerType)
        .add("cat1", IntegerType)
        .add("cat2", IntegerType)
        .add("cat3", IntegerType)
        .add("cat4", IntegerType)
        .add("cat5", IntegerType)
        .add("cat6", IntegerType)
        .add("cat7", IntegerType)
    )
    .add("Sensors",
      new StructType()
        .add("sensor0", DoubleType)
        .add("sensor1", DoubleType)
        .add("sensor2", DoubleType)
        .add("sensor3", DoubleType)
        .add("sensor4", DoubleType)
        .add("sensor5", DoubleType)
        .add("sensor6", DoubleType)
        .add("sensor7", DoubleType)
        .add("sensor8", DoubleType)
        .add("sensor9", DoubleType)
    )

  // columns that need to added to feature column
  val cols = Array("cat0", "cat1", "cat2", "cat3", "cat4", "cat5", "cat6", "cat7",
    "sensor0", "sensor1", "sensor2", "sensor3", "sensor4", "sensor5", "sensor6", "sensor7", "sensor8", "sensor9")

  def runSparkConsumer(topic: String): Unit = {
    // create or get SparkSession
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("SparkLRConsumer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // Subscribe to one topic with spark kafka connector
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.21.5:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val writer = new ForeachWriter[Row] {

      override def open(partitionId: Long, version: Long) = {
        true
      }

      override def process(row: Row) = {
        println(s"Time: ${row.getAs("CurrentTime")}, Features: ${row.getAs("features")}, " +
          s"Actual: ${row.getAs("label")}, Predicted: ${row.getAs("prediction")}  ")
      }

      override def close(errorOrNull: Throwable) = {
        println("CLOSING FOREACHWRITER...")
      }
    }

    // reload model
    val lrModel = LogisticRegressionModel
      .load("/home/bw/scala/Spark/SparkEpdTrainer/lr-test-model")

    println("-- WriteStream --")

    // With writer, verify consumer data is received and save data to MongoDB...
    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select($"key", from_json($"value", epdSchema).as("data"))
      .selectExpr("data.CurrentTime", "data.Topic", "data.Categories", "data.Sensors", "data.Result")

    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("features")

    val input = assembler
      .transform(query.select($"CurrentTime", $"Result", $"Categories.*", $"Sensors.*"))
      .drop(cols: _*).withColumnRenamed("Result", "label")

    // use saved LR model to predict resutls
    val output = lrModel.transform(input)
      .writeStream
      .foreach(writer)
      //.format("console")
      //.option("truncate", "false")
      .start()

    output.awaitTermination(10000)
    output.stop()
  }

}
