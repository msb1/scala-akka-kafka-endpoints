import Broker.{KafkaSparkConsumer, SparkLRConsumer}

object Main {

  val modelFlag:Boolean = true

  def main(args: Array[String]) {

    val topic = "epd01"
    println("Spark Kafka Consumer Test program...")

    // Spark SQL Stream with embedded Logistic Regression - data records from Kafka Consumer
    if(modelFlag) {
      println("Logistic Regression enabled...")
      SparkLRConsumer.runSparkConsumer(topic)
    }
    // Spark SQL Stream with data record sink to MongoDB
    else {
      println("Sink Data records to MongoDB enabled...")
      KafkaSparkConsumer.runSparkConsumer(topic)
    }
  }
}
