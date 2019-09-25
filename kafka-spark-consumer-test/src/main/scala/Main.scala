import Broker.KafkaSparkConsumer

object Main {

  def main(args: Array[String]) {

    val topic = "epd01"

    val filePath = "/home/barnwaldo/scala/Misc/kafkaTest/"
    println("Spark Kafka Consumer Test program...")

    KafkaSparkConsumer.runSparkConsumer(topic)

  }
}
