import .Message
import Broker.KafkaSparkConsumer
import Endpoint.DataRecord

object Main {

  def main(args: Array[String]) {

    val topic = "epd01"

    val filePath = "/home/barnwaldo/scala/Misc/kafkaTest/"
    println("Spark Kafka Consumer Test program...")
    val epdConfig = DataRecord.readConfigData(filePath + "epd.json")


    KafkaSparkConsumer.runSparkConsumer(topic)

//    for (i <- 0 until 20) {
//      val key = i
//      val value = DataRecord.makeSimulatedRecord(epdConfig, topic)
//      val msgStr = topic + "," + key + "," + value
//      producerActorRef ! Message(msgStr)
//    }

  }
}
