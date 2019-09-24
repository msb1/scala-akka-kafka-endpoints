import Broker.{KafkaConsumer, KafkaProducer}
import Endpoint.DataRecord
import Endpoint.DataRecord.SimClass.StartSimulator
import Endpoint.DataRecord.Simulate
import akka.actor.{ActorSystem, Props}
import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.LoggerFactory

object Main {

  def main(args: Array[String]) {
    // set Logger levels to WARN (to avoid excess verbosity)
    LoggerFactory.getLogger("org").asInstanceOf[Logger].setLevel(Level.WARN)
    LoggerFactory.getLogger("akka").asInstanceOf[Logger].setLevel(Level.WARN)
    LoggerFactory.getLogger("kafka").asInstanceOf[Logger].setLevel(Level.WARN)

    val topic = "epd01"
    val topicSet = Set(topic)

    // Read in simulator configuration
    val filePath = "/home/barnwaldo/scala/Misc/kafkaTest/"
    println("Kafka Akka Test program...")
    val epdConfig = DataRecord.readConfigData(filePath + "epd.json")

    //Create actors for Kafka Producer, Consumer and EPD Simulator
    val system = ActorSystem("Kafka")
    println("Start Kafka Producer Stream")
    val kafkaProducer = system.actorOf(Props[KafkaProducer], "kafkaProducer")
    val simulate = system.actorOf(Props(new Simulate(kafkaProducer)), "simulator")
    simulate ! StartSimulator(20, topic, epdConfig)     // start simulator
    println("Start Kafka Consumer Stream")
    KafkaConsumer.runConsumer(topicSet)
  }
}
