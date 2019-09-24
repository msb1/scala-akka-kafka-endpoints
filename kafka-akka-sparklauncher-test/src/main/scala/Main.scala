import Broker.SparkLaunch.StartMessage
import Broker.{KafkaProducer, SparkLaunch, Starter}
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
    val filePath = "/home/barnwaldo/scala/Misc/kafkaTest/"

    // Read in simulator configuration
    println("Kafka Akka Test program...")
    val epdConfig = DataRecord.readConfigData(filePath + "epd.json")

    //Create actors for Kafka Producer, EPD Simulator and SparkLauncher
    val system = ActorSystem("SparkLaunch")
    val kafkaProducer = system.actorOf(Props[KafkaProducer], "kafkaProducer")
    val simulate = system.actorOf(Props(new Simulate(kafkaProducer)), "simulator")
    val sparkLauncher = system.actorOf(Props[SparkLaunch], "sparkLauncher")
    val starter = system.actorOf(Props(new Starter(sparkLauncher)), "starter")
    simulate ! StartSimulator(20, topic, epdConfig)	// start simulator
    starter ! StartMessage				// start sparklauncher
  }
}

/** 
	-- Alternative (recommended) way to use SparkLauncher with CountDownLatch
	-- This approach does not use Akka Actors 
*/
// use countdown latch to hold program until spark app is finished
//    val countDownLatch = new CountDownLatch(1)
//
//    val launcher = new SparkLauncher()
//      .setSparkHome("/opt/spark")
//      // .setAppResource("/home/barnwaldo/scala/Misc/simple/build/libs/simple.jar")
//      // .setMainClass("SimpleApp")
//      .setConf("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.4")
//      .setAppResource("/home/barnwaldo/scala/Misc/KafkaSparkTest/build/libs/KafkaSparkTest.jar")
//      .setMainClass("Main")
//      .setMaster("local[*]")
//      .redirectToLog("console")
//      .startApplication(new SparkAppHandle.Listener() {
//        def infoChanged(handle: SparkAppHandle): Unit = {
//        }
//
//        def stateChanged(handle: SparkAppHandle): Unit = {
//          val appState = handle.getState
//          println(s"Spark App Id [${handle.getAppId}] State Changed. State [${handle.getState}]")
//          if (appState != null && appState.isFinal) {
//            //latch counts down when spark driver exits
//            println("App State isFinal...")
//            countDownLatch.countDown
//          }
//        }
//      })
//
//    // countdown latch keeps program alive until spark driver exits
//    countDownLatch.await()
