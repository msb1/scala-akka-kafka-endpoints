package Broker

import Broker.SparkLaunch.{Finished, StartMessage}
import akka.actor.{Actor, ActorRef}
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

// Actor to Launch SparkKafka (Consumer) Connector to receive data from EPD Simulator
class SparkLaunch extends Actor {
  import SparkLaunch._

  def receive: Receive = {
    case Launch() => {
      val launcher = new SparkLauncher()
        .setSparkHome("/opt/spark")
        .setConf("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.4")
        .setAppResource("/home/barnwaldo/scala/Misc/KafkaSparkTest/build/libs/KafkaSparkTest.jar")
        .setMainClass("Main")
        .setMaster("local[*]")
        .redirectToLog("console")

      val listener = new SparkAppHandle.Listener {
        override def infoChanged(handle: SparkAppHandle): Unit = {}
        override def stateChanged(handle: SparkAppHandle): Unit = self ! StateChanged
      }

      val handle = launcher.startApplication(listener)
      context become launched(handle, sender)
    }
  }

  def launched(handle: SparkAppHandle, origSender: ActorRef): Receive = {
    case StateChanged => {
      println(s"Spark App Id [${handle.getAppId}] State Changed. State [${handle.getState}]")
      if (handle.getState.isFinal) {
        origSender ! Finished(handle.getState)
        println(s"Spark App Id [${handle.getAppId}] state isFinal... Context stopped...")
        context stop self
      }
    }
  }
}

class Starter(sparkLauncher: ActorRef) extends Actor {
  def receive: Receive = {
    case StartMessage => {
      sparkLauncher ! SparkLaunch.Launch()
    }
    case Finished(state: SparkAppHandle.State) => {
      println(s"Finished message received from SparkLauncher: $state")
    }
    case _ => println("Starter received something unexpected...")
  }
}

object SparkLaunch {
  case class Launch()
  case object StateChanged
  case object StartMessage
  case class Finished(state: SparkAppHandle.State)
}
