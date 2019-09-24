package Broker

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.alpakka.mongodb.scaladsl.MongoSink
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape}
import com.mongodb.reactivestreams.client.MongoClients
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.mongodb.scala.bson.collection.immutable.Document

object KafkaConsumer {

  // define ActorSystem and Materializer for akka streams
  implicit val system = ActorSystem("Kafka")
  implicit val materializer = ActorMaterializer()

  // config for akka Kafka Consumer
  val conConfig = system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings: ConsumerSettings[String, String] = ConsumerSettings(conConfig, new StringDeserializer, new StringDeserializer)
    .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")

  // config for akka reactive streams MongoDB Client
  private val client = MongoClients.create("mongodb://xxxxxxxx:yyyyyyyy@localhost:27017/?authSource=zzzzz")
  private val db = client.getDatabase("barnwaldo")
  private val dbCollection = db.getCollection("epd01", classOf[Document])

  // implement Runnable Graph to split Kafka Consumer Source to Print Console, MongoDB, etc...
  def runConsumer(topicSet: Set[String]): Unit = {

    val consumerGraph = GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val controlSource = Consumer.plainSource(consumerSettings, Subscriptions.topics(topicSet))
      val printSink = Sink.foreach[ConsumerRecord[String, String]]((record) => toConsole(record))
      val recordConvert = Flow[ConsumerRecord[String, String]].map((record) => Document.apply(record.value()))
      val mongoSink = MongoSink.insertOne(dbCollection)

      val broadcaster = builder.add(Broadcast[ConsumerRecord[String, String]](2))
      controlSource ~> broadcaster.in
      broadcaster.out(0) ~> printSink
      broadcaster.out(1) ~> recordConvert ~> mongoSink
      ClosedShape
    }
    RunnableGraph.fromGraph(consumerGraph).run
  }

  def toConsole(record: ConsumerRecord[String, String]): Unit = {
    val topic = record.topic()
    val partition = record.partition()
    val offset = record.offset()
    val key = record.key()
    val value = record.value()
    println(s"Consumer: topic = $topic, partition = $partition, offset = $offset, key = $key \n$value")
  }

}

// implementation of Consumer Plain Source
//  def runConsumer(topicSet: Set[String]): DrainingControl[Done] = {
//    // retrieve the control object
//    val control = Consumer.plainSource(consumerSettings, Subscriptions.topics(topicSet))
//      .toMat(Sink.foreach((record) => toConsole(record)
//      ))(Keep.both)
//      .mapMaterializedValue(DrainingControl.apply)
//      .run()
//    control
//  }

