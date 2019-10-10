<h4><strong>kafka-akka-producer-consumer-test</strong></h4>
<ul>
<li>Scala Akka (Alpakka) implementation of Kafka Producer/Consumer</li>
<li>Akka reactive streams uses sources, sinks and graph</li>
<li>Kafka Consumer implemented in graph with two parallel sinks using broadcast - PrintSink (to Console) and MongoSink (to MongoDB)</li>
</ul>
</br>
<h4><strong>kafka-spark-consumer-test</strong></h4>
<ul>
<li>Scala Spark Kafka consumer implementation with Spark Structured Streaming</li>
<li>Consumer stream implements ForeachWriter that implements Spark MongoConnector to log each incoming datarecord to MongoDB in addition to other processing</li>
</ul>
</br>
<h4><strong>kafka-akka-sparklauncher-test</strong></h4>
<ul>
<li>Scala Spark implementation of SparkLauncher for kafka-spark-consumer</li>
<li>App includes Scala Akka Kafka Producer and EPD simulator to generate data records</li>
</ul>
</br>
