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
<li>Flag in main determines whether Kafka consumed data records are written to MongoDB or streamed through trained Logistic Regression Spark MLib model
<ol>
<li>Consumer stream includes ForeachWriter that implements Spark MongoConnector to log each incoming datarecord to MongoDB in addition to other processing - log to file or print to console</li>
<li>Consumer stream includes capability to process SQL Spark Structured Stream DataFrame through trained MLib Logistic Regression Model
</ol>
</ul>
</br>
<h4><strong>kafka-akka-sparklauncher-test</strong></h4>
<ul>
<li>Scala Spark implementation of SparkLauncher for kafka-spark-consumer</li>
<li>App includes Scala Akka Kafka Producer and EPD simulator to generate data records</li>
</ul>
</br>
<h4><strong>spark-sql-logistic-regression-trainer</strong></h4>
<ul>
<li>Scala Spark MLib train Logistic Regression model</li>
<li>Read dataset from Spark MongoConnector and then train/test Logistic Regression model</li>
</ul>
</br>
