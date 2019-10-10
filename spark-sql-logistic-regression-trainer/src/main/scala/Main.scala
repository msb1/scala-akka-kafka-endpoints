import com.mongodb.spark._
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]) {
    // create or get SparkSession
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("SparkEpdTrainer")
      .config("spark.mongodb.input.uri", "mongodb://barnwaldo:shakeydog@192.168.21.5:27017/barnwaldo.epd01?authSource=admin")
      .config("spark.mongodb.output.uri", "mongodb://barnwaldo:shakeydog@192.168.21.5:27017/barnwaldo.epd01?authSource=admin")
      .getOrCreate()
    import spark.implicits._

    // load dataframe from MongoDB - simulated EPD data
    val df = MongoSpark.load(spark)

    // columns that need to added to feature column
    val cols =  Array("cat0", "cat1", "cat2", "cat3", "cat4", "cat5", "cat6", "cat7",
                     "sensor0", "sensor1", "sensor2", "sensor3", "sensor4", "sensor5", "sensor6", "sensor7", "sensor8", "sensor9")

    // VectorAssembler to add feature column
    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("features")

    // transform dataframe to prepare for Logistic Regression Test/Train
    val input = assembler
              .transform(df.select($"CurrentTime", $"Result", $"Categories.*", $"Sensors.*"))
              .drop(cols: _*).withColumnRenamed("Result","label")

    input.printSchema()
    input.show(10, false)

    // split data set training and test
    val seed = 42
    val Array(train, test) = input.randomSplit(Array(0.8, 0.2), seed)

    // train logistic regression model with training data set
    val logisticRegression = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.02)
      .setElasticNetParam(0.8)

    val logisticRegressionModel = logisticRegression.fit(train)

    // run model with test data set to get predictions
    val prediction = logisticRegressionModel.transform(test)
    prediction.show(10)

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")

    // measure the accuracy
    val accuracy = evaluator.evaluate(prediction)
    println(f"ROC acc: $accuracy%1.3f")

    // save model
    logisticRegressionModel.write.overwrite()
      .save("/home/bw/scala/Spark/SparkEpdTrainer/lr-test-model")

    // reload model
    val lrModelReload = LogisticRegressionModel
      .load("/home/bw/scala/Spark/SparkEpdTrainer/lr-test-model")

    // test reloaded model with test data set
    val reloadPrediction = lrModelReload.transform(test)
    reloadPrediction.show(10)
    val reloadAccuracy = evaluator.evaluate(prediction)
    println(f"ROC acc: $reloadAccuracy%1.3f")
  }
}
