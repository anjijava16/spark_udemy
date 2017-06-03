// choose the right evaluator -- if binary classificaiton, use BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{TrainValidationSplit, ParamGridBuilder}

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

val data = spark.read.option("inferSchema", "true").option("header", "true").format("csv").load("../Regression/Clean-USA-Housing.csv")

data.printSchema()

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector

val df = data.select(data("Price").as("label"), $"Avg Area Income", $"Avg Area House Age", $"Avg Area Number of Rooms", $"Avg Area Number of Bedrooms", $"Area Population")

val assembler = (new VectorAssembler()
                    .setInputCols(Array("Avg Area Income", "Avg Area House Age", "Avg Area Number of Rooms", "Avg Area Number of Bedrooms", "Area Population"))
                    .setOutputCol("features"))

val output = assembler.transform(df).select($"label", $"features")

// Training and test data
val Array(training,test) = output.randomSplit(Array(0.70, 0.30), seed=12345)

// model -- estimator
val lr = new LinearRegression()

// Parameter Grid builder
// lr.regParam is the regularization parameter; choose two values; model will be run with both options
// if we have multiple param grid values, the model will be run with each unique combination of param values
val paramGrid = new ParamGridBuilder().addGrid(lr.regParam, Array(10000, 0.0001)).build()

// evaluator
// can choose any metric suited for RegressionEvaluator
// model runs will be compared with each other using this Evaluator
val evaluator = new RegressionEvaluator().setMetricName("r2")

// Training data -- split out the validation data
val trainvalsplit = (new TrainValidationSplit()
                      .setEstimator(lr) // the model to use to estimate
                      .setEvaluator(evaluator)  // the evaluator to evaluate results
                      .setEstimatorParamMaps(paramGrid)        // parameters to use with the estimator model
                      .setTrainRatio(0.8)
                    )

val model = trainvalsplit.fit(training)

model.transform(test).select("features", "label", "prediction").show()

model.validationMetrics
