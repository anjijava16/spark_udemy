import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer, OneHotEncoder}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.Pipeline

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()

val data = spark.read.option("inferSchema", "true").option("header", "true").format("csv").load("titanic.csv")

data.printSchema()

// print the columns
val cols = data.columns
for (i <- Range(0, cols.length)) {
  println(cols(i))
}

// select the columns we need
val logregdataall = data.select(data("Survived").as("label"), $"Pclass", $"Sex", $"Age", $"SibSp", $"Parch",
                                    $"Fare", $"Embarked")

// drop missing values
val logregdata = logregdataall.na.drop()

// convert strings into numeric values
val genderIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("SexIndex")
val embarkIndexer = new StringIndexer().setInputCol("Embarked").setOutputCol("EmbarkedIndex")

// convert numeric values into one-hot-encodings
val genderEncoder = new OneHotEncoder().setInputCol("SexIndex").setOutputCol("SexVec")
val embarkEncoder = new OneHotEncoder().setInputCol("EmbarkedIndex").setOutputCol("EmbarkedVec")

// assemble the features into a features columns
// (label, features)
val assember = (new VectorAssembler().setInputCols(Array("Pclass", "SexVec", "Age",
                            "SibSp", "Parch", "Fare", "EmbarkedVec")).setOutputCol("features"))

// split data into training and test data
// (training, test)
val Array(training,test) = logregdata.randomSplit(Array(0.7, 0.3), seed=12345)

// create the model
val lr = new LogisticRegression()

// define a pipeline containing all the stages, each stage is a transformation, final stage is model creation
val pipeline = new Pipeline().setStages(Array(genderIndexer, embarkIndexer, genderEncoder, embarkEncoder, assember, lr))

// create the model using the pipeline against training data
val model = pipeline.fit(training)

// execute the model against test data
val results = model.transform(test)

//////////////////////
// MODEL EVALUATION
// Evaluaton & Metrics
//////////////////////

import org.apache.spark.mllib.evaluation.MulticlassMetrics

val predictionAndLabels = results.select($"prediction", $"label").as[(Double, Double)].rdd

val metrics = new MulticlassMetrics(predictionAndLabels)

println("Confusion Matrix")
println(metrics.confusionMatrix)
