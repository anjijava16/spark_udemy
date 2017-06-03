import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression

import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()

val data = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("Clean-USA-Housing.csv")

data.printSchema

val colnames = data.columns
// head(1) returns an array with one element; (0) extracts that element and returns a value of type
// org.apache.spark.sql.Row
val firstrow = data.head(1)(0)
println("\n")
println("Example Data Row")

for (ind <- Range(1, colnames.length)) {
  // print name of column header
  println(colnames(ind))
  // print value of that column from first row
  println(firstrow(ind))
  println("\n")
}

// Labels, features
import org.apache.spark.ml.feature.VectorAssembler
// Factory methods for org.apache.spark.ml.linalg.Vector.
import org.apache.spark.ml.linalg.Vectors

// data("Price"): org.apache.spark.sql.Column
// .as("label") is a method call on Column; returns a Column names "Price AS label"
// $"Avg Area Income" makes it a org.apache.spark.sql.ColumnName
val df = (data.select(data("Price").as("label"), $"Avg Area Income", $"Avg Area House Age",
                    $"Avg Area Number of Rooms", $"Avg Area Number of Bedrooms", $"Area Population"))

// assemble input columns into a single output column that contains a vector of input columns
val assembler = new VectorAssembler().setInputCols(Array("Avg Area Income", "Avg Area House Age",
                  "Avg Area Number of Rooms", "Avg Area Number of Bedrooms", "Area Population")).setOutputCol("features")


val output = assembler.transform(df).select($"label", $"features")

val lr = new LinearRegression()

val lrModel = lr.fit(output)

val trainingSummary = lrModel.summary

// the training summary object can be useful to get info about the model
trainingSummary.residuals.show()

trainingSummary.predictions.show()

println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"MSE: ${trainingSummary.meanSquaredError}")
println(s"r2: ${trainingSummary.r2}")
