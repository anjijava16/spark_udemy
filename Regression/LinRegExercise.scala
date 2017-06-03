////////////////////////////////////////////
//// LINEAR REGRESSION EXERCISE ///////////
/// Complete the commented tasks below ///
/////////////////////////////////////////

// Import LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression

// Optional: Use the following code below to set the Error reporting
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)


// Start a simple Spark Session
val spark = SparkSession.builder().getOrCreate()


// Use Spark to read in the Ecommerce Customers csv file.
val custdf = spark.read.option("header", "true").option("inferSchema", "true").csv("Clean-Ecommerce.csv")

// Print the Schema of the DataFrame
custdf.printSchema

// Print out an example Row
// Various ways to do this, just
// choose whichever way you prefer
val fifthrow = custdf.head(10)(4)
println(fifthrow)

val cols = custdf.columns

for (i <- Range(0, cols.length)) {
  println(cols(i))
  println(fifthrow(i))
}

////////////////////////////////////////////////////
//// Setting Up DataFrame for Machine Learning ////
//////////////////////////////////////////////////

// A few things we need to do before Spark can accept the data!
// It needs to be in the form of two columns
// ("label","features")

// Import VectorAssembler and Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

// Rename the Yearly Amount Spent Column as "label"
// Also grab only the numerical columns from the data
// Set all of this as a new dataframe called df
val df = custdf.select(custdf("Yearly Amount Spent").as("label"), $"Avg Session Length", $"Time on App",
                              $"Time on Website", $"Length of Membership")

// An assembler converts the input values to a vector
// A vector is what the ML algorithm reads to train a model

// Use VectorAssembler to convert the input columns of df
// to a single output column of an array called "features"
// Set the input columns from which we are supposed to read the values.
// Call this new object assembler
val assember = new VectorAssembler().setInputCols(Array("Avg Session Length",
                    "Time on App", "Time on Website", "Length of Membership")).setOutputCol("features")

// Use the assembler to transform our DataFrame to the two columns: label and features
val trainingdf = assember.transform(df).select($"label", $"features")

// Create a Linear Regression Model object
val lr = new LinearRegression()

// Fit the model to the data and call this model lrModel
val lrmodel = lr.fit(trainingdf)

// Print the coefficients and intercept for linear regression
val coeff = lrmodel.coefficients
val intercept = lrmodel.intercept
println(s"Coefficients = ${coeff}, Intercept = ${intercept}")

// Summarize the model over the training set and print out some metrics!
// Use the .summary method off your model to create an object
// called trainingSummary
val summary = lrmodel.summary

// Show the residuals, the RMSE, the MSE, and the R^2 Values.
println(s"residuals = ${summary.residuals}")
println(s"RMSE = ${summary.rootMeanSquaredError}")
println(s"MSE = ${summary.meanSquaredError}")
println(s"r^2 = ${summary.r2}")


// Great Job!
