// DATAFRAME PROJECT
// Use the Netflix_2011_2016.csv file to Answer and complete
// the commented tasks below!

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Load the Netflix Stock CSV File, have Spark infer the data types.
val df = spark.read.option("header","true").option("inferSchema","true").csv("Netflix_2011_2016.csv")

// What are the column names?
df.columns
df.head(1)

// What does the Schema look like?
df.printSchema

// Print out the first 5 columns. I think he meant rows
df.show(5)
df.head(5)
// how do we select the first n columns?
df.select(year($"Date"), $"Open", $"High").show()

// Use describe() to learn about the DataFrame.
df.describe()

// Create a new dataframe with a column called HV Ratio that
// is the ratio of the High Price versus volume of stock traded
// for a day.
val df2 = df.withColumn("HV Ratio", df("High")/df("Volume"))
df2.show()

// What day had the Peak High in Price?
// df.select(df("Date"), max(df("High")))
// this selects the row with the max value of High; but does not show other columns
// how do we get other columns to display?
df.select(max(df("High"))).show()
// maxhigh type is Array[org.apache.spark.sql.Row]; how do we extract the Double from this?
val maxhigh_arr = df.select(max(df("High"))).collect()
// maxhigh_row is a row
val maxhigh_row = maxhigh_arr(0)
// how do we get the double out of the Row?
val maxhigh = maxhigh_row.getDouble(0)
// this creates a new column "(High = 716.159996)" and displays a boolean values
df.select($"Date", $"High"===maxhigh).show()
// need a way to select; "filter" does the right thing
df.filter($"High"===maxhigh).show()

// What is the mean of the Close column?
df.select(avg("Close")).show()

// What is the max and min of the Volume column?
df.select(max("Volume"), min("Volume")).show()

// For Scala/Spark $ Syntax

// How many days was the Close lower than $ 600?
val df3 = df.filter($"Close" < 600)
df3.count() // 1218
df.filter($"Close" > 600).count() // 41; total rows 1259, which is correct 1260 - 1 header
df.count()  // 1259

// What percentage of the time was the High greater than $500 ?
// this returns ZERO as it is Div; how do we get it to give us a Double?
val higherThanFiveHundred = (df.filter($"High" > 500).count())/(df.count())
// still 0, but of type Double; this does it 4.924543288324067
val higherThanFiveHundred = (df.filter($"High" > 500).count().toDouble)/(df.count())*100

// What is the Pearson correlation between High and Volume?
// -0.20960233287942157
df.select(corr("High", "Volume")).show()

// What is the max High per year?
// groupBy Year; then get Max High for each year
// this returns a org.apache.spark.sql.RelationalGroupedDataset
df.groupBy(year(df("Date")))
// this works too and adds another column
df.groupBy(year(df("Date")), max("High"))
// this one works and shows all columns
val df4 = df.groupBy(year(df("Date"))).max()
// select only "year(Date)" and "max(High)" columns
df4.select($"year(Date)", $"max(High)").show()


// What is the average Close for each Calender Month?
df.groupBy(month(df("Date"))).avg().select($"month(Date)", $"avg(Close)").show()
