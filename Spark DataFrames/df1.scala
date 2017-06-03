import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header", "true").option("inferSchema", "true").csv("CitiGroup2006_2008")

// df.head(5)

for (row <- df.head(5)) {
  println(row)
}

df.columns

df.describe().show()

df.select("Volume").show()

//why use $ to prefix the string?
df.select($"Date", $"Close").show()

// add a new column; make a new dataframe
val df2 = df.withColumn("HighPlusLow", df("High")+df("Low"))
df2.printSchema()

// rename columns
// this returns a column, not data
val hpl = df2("HighPlusLow").as("HPL")
//now select using this column
df2.select(hpl).show()
// select multiple columns
df2.select(hpl, $"Close").show()
