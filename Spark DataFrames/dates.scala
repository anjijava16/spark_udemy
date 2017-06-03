// Dates and TimeStamps

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

df.printSchema()

// transform the Date column to just Month; and select it; returns col name month(Date) or year(Date)
df.select(month(df("Date"))).show()
df.select(year(df("Date"))).show()

// create a new Year column using the Date column; this just adds a new column Year with same value as Date
// df.withColumn("Year", df("Date")).show()
// transform the new Year column to have only the Year value, not full Date; df2 has all columns plus Year
val df2 = df.withColumn("Year", year(df("Date")))
// use the new "Year" column to groupBy; calculates mean where applicable
val dfavgs = df2.groupBy("Year").mean()
// select Year, Close columns; note the name of the Close column is "avg(Close)" due to the mean function
dfavgs.select($"Year", $"avg(Close)").show()
