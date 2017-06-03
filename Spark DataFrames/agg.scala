// GROUP BY and AGG (Aggregate methods)

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset
val df = spark.read.option("header","true").option("inferSchema","true").csv("Sales.csv")

// GroupBy groups the rows by a given column and then aggregates the values across the grouped rows
df.groupBy("Company").count().show()
df.groupBy("Company").min().show()
df.groupBy("Company").max().show()
df.groupBy("Company").sum().show()

// Aggregate functions aggregate across all values of a Columns
df.select(sum("Sales")).show()
df.select(countDistinct("Sales")).show()
df.select(sumDistinct("Sales")).show()

df.select(avg("Sales")).show()
df.select(variance("Sales")).show()
df.select(stddev("Sales")).show()

df.select(collect_set("Sales")).show()

df.orderBy("Sales").show()
df.orderBy($"Sales".desc).show()
