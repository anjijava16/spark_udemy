// Lecture 40 -- missing information in data file ContainsNull.csv

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Grab small dataset with some missing data
val df = spark.read.option("header","true").option("inferSchema","true").csv("ContainsNull.csv")

df.show()

// dropping values
// drop all rows where there are 'null' values
df.na.drop().show()
// drop only those rows where there are less than 2 non-null values;
// if a row has 0 or 1 values that is not null, drop it; else keep it
df.na.drop(2).show()

// filling values
// infers type and fills all matching type cells that are null
df.na.fill(100).show()
// fill a strings
df.na.fill("missing name").show()
// specify which column to fill
df.na.fill("missing name", Array("Name")).show()
df.na.fill(100, Array("Sales")).show()
// combine the two together; df2 should have all missing values filled
val df2 = df.na.fill("missing name", Array("Name"))
df2.na.fill(100, Array("Sales")).show()
