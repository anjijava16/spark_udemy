import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")
// df.printSchema()

import spark.implicits._

// scala notation with $
df.filter($"Close">480 && $"High">$"Close").show()
// SQL-like notation
// df.filter("Close > 480 AND High > 480").show()

// instead of showing, it is useful to collect these results into a Scala Array
val CH_low1 = df.filter($"Close">480 && $"High">$"Close").collect()

// getting the count
val CH_low2 = df.filter($"Close">480 && $"High"<$"Close").count()

// need triple equal to do an exact match; not double ==
df.filter($"Close"===486.20).show()

// find correlation between two columns; select takes a function
// 'corr' calculates Pearson correlation coefficient between two values
df.select(corr("High", "Low")).show()
