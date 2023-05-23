from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("merge_csv").getOrCreate()

df = spark.read.format("csv").option("header","true").load("/user/mocha/data/by-day/*.csv")

df.write.format("csv").option("header", "true").mode("overwrite").save("/user/mocha/data/merged_retail.csv")

df.show()
spark.stop()
