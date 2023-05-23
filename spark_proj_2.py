from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("query_retail").getOrCreate()

df = spark.read.csv("/user/mocha/data/merged_retail.csv", header=True, inferSchema=True)

df.createOrReplaceTempView("retail")

query = """
  WITH cte AS (
        SELECT CustomerID, 
              ROUND(SUM(Quantity * UnitPrice),2) AS total_sales,
               RANK() OVER (ORDER BY SUM(Quantity * UnitPrice) DESC) AS rank_num
        FROM retail
	WHERE CustomerID is not null 
        GROUP BY CustomerID
    )
    SELECT CustomerID, total_sales, rank_num
    FROM cte
    WHERE rank_num <= 5
"""

result = spark.sql(query)

result.show()
spark.stop()
