# Databricks notebook source
# This is a .py file.

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,Create Data (list → DataFrame)
data = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Chet", 28)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic Transformations

# COMMAND ----------

# DBTITLE 1,Filter
df.filter(df.age > 26).show()

# COMMAND ----------

# DBTITLE 1,Select specific columns
df.select("name", "age").show()

# COMMAND ----------

# DBTITLE 1,Add new column
from pyspark.sql.functions import col

df = df.withColumn("age_plus_5", col("age") + 5)
df.show()

# COMMAND ----------

# DBTITLE 1,Aggregation
df.groupBy().avg("age").show()

# COMMAND ----------

# DBTITLE 1,Sorting
df.orderBy("age").show()
