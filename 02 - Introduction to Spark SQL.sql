-- Databricks notebook source
-- MAGIC %md
-- MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Disclaimer
-- MAGIC
-- MAGIC For this learning sample, static synthetic data samples are used as an input

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Spark SQL
-- MAGIC
-- MAGIC Demonstrate fundamental concepts in Spark SQL
-- MAGIC
-- MAGIC ##### Objectives
-- MAGIC 1. Run a SQL query
-- MAGIC 1. Create a DataFrame
-- MAGIC 1. Convert between DataFrames and SQL
-- MAGIC 1. Write DataFrame to tables
-- MAGIC
-- MAGIC ##### Methods
-- MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>
-- MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html" target="_blank">DataFrameReader</a>
-- MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html" target="_blank">DataFrameWriter</a>

-- COMMAND ----------

-- MAGIC %md ## Multiple Interfaces
-- MAGIC Spark SQL is a module for structured data processing with multiple interfaces.
-- MAGIC
-- MAGIC We can interact with Spark SQL in two ways:
-- MAGIC 1. Using SQL cells
-- MAGIC 1. Run SQL queries using Python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Method 1: Executing queries in SQL cells**

-- COMMAND ----------

SELECT REPORT_DATE, COUNT(DISTINCT customer_id) AS DISTINCT_CUSTOMER_COUNT
FROM jay_mehta_catalog.sas.household_activity
GROUP BY REPORT_DATE
ORDER BY REPORT_DATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC SQL is ANSI complaint and built-in functions are available to use
-- MAGIC
-- MAGIC <a href="https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-functions-builtin" target="_blank">SQL Built-in Functions Reference Doc</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Case When Queries**

-- COMMAND ----------

SELECT
  activity_date,
  activity,
  CASE
    WHEN activity = "CANC" THEN 1
    WHEN activity = "NAC" THEN 2
    ELSE 3
  END AS activity_bucket,
  enterprise_id,
  customer_account,
  customer_id
FROM
  jay_mehta_catalog.sas.subscriber_activity

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Window Function Queries and Subqueries**

-- COMMAND ----------

SELECT
  *
FROM
  (
    SELECT
      activity_date,
      activity,
      customer_id,
      RANK() OVER (
        PARTITION BY activity_date,
        activity
        ORDER BY
          customer_id DESC
      ) as rnk
    FROM
      jay_mehta_catalog.sas.subscriber_activity
  )
WHERE
  rnk < 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Common Table Expressions (CTE)**

-- COMMAND ----------

WITH distinct_activity_cte AS (
  SELECT
    DISTINCT activity
  FROM
    jay_mehta_catalog.sas.subscriber_activity
)
SELECT
  *
FROM
  distinct_activity_cte

-- COMMAND ----------

-- MAGIC %md **Method 2: Working with the DataFrame API**
-- MAGIC
-- MAGIC We can also express Spark SQL queries using the DataFrame API.
-- MAGIC The following cell returns a DataFrame containing the same results as those retrieved above.

-- COMMAND ----------

-- MAGIC %md ## DataFrames
-- MAGIC
-- MAGIC A **DataFrame** is a distributed collection of data grouped into named columns.

-- COMMAND ----------

-- MAGIC %md ##Why use DataFrames?
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create a dataframe for the entire table or with a subset of columns, filtered records

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC result_df = spark.sql("""
-- MAGIC SELECT * FROM jay_mehta_catalog.sas.household_activity
-- MAGIC """)
-- MAGIC
-- MAGIC display(result_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC result_df = spark.sql("""
-- MAGIC SELECT REPORT_DATE, COUNT(DISTINCT customer_id) AS DISTINCT_CUSTOMER_COUNT
-- MAGIC FROM jay_mehta_catalog.sas.household_activity
-- MAGIC GROUP BY REPORT_DATE
-- MAGIC ORDER BY REPORT_DATE
-- MAGIC """)
-- MAGIC
-- MAGIC display(result_df)

-- COMMAND ----------

-- MAGIC %md The **schema** defines the column names and types of a dataframe.
-- MAGIC
-- MAGIC Access a dataframe's schema using the **`schema`** attribute.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC result_df.printSchema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **`count`** returns the number of records in a DataFrame.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC result_df.count()

-- COMMAND ----------

-- MAGIC %md ## Query Execution
-- MAGIC We can express the same query using any interface. The Spark SQL engine generates the same query plan used to optimize and execute on our Spark cluster.
-- MAGIC
-- MAGIC ![query execution engine](https://files.training.databricks.com/images/aspwd/spark_sql_query_execution_engine.png)

-- COMMAND ----------

-- MAGIC %md ## Spark API Documentation
-- MAGIC
-- MAGIC To learn how we work with DataFrames in Spark SQL, let's first look at the Spark API documentation.
-- MAGIC The main Spark <a href="https://spark.apache.org/docs/latest/" target="_blank">documentation</a> page includes links to API docs and helpful guides for each version of Spark.

-- COMMAND ----------

-- MAGIC %md ### Creating dataframe using `createDataFrame` method manually

-- COMMAND ----------

-- MAGIC %python
-- MAGIC cols = ["Emp_ID", "Name", "Department", "Salary"]
-- MAGIC data = [[1, "Tom", "IT", 50000], [2, "Megan", "Finance", 60000], [3, "Petra", "Sales", 100000], [4, "Max", "HR", 40000]]
-- MAGIC
-- MAGIC sample_df = (spark
-- MAGIC            .createDataFrame(data=data, schema=cols)
-- MAGIC           )
-- MAGIC
-- MAGIC sample_df.printSchema()
-- MAGIC display(sample_df)

-- COMMAND ----------

-- MAGIC %md ## Convert between DataFrames and SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **`createOrReplaceTempView`** creates a temporary view based on the DataFrame. The lifetime of the temporary view is tied to the SparkSession that was used to create the DataFrame.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sample_df.createOrReplaceTempView("vw_emp")

-- COMMAND ----------

SELECT * FROM vw_emp

-- COMMAND ----------

-- MAGIC %md ## DataFrameWriter
-- MAGIC Interface used to write a DataFrame to external storage systems
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC (df  
-- MAGIC &nbsp;  .write                         
-- MAGIC &nbsp;  .option("compression", "snappy")  
-- MAGIC &nbsp;  .mode("overwrite")      
-- MAGIC &nbsp;  .parquet(output_dir)       
-- MAGIC )
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC DataFrameWriter is accessible through the SparkSession attribute **`write`**. This class includes methods to write DataFrames to different external storage systems.

-- COMMAND ----------

-- MAGIC %md ### Write DataFrames to tables
-- MAGIC
-- MAGIC Write **`events_df`** to a table using the DataFrameWriter method **`saveAsTable`**
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> This creates a global table, unlike the local view created by the DataFrame method **`createOrReplaceTempView`**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sample_df.write.mode("overwrite").saveAsTable("jay_mehta_catalog.sas.employees")

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>