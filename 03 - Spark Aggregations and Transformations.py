# Databricks notebook source
# MAGIC %md
# MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Disclaimer
# MAGIC
# MAGIC For this learning sample, static synthetic data samples are used as an input

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Things to consider
# MAGIC
# MAGIC * There are two interfaces
# MAGIC   1. Using SQL cells
# MAGIC   2. SQL queries using Python methods
# MAGIC * There is no difference in performance
# MAGIC * Code with Python methods sometimes becomes shorter, especially if you are working with tables with many columns

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br><br>
# MAGIC
# MAGIC * Data aggregation
# MAGIC * Data filtering
# MAGIC * Data cleansing
# MAGIC * Joins and Union

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregation
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Group data by specified columns
# MAGIC 1. Apply grouped data methods to aggregate data
# MAGIC 1. Apply built-in functions to aggregate data
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`groupBy`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank" target="_blank">Grouped Data</a>: **`agg`**, **`avg`**, **`count`**, **`max`**, **`sum`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-In Functions</a>: **`approx_count_distinct`**, **`avg`**, **`sum`**

# COMMAND ----------

# MAGIC %md ### Grouped data methods
# MAGIC Various aggregation methods are available on the <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank">GroupedData</a> object.
# MAGIC
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | agg | Compute aggregates by specifying a series of aggregate columns |
# MAGIC | avg | Compute the mean value for each numeric columns for each group |
# MAGIC | count | Count the number of rows for each group |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | min | Compute the min value for each numeric column for each group |
# MAGIC | pivot | Pivots a column of the current DataFrame and performs the specified aggregation |
# MAGIC | sum | Compute the sum for each numeric columns for each group |

# COMMAND ----------

cols = ["Emp_ID", "Name", "Department", "Salary"]
data = [[1, "Tom", "IT", 50000], [2, "Megan", "Finance", 60000], [3, "Petra", "Sales", 100000], [4, "Max", "HR", 40000], [5, "Prasad", "Finance", 50000], [6, "Terry", "Sales", 90000], [7, "Alex", "HR", 90000]]

sample_df = (spark
           .createDataFrame(data=data, schema=cols)
          )

sample_df.printSchema()
display(sample_df)

# COMMAND ----------

sum_salary_df = sample_df.groupBy("Department").sum("Salary")
display(sum_salary_df)

# COMMAND ----------

# MAGIC %md ## Built-In Functions
# MAGIC In addition to DataFrame and Column transformation methods, there are a ton of helpful functions in Spark's built-in <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html" target="_blank">SQL functions</a> module.
# MAGIC
# MAGIC In Scala, this is <a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">**`org.apache.spark.sql.functions`**</a>, and <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#functions" target="_blank">**`pyspark.sql.functions`**</a> in Python. Functions from this module must be imported into your code.

# COMMAND ----------

# MAGIC %md ### Aggregate Functions
# MAGIC
# MAGIC Here are some of the built-in functions available for aggregation.
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | approx_count_distinct | Returns the approximate number of distinct items in a group |
# MAGIC | avg | Returns the average of the values in a group |
# MAGIC | collect_list | Returns a list of objects with duplicates |
# MAGIC | corr | Returns the Pearson Correlation Coefficient for two columns |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | stddev_samp | Returns the sample standard deviation of the expression in a group |
# MAGIC | sumDistinct | Returns the sum of distinct values in the expression |
# MAGIC | var_pop | Returns the population variance of the values in a group |
# MAGIC
# MAGIC Use the grouped data method <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html#pyspark.sql.GroupedData.agg" target="_blank">**`agg`**</a> to apply built-in aggregate functions
# MAGIC
# MAGIC This allows you to apply other transformations on the resulting columns, such as <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html" target="_blank">**`alias`**</a>.

# COMMAND ----------

display(sample_df)

# COMMAND ----------

from pyspark.sql.functions import avg

avg_salary_df = sample_df.groupBy("Department").agg(avg("Salary").alias("Avg_Salary"))
display(avg_salary_df)

# COMMAND ----------

# MAGIC %md ## DataFrame Transformation Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`select`** | Returns a new DataFrame by computing given expression for each element |
# MAGIC | **`drop`** | Returns a new DataFrame with a column dropped |
# MAGIC | **`withColumnRenamed`** | Returns a new DataFrame with a column renamed |
# MAGIC | **`withColumn`** | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | **`filter`**, **`where`** | Filters rows using the given condition |
# MAGIC | **`sort`**, **`orderBy`** | Returns a new DataFrame sorted by the given expressions |
# MAGIC | **`dropDuplicates`**, **`distinct`** | Returns a new DataFrame with duplicate rows removed |
# MAGIC | **`limit`** | Returns a new DataFrame by taking the first n rows |
# MAGIC | **`groupBy`** | Groups the DataFrame using the specified columns, so we can run aggregation on them |

# COMMAND ----------

emp_df = sample_df.select("Emp_ID", "Name")
display(emp_df)

# COMMAND ----------

anonymous_df = sample_df.drop("Name", "Department")
display(anonymous_df)

# COMMAND ----------

# MAGIC %md ### Add or replace columns
# MAGIC Use DataFrame transformations to add or replace columns

# COMMAND ----------

# MAGIC %md 
# MAGIC #### **`withColumn()`**
# MAGIC Returns a new DataFrame by adding a column or replacing an existing column that has the same name.

# COMMAND ----------

from pyspark.sql.functions import col

monthly_salary_df = sample_df.withColumn("Monthly_Salary", (col("Salary")/12).cast("Integer"))
display(monthly_salary_df)

# COMMAND ----------

# MAGIC %md #### **`withColumnRenamed()`**
# MAGIC Returns a new DataFrame with a column renamed.

# COMMAND ----------

annual_salary_df = sample_df.withColumnRenamed("Saalry", "Annual_Salary")
display(annual_salary_df)

# COMMAND ----------

# MAGIC %md #### **`filter()`**
# MAGIC Filters rows using the given SQL expression or column based condition.
# MAGIC
# MAGIC ##### Alias: **`where`**

# COMMAND ----------

finance_df = sample_df.filter((col("Department") == "Finance") & (col("Salary") >= 50000))
display(finance_df)

# COMMAND ----------

# MAGIC %md #### **`dropDuplicates()`**
# MAGIC Returns a new DataFrame with duplicate rows removed, optionally considering only a subset of columns.
# MAGIC
# MAGIC ##### Alias: **`distinct`**

# COMMAND ----------

cols = ["Emp_ID", "Name", "Department", "Salary"]
data = [[1, "Tom", "IT", 50000], [2, "Megan", "Finance", 60000], [3, "Petra", "Sales", 100000], [4, "Max", "HR", 40000], [5, "Prasad", "Finance", 50000], [6, "Terry", "Sales", 90000], [7, "Alex", "HR", 90000], [7, "Alex", "HR", 90000]]

sample_df = (spark
           .createDataFrame(data=data, schema=cols)
          )

display(sample_df)

# COMMAND ----------

display(sample_df.distinct().sort("Emp_ID"))

# COMMAND ----------

distinct_empID_df = sample_df.dropDuplicates(["Name"])
display(distinct_empID_df)

# COMMAND ----------

# MAGIC %md ### Sort rows
# MAGIC Use DataFrame transformations to sort rows

# COMMAND ----------

# MAGIC %md #### **`sort()`**
# MAGIC Returns a new DataFrame sorted by the given columns or expressions.
# MAGIC
# MAGIC ##### Alias: **`orderBy`**

# COMMAND ----------

sorted_salary_df = sample_df.sort(col("Salary").desc())
display(sorted_salary_df)

# COMMAND ----------

sort_df = sample_df.orderBy(["Department", "Salary"])
display(sort_df)

# COMMAND ----------

# MAGIC %md ### Joining DataFrames
# MAGIC The DataFrame <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.join.html?highlight=join#pyspark.sql.DataFrame.join" target="_blank">**`join`**</a> method joins two DataFrames based on a given join expression. 
# MAGIC
# MAGIC Several different types of joins are supported:
# MAGIC
# MAGIC Inner join based on equal values of a shared column called "name" (i.e., an equi join)<br/>
# MAGIC **`df1.join(df2, on="name")`**
# MAGIC
# MAGIC Inner join based on equal values of the shared columns called "name" and "age"<br/>
# MAGIC **`df1.join(df2, on=["name", "age"])`**
# MAGIC
# MAGIC Full outer join based on equal values of a shared column called "name"<br/>
# MAGIC **`df1.join(df2, on="name", how="outer")`**
# MAGIC
# MAGIC Left outer join based on an explicit column expression<br/>
# MAGIC **`df1.join(df2, df1["customer_name"] == df2["account_name"], how="left_outer")`**

# COMMAND ----------

cols = ["Department_Name", "Department_Head"]
data = [["IT", "Andrew"], ["Finance", "Kathy"], ["Sales", "Ravi"]]

dept_df = (spark
           .createDataFrame(data=data, schema=cols)
          )

display(dept_df)

# COMMAND ----------

joined_df = sample_df.join(dept_df, sample_df["Department"] == dept_df["Department_Name"], "left_outer").select(["Emp_ID","Name", "Department", "Salary", "Department_Head"])
display(joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Union and unionByName
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> The DataFrame <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.union.html" target="_blank">**`union`**</a> method resolves columns by position, as in standard SQL. You should use it only if the two DataFrames have exactly the same schema, including the column order. In contrast, the DataFrame <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.unionByName.html" target="_blank">**`unionByName`**</a> method resolves columns by name.  This is equivalent to UNION ALL in SQL.  Neither one will remove duplicates.  
# MAGIC
# MAGIC Below is a check to see if the two dataframes have a matching schema where **`union`** would be appropriate

# COMMAND ----------

cols = ["Emp_ID", "Name", "Department", "Salary"]
data = [[10, "Van", "IT", 60000], [11, "Chloe", "Sales", 100000]]

new_df = (spark
           .createDataFrame(data=data, schema=cols)
          )
display(new_df)

# COMMAND ----------

union_df = sample_df.union(new_df)
display(union_df)


# COMMAND ----------

union_df = sample_df.select(["Emp_ID", "Name", "Department"]).union(new_df.select(["Emp_ID", "Name", "Department"]))
display(union_df)