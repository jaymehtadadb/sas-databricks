-- Databricks notebook source
-- MAGIC %md
-- MAGIC     /* create a function */
-- MAGIC     %macro null_to_zero (table_name=&table_name.);
-- MAGIC     data &table_name.;
-- MAGIC     set &table_name.;
-- MAGIC     array change _numeric_;
-- MAGIC     do over change;
-- MAGIC     if change=. then change=0;
-- MAGIC     end;
-- MAGIC     run ;
-- MAGIC     %mend null_to_zero ;
-- MAGIC
-- MAGIC     /* flip from row value of segment_desc to column */
-- MAGIC     proc transpose data=dataset
-- MAGIC     out=step1 (drop=measurement);
-- MAGIC     by fsa pop_Total subs mrc segment_desc;
-- MAGIC     var subs_rogers mrc_rogers; 
-- MAGIC     run;
-- MAGIC
-- MAGIC     /* row to column with name change */
-- MAGIC     proc transpose data=step1 out=Final_output(drop=_name_) delim=_ ;
-- MAGIC     by fsa pop_Total subs mrc;
-- MAGIC     id _name_ segment_desc ;
-- MAGIC     var col1;
-- MAGIC     run;
-- MAGIC
-- MAGIC     /* calling the function */
-- MAGIC     %null_to_zero(table_name=Final_output);

-- COMMAND ----------

select * from jay_mehta_catalog.sas.transpose_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### SQL query with case when statements

-- COMMAND ----------

SELECT fsa, pop_total, subs, mrc, 
COALESCE(Consumer_subs_rogers, 0) AS Consumer_subs_rogers, 
COALESCE(Consumer_mrc_rogers, 0) AS Consumer_mrc_rogers,
COALESCE(RPP_subs_rogers, 0) AS RPP_subs_rogers,
COALESCE(RPP_mrc_rogers, 0) AS RPP_mrc_rogers FROM (
SELECT fsa, pop_total, subs, mrc,
max(CASE WHEN segment_desc = "Consumer" THEN subs_rogers END) AS Consumer_subs_rogers, 
max(CASE WHEN segment_desc = "Consumer" THEN mrc_rogers END) AS Consumer_mrc_rogers, 
max(CASE WHEN segment_desc = "RPP" THEN subs_rogers END) AS RPP_subs_rogers,
max(CASE WHEN segment_desc = "RPP" THEN mrc_rogers END) AS RPP_mrc_rogers  
FROM jay_mehta_catalog.sas.transpose_table 
group by fsa, pop_total, subs, mrc
order by fsa, pop_total
)A

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### SQL query with pivot function

-- COMMAND ----------

SELECT *
FROM (
  SELECT fsa, pop_total, subs, mrc, segment_desc, subs_rogers, mrc_rogers
  FROM jay_mehta_catalog.sas.transpose_table
)
PIVOT (
  SUM(subs_rogers) AS subs_rogers, SUM(mrc_rogers) AS mrc_rogers
  FOR segment_desc IN ('Consumer', 'RPP') -- Replace with your segment values
) order by fsa, pop_total


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Dynamically transposing columns with Pyspark

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Creating a dataframe

-- COMMAND ----------

insert into jay_mehta_catalog.sas.transpose_table values ("XYZ", 1000, 99, 999.99, "NEW_TEST", 999, 999);

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.sql("""
-- MAGIC                select * from jay_mehta_catalog.sas.transpose_table
-- MAGIC                """)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Dynamically transposing "segment_desc" column

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import when, col
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC transposed_df = df.groupBy('fsa', 'pop_total', 'subs', 'mrc').pivot('segment_desc').agg(
-- MAGIC     F.first('subs_rogers').alias('subs_rogers'),
-- MAGIC     F.first('mrc_rogers').alias('mrc_rogers')
-- MAGIC )
-- MAGIC display(transposed_df)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC def null_to_zero(df):
-- MAGIC     numeric_columns = [c for c, t in df.dtypes if t == 'int' or t == 'double' or t == 'bigint']
-- MAGIC     for column in numeric_columns:
-- MAGIC         df = df.withColumn(column, when(col(column).isNull(), 0).otherwise(col(column)))
-- MAGIC     return df

-- COMMAND ----------

-- MAGIC %py
-- MAGIC transposed_df = null_to_zero(transposed_df)
-- MAGIC display(transposed_df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC     /* merge multiple rows to one row by variable */
-- MAGIC     data want1;
-- MAGIC     length x $40.;
-- MAGIC       do until (last.ctn);
-- MAGIC         set test;
-- MAGIC           by ctn notsorted;
-- MAGIC         x=catx(',',x,soc);
-- MAGIC         end;
-- MAGIC     drop soc;
-- MAGIC     run;
-- MAGIC
-- MAGIC     cnt | soc
-- MAGIC     222 | a
-- MAGIC     222 | b
-- MAGIC     222 | c
-- MAGIC     222 | c
-- MAGIC     333 | a
-- MAGIC     333 | b
-- MAGIC     333 | f
-- MAGIC
-- MAGIC     Results:
-- MAGIC     cnt | x
-- MAGIC     222 | a,b,c
-- MAGIC     333 | a,b,f

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC data = [[222, "a"], [222, "b"], [222, "c"], [222, "c"], [333, "a"], [333, "b"], [333, "f"]]
-- MAGIC cols = ["cnt", "soc"]
-- MAGIC
-- MAGIC data_df = spark.createDataFrame(data=data, schema=cols)
-- MAGIC display(data_df)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC from pyspark.sql.window import Window
-- MAGIC from pyspark.sql.functions import col, concat_ws, when, last, collect_list, collect_set
-- MAGIC
-- MAGIC # Define the window specification
-- MAGIC windowSpec = Window.partitionBy("cnt").orderBy("cnt")
-- MAGIC
-- MAGIC # Use the last function to get the last value of 'soc' within each partition
-- MAGIC df = data_df.withColumn("x", when(last(col("cnt")).over(windowSpec) == col("cnt"), col("soc")).otherwise(None))
-- MAGIC
-- MAGIC # Aggregate the 'soc' values into a single column 'x' separated by commas
-- MAGIC #df = df.groupBy("cnt").agg(concat_ws(",", collect_list("x")).alias("x"))
-- MAGIC df = df.groupBy("cnt").agg(concat_ws(",", collect_set("x")).alias("x"))
-- MAGIC
-- MAGIC # Drop the 'soc' column
-- MAGIC df = df.drop("soc")
-- MAGIC
-- MAGIC # Show the resulting DataFrame
-- MAGIC display(df)
-- MAGIC