# Databricks notebook source
# MAGIC %md
# MAGIC ##### Scenario 1: Calculate DISCOUNT_AMT and MRC_BALANCE based on DISC_QTY_TYPE per YEAR, MONTH, BAN, SUBSCRIBER_NO, PRICE_PLAN_SOC

# COMMAND ----------

# MAGIC %py
# MAGIC import os
# MAGIC import json
# MAGIC from pyspark.sql import *
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.functions import monotonically_increasing_id, row_number, col
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.session import SparkSession
# MAGIC from datetime import datetime, timedelta
# MAGIC from pyspark.sql.functions import sum, col, desc
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.functions import pandas_udf, PandasUDFType
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# MAGIC
# MAGIC # Set Notebook start time.
# MAGIC date_time = datetime.now()
# MAGIC run_start_time = date_time.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# creating dummy data
 
columns = ['sys_row_id',
           'year',
           'month',
           'BAN', 
           'SUBSCRIBER_NO',
           'PRICE_PLAN_SOC',
           'o_SOC_DESCRIPTION',
           'SOC_TYPE',
           'SOC_EFFECTIVE_DATE',
           'SOC_EXP_DT',
           'PRICE_PLAN_MSF',
           'o_DISCOUNT_CODE',
           'DISC_EFFECTIVE_DATE',
           'DISC_EXPIRATION_DATE',
           'o_DISCOUNT_DESC',
           'FLAT_AMT',
           'DISCOUNT_DURATION',
           'DISC_QTY_TYPE',
           'DISC_SEQ_NO']
vals = [
     (190,2022,6,1234,88888,'soc1','sample','cd','12-10-2020','12-10-2020',850,'JNJ','12-10-2020','12-10-2020','flat',20,11,'D',111),
	 (190,2022,6,1234,88888,'soc1','sample','cd','12-10-2020','12-10-2020',850,'JNJ','12-10-2020','12-10-2020','flat',20,11,'D',222),
	 (190,2022,6,1234,88888,'soc1','sample','cd','12-10-2020','12-10-2020',850,'JNJ','12-10-2020','12-10-2020','flat',20,11,'P',333),
	 (190,2022,6,1234,88888,'soc1','sample','cd','12-10-2020','12-10-2020',850,'JNJ','12-10-2020','12-10-2020','flat',20,11,'P',444),
	 
	 (190,2022,6,1235,88888,'soc1','sample','cd','12-10-2020','12-10-2020',850,'JNJ','12-10-2020','12-10-2020','flat',20,11,'D',111),
	 (190,2022,6,1235,88888,'soc1','sample','cd','12-10-2020','12-10-2020',850,'JNJ','12-10-2020','12-10-2020','flat',20,11,'D',222),
	 (190,2022,6,1236,88888,'soc1','sample','cd','12-10-2020','12-10-2020',850,'JNJ','12-10-2020','12-10-2020','flat',20,11,'P',333),
	 (190,2022,6,1237,88888,'soc1','sample','cd','12-10-2020','12-10-2020',850,'JNJ','12-10-2020','12-10-2020','flat',20,11,'P',444),
    (190,2022,6,1237,88888,'soc1','sample','cd','12-10-2020','12-10-2020',850,'JNJ','12-10-2020','12-10-2020','flat',20,11,'P',444),
      ]

df = spark.createDataFrame(vals, columns)
display(df)
# exp_PASS_THROUGH = df
# display(exp_PASS_THROUGH)

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC windowSpec  = Window.partitionBy("YEAR", "MONTH", "BAN", "SUBSCRIBER_NO", "PRICE_PLAN_SOC").orderBy('DISC_QTY_TYPE','DISC_SEQ_NO')
# MAGIC row_df = df.withColumn("row_DISC",row_number().over(windowSpec))
# MAGIC display(row_df)

# COMMAND ----------

#Other_Columns_DF=srt_ASC_COLS.select(\
spark_df=row_df.select(\
row_df.year.alias("YEAR"),\
row_df.month.alias("MONTH"),\
row_df.BAN,\
row_df.SUBSCRIBER_NO,\
row_df.PRICE_PLAN_SOC.alias("o_PRICE_PLAN_SOC"),\
row_df.DISC_QTY_TYPE,\
row_df.DISC_SEQ_NO,\
row_df.o_SOC_DESCRIPTION,\
row_df.SOC_TYPE,\
row_df.SOC_EFFECTIVE_DATE,\
row_df.SOC_EXP_DT,\
row_df.PRICE_PLAN_MSF,\
row_df.o_DISCOUNT_CODE,\
row_df.DISC_EFFECTIVE_DATE,\
row_df.DISC_EXPIRATION_DATE,\
row_df.o_DISCOUNT_DESC,\
row_df.FLAT_AMT,\
row_df.DISCOUNT_DURATION,\
row_df.row_DISC\
)

pandas_df = spark_df.toPandas()

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC def my_loop_function(xdf):
# MAGIC
# MAGIC   for i in range(0, len(xdf)):
# MAGIC     if xdf.loc[i, 'row_DISC'] == 1:
# MAGIC       if  xdf.loc[i, 'DISC_QTY_TYPE'] == 'D':
# MAGIC           xdf.loc[i, 'DISCOUNT_AMT'] = xdf.loc[i, 'FLAT_AMT']
# MAGIC           xdf.loc[i, 'MRC_BALANCE'] = xdf.loc[i, 'PRICE_PLAN_MSF'] - xdf.loc[i, 'FLAT_AMT']
# MAGIC       else:
# MAGIC          if xdf.loc[i, 'DISC_QTY_TYPE'] == 'P':
# MAGIC             xdf.loc[i, 'DISCOUNT_AMT'] = (xdf.loc[i, 'PRICE_PLAN_MSF'] * (xdf.loc[i, 'FLAT_AMT'] / 100))
# MAGIC             xdf.loc[i, 'MRC_BALANCE'] = xdf.loc[i, 'PRICE_PLAN_MSF'] - xdf.loc[i, 'DISCOUNT_AMT']
# MAGIC     else:
# MAGIC       if xdf.loc[i, 'DISC_QTY_TYPE'] == 'D':
# MAGIC         xdf.loc[i, 'DISCOUNT_AMT'] = xdf.loc[i, 'FLAT_AMT']
# MAGIC         xdf.loc[i, 'MRC_BALANCE'] = xdf.loc[i-1, 'MRC_BALANCE'] - xdf.loc[i, 'FLAT_AMT']
# MAGIC       else:
# MAGIC         if xdf.loc[i, 'DISC_QTY_TYPE'] == 'P':
# MAGIC           xdf.loc[i, 'DISCOUNT_AMT'] = (xdf.loc[i-1, 'MRC_BALANCE'] * xdf.loc[i, 'FLAT_AMT'] / 100)
# MAGIC           xdf.loc[i, 'MRC_BALANCE'] = xdf.loc[i-1, 'MRC_BALANCE'] - xdf.loc[i, 'DISCOUNT_AMT']
# MAGIC   return xdf
# MAGIC

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC result_df = my_loop_function(pandas_df)
# MAGIC display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### How to optimize my_loop_function?

# COMMAND ----------

schema = StructType([
#StructField('RowID', IntegerType()),
StructField('YEAR', IntegerType()),
StructField('MONTH', IntegerType()),
StructField('BAN', IntegerType()),
StructField('SUBSCRIBER_NO', IntegerType()),
StructField('o_PRICE_PLAN_SOC', StringType()),
StructField('DISC_QTY_TYPE', StringType()),
StructField('DISC_SEQ_NO', IntegerType()),
StructField('o_SOC_DESCRIPTION', StringType()),
StructField('SOC_TYPE', StringType()),
StructField('SOC_EFFECTIVE_DATE', StringType()),
StructField('SOC_EXP_DT', StringType()),
StructField('PRICE_PLAN_MSF', IntegerType()),
StructField('o_DISCOUNT_CODE', StringType()),
StructField('DISC_EFFECTIVE_DATE', StringType()),
StructField('DISC_EXPIRATION_DATE', StringType()),
StructField('o_DISCOUNT_DESC', StringType()),
StructField('FLAT_AMT', IntegerType()),
StructField('DISCOUNT_DURATION', IntegerType()),
StructField('row_DISC', IntegerType()),
StructField('Discount_Amt', IntegerType()),
StructField('mrc_balance', IntegerType()),
])

# COMMAND ----------

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def my_grouped_map_function(xdf):
  # Perform some operations on the DataFrame
  #print(key)
  #xdf = x_df.iloc[0]
  for i in range(0, len(xdf)):
    if xdf.loc[i, 'row_DISC'] == 1:
      if  xdf.loc[i, 'DISC_QTY_TYPE'] == 'D':
          xdf.loc[i, 'Discount_Amt'] = xdf.loc[i, 'FLAT_AMT']
          xdf.loc[i, 'mrc_balance'] = xdf.loc[i, 'PRICE_PLAN_MSF'] - xdf.loc[i, 'FLAT_AMT']
      else:
         if xdf.loc[i, 'DISC_QTY_TYPE'] == 'P':
            xdf.loc[i, 'Discount_Amt'] = (xdf.loc[i, 'PRICE_PLAN_MSF'] * (xdf.loc[i, 'FLAT_AMT'] / 100))
            xdf.loc[i, 'mrc_balance'] = xdf.loc[i, 'PRICE_PLAN_MSF'] - xdf.loc[i, 'Discount_Amt']
    else:
      if  xdf.loc[i, 'DISC_QTY_TYPE'] == 'D':
        xdf.loc[i, 'Discount_Amt'] = xdf.loc[i, 'FLAT_AMT']
        xdf.loc[i, 'mrc_balance'] = xdf.loc[i-1, 'mrc_balance'] - xdf.loc[i, 'FLAT_AMT']
      else:
        if xdf.loc[i, 'DISC_QTY_TYPE'] == 'P':
          xdf.loc[i, 'Discount_Amt'] = (xdf.loc[i-1, 'mrc_balance'] * xdf.loc[i, 'FLAT_AMT'] / 100)
          xdf.loc[i, 'mrc_balance'] = xdf.loc[i-1, 'mrc_balance'] - xdf.loc[i, 'Discount_Amt']
  return xdf

# COMMAND ----------

df=spark_df.groupby(["YEAR", "MONTH", "BAN", "SUBSCRIBER_NO", "o_PRICE_PLAN_SOC"]).apply(my_grouped_map_function)

# View the transformed DataFrame
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Scenario 2: Finding delta records from source table and load in target table

# COMMAND ----------

# MAGIC %sql
# MAGIC use jay_mehta_catalog.sas;
# MAGIC select * from ey_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inflow_data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating variables

# COMMAND ----------

# MAGIC %md
# MAGIC ###### end_date

# COMMAND ----------

from datetime import datetime, timedelta

# Get the current date
current_date = datetime.now()

# Calculate yesterday's date
yesterday_date = current_date - timedelta(days=1)

# Print yesterday's date in a specific format (optional)
end_date = yesterday_date.strftime('%Y-%m-%d')
print(end_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### base_year

# COMMAND ----------

import datetime

# Get the current date
current_date = datetime.date.today()

# Create a new date object with the same year and month as the current date, but with day 1
base_year = datetime.date(current_date.year, current_date.month, 1)

print(base_year)


# COMMAND ----------

# MAGIC %md
# MAGIC ###### current year

# COMMAND ----------

var_year = datetime.date.today().year

print(var_year)

# COMMAND ----------

inflow_df = spark.table("jay_mehta_catalog.sas.inflow_data")
display(inflow_df)

# COMMAND ----------

ey_df = spark.table("jay_mehta_catalog.sas.ey_data")
display(ey_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### max saturaday date from inflow table

# COMMAND ----------

from pyspark.sql.functions import col, rank, dense_rank
from pyspark.sql import Window

# first, create a Window partitioned by the week number (starting on Saturday) and ordered by descending dates
w = Window.partitionBy((col('date').cast('long')/604800).cast('integer')).orderBy(col("date").desc())

# assign a rank to each date based on the week number
df = inflow_df.withColumn("rank", dense_rank().over(w))

# keep only the latest week (rank 1), which corresponds to the latest Saturday date in the dataframe
latest_saturday_date_df = df.filter(col("rank") == 1)

# display the latest Saturday date
inflow_max_sat = latest_saturday_date_df.select("date").collect()[0][0]
print(inflow_max_sat)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### max date from ey table

# COMMAND ----------

from pyspark.sql.functions import max

max_ey_date = ey_df.select(max("date")).collect()[0][0]
print(max_ey_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### load data from 2023-08-27 till 2023-09-02 from ey table to inflow table

# COMMAND ----------

from datetime import datetime, timedelta

# Convert the string to a datetime object
date_object = datetime.strptime(str(inflow_max_sat), "%Y-%m-%d")

# Subtract one day from the datetime object
new_date_object = date_object + timedelta(days=1)

# Format the result as a string
start_date = new_date_object.strftime("%Y-%m-%d")

print(start_date)

# COMMAND ----------

delta_df = spark.sql("""
insert into jay_mehta_catalog.sas.inflow_data
select date, subject, attendance, 15, 15-attendance 
from jay_mehta_catalog.sas.ey_data
where date between '{start_date}' and '{max_ey_date}'
""".format(start_date=start_date,max_ey_date=max_ey_date)
)
display(delta_df)             