-- Databricks notebook source
CREATE TABLE jay_mehta_catalog.sas.dim_address (id int, city string, state string, country string);
insert into jay_mehta_catalog.sas.dim_address values (1, "Toronto", "ON", "CA");
insert into jay_mehta_catalog.sas.dim_address values (2, "Ottawa", "ON", "CA");
insert into jay_mehta_catalog.sas.dim_address values (3, "Mississauga", "ON", "CA");
insert into jay_mehta_catalog.sas.dim_address values (4, "Calgary", "AB", "CA");
insert into jay_mehta_catalog.sas.dim_address values (5, "Montreal", "QC", "CA");

-- COMMAND ----------

select * from jay_mehta_catalog.sas.dim_address order by id

-- COMMAND ----------

-- MAGIC %py
-- MAGIC state = "ON"
-- MAGIC city = "Toronto"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Option 1: Use format() method

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql("""
-- MAGIC           create or replace temporary view vw_dim_on as 
-- MAGIC           select * from jay_mehta_catalog.sas.dim_address 
-- MAGIC           where state = '{state}' and city = '{city}'
-- MAGIC           """.format(state=state, city=city))

-- COMMAND ----------

select * from vw_dim_on

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Option 2: Set spark config

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC state = "AB"
-- MAGIC
-- MAGIC spark.conf.set("spark.state_var", state)

-- COMMAND ----------

create or replace temporary view vw_dim_ab as 
select * from jay_mehta_catalog.sas.dim_address 
where state = '${spark.state_var}';
select * from vw_dim_ab;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Option 3: Use f string --> Recommended

-- COMMAND ----------

-- MAGIC %py
-- MAGIC state_qc = "QC"

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f""" create or replace temporary view vw_dim_qc as  SELECT * from jay_mehta_catalog.sas.dim_address 
-- MAGIC where state = '{state_qc}'; """)
-- MAGIC

-- COMMAND ----------

select * from vw_dim_qc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Int variables

-- COMMAND ----------

-- MAGIC %py
-- MAGIC id = 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC id = 1  # replace with the desired value
-- MAGIC query = """
-- MAGIC         CREATE OR REPLACE TEMPORARY VIEW vw_dim_1 AS  
-- MAGIC         SELECT *
-- MAGIC         FROM jay_mehta_catalog.sas.dim_address
-- MAGIC         WHERE id = {}
-- MAGIC         """.format(id)
-- MAGIC
-- MAGIC spark.sql(query)

-- COMMAND ----------

select * from vw_dim_1;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f""" 
-- MAGIC         CREATE OR REPLACE TEMPORARY VIEW vw_dim_1 AS  
-- MAGIC         SELECT *
-- MAGIC         FROM jay_mehta_catalog.sas.dim_address
-- MAGIC         WHERE id = {id}
-- MAGIC         """
-- MAGIC         )

-- COMMAND ----------

select * from vw_dim_1

-- COMMAND ----------

select * from vw_dim_qc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Complex use case of dynamically creating views by state

-- COMMAND ----------

-- MAGIC %py
-- MAGIC unique_state_df = spark.sql("select distinct state from jay_mehta_catalog.sas.dim_address")
-- MAGIC display(unique_state_df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Approach 1: Using collect()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC for row in unique_state_df.collect():
-- MAGIC     state_var = row["state"]
-- MAGIC     print(f"running loop for {state_var}")
-- MAGIC     dynamic_view_name = f"dyn_vw_dim_{state_var}"
-- MAGIC     print(dynamic_view_name)
-- MAGIC     spark.sql(f"""
-- MAGIC         create or replace temporary view {dynamic_view_name} as
-- MAGIC         select * from jay_mehta_catalog.sas.dim_address
-- MAGIC         where state = '{state_var}'
-- MAGIC     """)

-- COMMAND ----------

select * from dyn_vw_dim_QC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Approach 2: Using pandas
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.pandas as ps
-- MAGIC
-- MAGIC pandas_unique_state_df = unique_state_df.pandas_api()
-- MAGIC display(pandas_unique_state_df)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC for i in range(0, len(pandas_unique_state_df)):
-- MAGIC   state_var = pandas_unique_state_df.loc[i, "state"]
-- MAGIC   print(f"running loop for {state_var}")
-- MAGIC   dynamic_view_name = f"dyn_vw_dim_{state_var}"
-- MAGIC   print(dynamic_view_name)
-- MAGIC   spark.sql(f"""
-- MAGIC             create or replace temporary view {dynamic_view_name} as
-- MAGIC             select * from jay_mehta_catalog.sas.dim_address
-- MAGIC             where state = '{state_var}'
-- MAGIC             """)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### How to execute a Databricks notebook from a different notebook

-- COMMAND ----------

-- MAGIC %py
-- MAGIC a = 1
-- MAGIC b = 2

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if a == 1 and b == 2:
-- MAGIC   dbutils.notebook.run("./SDW10-Test", timeout_seconds=6000)

-- COMMAND ----------

-- MAGIC %run ./SDW10-Test

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### How to deal with Dates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### The pytz module allows for date-time conversion and timezone calculations

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %pip install pytz

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime, timedelta, time
-- MAGIC import calendar
-- MAGIC import pytz

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC # set the time zone
-- MAGIC est_timezone = pytz.timezone("US/Eastern")
-- MAGIC
-- MAGIC today = datetime.now(est_timezone).date()
-- MAGIC print(f"today is: {today}")
-- MAGIC
-- MAGIC I = 1
-- MAGIC
-- MAGIC curr_d = today - timedelta(days=I)
-- MAGIC print(f"current date is: {curr_d}")
-- MAGIC
-- MAGIC curr_d_formatted = curr_d.strftime("%d%m%Y")
-- MAGIC print(f"formatted current date is: {curr_d_formatted}")
-- MAGIC
-- MAGIC new_curr_d_formatted = curr_d.strftime("%d%b%Y:%H:%M:%S")
-- MAGIC print(f"formatted current date is: {new_curr_d_formatted}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### to_date() function in Databricks
-- MAGIC Documentation link for builtin date functions - https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-functions-builtin#date-timestamp-and-interval-functions

-- COMMAND ----------

select to_date("31DEC2017:00:00", "ddMMMyyyy:HH:mm")

-- COMMAND ----------

select to_date("31DEC2017:00:00", "ddMMMyyyy:HH:mm")

-- COMMAND ----------

select to_date("29112023", "ddMMyyyy")

-- COMMAND ----------

select to_date("01JUL2020:00:00:02", "ddMMMyyyy:HH:mm:ss")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Macros to function conversion
-- MAGIC Convert null values in numeric cols to 0

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql.functions import col, when
-- MAGIC
-- MAGIC def null_to_zero(df):
-- MAGIC     numeric_columns = [c for c, t in df.dtypes if t == 'int' or t == 'double' or t == 'bigint']
-- MAGIC     for column in numeric_columns:
-- MAGIC         df = df.withColumn(column, when(col(column).isNull(), 0).otherwise(col(column).cast('double')))
-- MAGIC     return df

-- COMMAND ----------

-- MAGIC %py
-- MAGIC data = [("John", 25, None), ("Alice", None, 30), ("Tom", 28, 35)]
-- MAGIC columns = ["Name", "Age", "Score"]
-- MAGIC
-- MAGIC df = spark.createDataFrame(data, columns)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC result_df = null_to_zero(df)
-- MAGIC display(result_df)