# Databricks notebook source
# MAGIC %md ## Query Execution
# MAGIC We can express the same query using any interface. The Spark SQL engine generates the same query plan used to optimize and execute on our Spark cluster.
# MAGIC
# MAGIC ![query execution engine](https://files.training.databricks.com/images/aspwd/spark_sql_query_execution_engine.png)
# MAGIC

# COMMAND ----------

from datetime import datetime, timedelta
import calendar
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC First day of the month is considered as the anchor for all the calculations

# COMMAND ----------

# %let var_x = 0

var_x = 0

# COMMAND ----------

# CALL SYMPUT('var_year',substr(PUT(intnx('MONTH',today(),&var_x,"B"),YYMMN6.)1,4));

shifted_date = datetime.today() + timedelta(days=var_x*30)

# Extract the year portion
var_year = str(shifted_date.year)

print(var_year)

# COMMAND ----------

# CALL SYMPUT('var_month',substr(PUT(intnx('MONTH',today(),&var_x,"B"),YYMMN6.)5,2));

shifted_date = datetime.today() + timedelta(days=var_x * 30)

# Extract the month portion
var_month = str(shifted_date.month).zfill(2)
print(var_month)

# COMMAND ----------

# CALL SYMPUT('var_monthname',substr(strip(put(intnx('MONTH',today(),&var_x,"B"),MONNAME9.))1,3));

shifted_date = datetime.today() + timedelta(days=var_x * 30)

# Extract the abbreviated month name
month_name = calendar.month_abbr[shifted_date.month]

print(month_name)

# COMMAND ----------

# CALL SYMPUT('var_year_minus',substr(put(intnx('MONTH',today(),&var_x-1,"B"),YYMMN6.)1,4));

shifted_date = datetime.today() + timedelta(days=(var_x - 1) * 30)

# Extract the year portion
var_year_minus = str(shifted_date.year)

print(var_year_minus)

# COMMAND ----------

# CALL SYMPUT('var_month_minus',substr(put(intnx('MONTH',today(),&var_x-1,"B"),YYMMN6.)5,2));

shifted_date = datetime.today() + timedelta(days=(var_x - 1) * 30)

# Extract the month portion
var_month_minus = str(shifted_date.month).zfill(2)  # Zero-pad the month if necessary

print(var_month_minus)
# In SAS number of days in a month are calculated for each month

# COMMAND ----------

# CALL SYMPUT('var_monthname_minus',substr(strin(put(intnx('MONTH',today(),&var_x-1,"B"),MONNAME9.))1,3));

shifted_date = datetime.today() + timedelta(days=(var_x - 1) * 30)

# Get the month name and extract the first three characters
var_monthname_minus = calendar.month_abbr[shifted_date.month][:3]

print(var_monthname_minus)

# COMMAND ----------

# CALL SYMPUT('var_date1',PUT(intnx('MONTH',today(),&var_x,"B"),EURDFDE9.));

shifted_date = datetime.today().replace(day=1) + relativedelta(months=var_x)

# Format the shifted date as "DDMMMYYYY"
var_date1 = shifted_date.strftime("%d%b%Y")

print(var_date1)

# COMMAND ----------

# CALL SYMPUT('var_date2',PUT(intnx('MONTH',today(),&var_x,"E"),EURDFDE9.));

shifted_date = datetime.today().replace(day=1) + relativedelta(months=var_x + 1) - relativedelta(days=1)

# Format the shifted date as "DDMMMYYYY"
var_date2 = shifted_date.strftime("%d%b%Y")
print(var_date2)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table jay_mehta_catalog.sas.jay_test (month_name string, month_last_date date);
# MAGIC insert into jay_mehta_catalog.sas.jay_test values ("May", '2024-05-31');
# MAGIC insert into jay_mehta_catalog.sas.jay_test values ("June", '2024-06-30');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jay_mehta_catalog.sas.jay_test

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Use string python variable and use it in where condition where the column type is date

# COMMAND ----------

display(spark.sql(f"""
                  select * from jay_mehta_catalog.sas.jay_test
                  where month_last_date = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('{var_date2}', 'ddMMMyyyy'), 'yyyy-MM-dd'))
                  """))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Use a "select *" query after creating a table or view

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table jay_mehta_catalog.sas.jay_1 as select 1;
# MAGIC select * from jay_mehta_catalog.sas.jay_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view vw_jay_1 as select 1 as colA;
# MAGIC select * from vw_jay_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DECLARE VARIABLE myvar1 INT DEFAULT 10;
# MAGIC
# MAGIC SET VAR myvar1 = 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC values(myvar1)

# COMMAND ----------

# MAGIC %sql
# MAGIC set dummy.var_x = -0;    
# MAGIC
# MAGIC set dummy.current_date = dateadd (month,${dummy.var_x}, current_date()); 
# MAGIC set dummy.var_year = year (dateadd(month,${dummy.var_x}, current_date()));
# MAGIC set dummy.var_month = month (dateadd (month,${dummy.var_x}, current_date()));
# MAGIC set dummy.var_year_str = cast(${dummy.var_year} as string);
# MAGIC set dummy.evar_month_str = cast(${dummy.var_month} as string) ;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table jay_mehta_catalog.sas.base_rcibase_sql_test as
# MAGIC select ${dummy.current_date} AS CURR_DATE, ${dummy.var_year} as YEAR, ${dummy.var_month} as MONTH, ${dummy.var_year_str} as YEAR_STRING, ${dummy.evar_month_str} AS MONTH_STRING

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jay_mehta_catalog.sas.base_rcibase_sql_test

# COMMAND ----------

from datetime import datetime, timedelta
import calendar
from dateutil.relativedelta import relativedelta

# COMMAND ----------

var_year = (datetime.today().year)

print(var_year)

# COMMAND ----------

spark.sql(f"""
          create or replace table jay_mehta_catalog.sas.year_{var_year} as 
          select {var_year} as year
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jay_mehta_catalog.sas.year_2024;