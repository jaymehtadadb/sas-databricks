-- Databricks notebook source
-- MAGIC %md
-- MAGIC 	proc printto log="/app/sasdata/awesome/data/LM/RTDM/schedule/logs/04_ch_nac.log" new; run;
-- MAGIC
-- MAGIC 	options compress=yes reuse=yes;
-- MAGIC
-- MAGIC 	proc sql;
-- MAGIC 	create table day_count as 
-- MAGIC 	select activity_date, count(*) as day_count
-- MAGIC 	from ch_new_sales
-- MAGIC 	group by activity_date
-- MAGIC 	order by activity_date;
-- MAGIC 	quit;
-- MAGIC
-- MAGIC 	data day_count; 
-- MAGIC 	set day_count;
-- MAGIC 	by activity_date;
-- MAGIC 	if activity_date ne '' then count + 1;
-- MAGIC 	run;
-- MAGIC
-- MAGIC 	proc sql;
-- MAGIC 	select max(count) into:max_count
-- MAGIC 	from day_count 
-- MAGIC 	;
-- MAGIC 	quit;
-- MAGIC
-- MAGIC 	data _null_;
-- MAGIC 	call symput('max',strip(&max_count.));
-- MAGIC 	run;
-- MAGIC 	%put &max.;
-- MAGIC
-- MAGIC 	%let count_number=1;
-- MAGIC
-- MAGIC 	%put &count_number.;
-- MAGIC
-- MAGIC
-- MAGIC 	%macro ch_nac ();
-- MAGIC 	%do i=&count_number. %to &max.;
-- MAGIC 		proc sql;
-- MAGIC 		select datepart(activity_date) format=yymmddn8. into :activity_date
-- MAGIC 		from day_count where count=&i.;
-- MAGIC
-- MAGIC 		select datepart(activity_date) format=date9. into :activity_date_1
-- MAGIC 		from day_count where count=&i.;
-- MAGIC
-- MAGIC 		%put &activity_date. &activity_date_1.;
-- MAGIC 		
-- MAGIC 		proc sql;
-- MAGIC 		create table ch_nac_&i. as 
-- MAGIC 		select distinct a.*,
-- MAGIC 				b.*
-- MAGIC 		from 
-- MAGIC 			(select *
-- MAGIC 			from ch_new_sales
-- MAGIC 			where datepart(activity_date)="&activity_date_1."d
-- MAGIC 			)a
-- MAGIC 		left join 
-- MAGIC 			(select enterprise_id, customer_id, customer_location_id, product_lob,
-- MAGIC 					product_price_plan, product_offer, product_tier, product_code,
-- MAGIC 					product_discount_code, activity_date, cr_key,
-- MAGIC 					sum(product_quantity) as product_quantity,
-- MAGIC 					sum(closing) as closing, sum(product_msf) as product_msf, 
-- MAGIC 					sum(product_discount) as product_discount, 
-- MAGIC 					sum(product_msf_net) as product_msf_net
-- MAGIC 			from ch_closing 
-- MAGIC 			group by enterprise_id, customer_id, customer_location_id, product_lob,
-- MAGIC 					product_price_plan, product_offer, product_tier, product_code,
-- MAGIC 					product_discount_code, activity_date, cr_key
-- MAGIC 			)b	
-- MAGIC 		on a.activity_date=b.activity_date and a.enterprise_id=b.enterprise_id
-- MAGIC 		and a.customer_location_id=b.customer_location_id
-- MAGIC 		and a.customer_id=b.customer_id	and a.product_code=b.product_code
-- MAGIC 		and a.product_price_plan=b.product_price_plan;
-- MAGIC 		quit;
-- MAGIC
-- MAGIC 	%end;
-- MAGIC 	%do i=&count_number. %to &max.;
-- MAGIC 		%if  &i.=&count_number. %then %do;
-- MAGIC 		proc sql;
-- MAGIC 		create table ch_nac_rev as 
-- MAGIC 		select *, &i. as item
-- MAGIC 		from ch_nac_&i	
-- MAGIC 		; quit;   %end;
-- MAGIC 		%if  &i.> &count_number. %then %do;
-- MAGIC 		proc sql;
-- MAGIC 		insert into ch_nac_rev 
-- MAGIC 		select *, &i. as item
-- MAGIC 		from ch_nac_&i; quit; %end;
-- MAGIC 	%end;
-- MAGIC
-- MAGIC 	%do i=&count_number. %to &max.;
-- MAGIC 		proc sql;
-- MAGIC 		drop table ch_nac_&i;
-- MAGIC 	%end;
-- MAGIC
-- MAGIC 	%mend ch_nac;
-- MAGIC
-- MAGIC 	%ch_nac ();

-- COMMAND ----------

select * from jay_mehta_catalog.sas.ch_closing

-- COMMAND ----------

select * from jay_mehta_catalog.sas.ch_new_sales

-- COMMAND ----------

create or replace table jay_mehta_catalog.sas.day_count as
select activity_date, day_count + 1 as day_count from (
select activity_date, count(*) as day_count
from jay_mehta_catalog.sas.ch_new_sales
group by activity_date
order by activity_date
);

-- COMMAND ----------

select * from jay_mehta_catalog.sas.day_count

-- COMMAND ----------

-- MAGIC %python
-- MAGIC max_count_df = spark.sql("""
-- MAGIC                       select max(day_count)as max_day_count from jay_mehta_catalog.sas.day_count
-- MAGIC                       """)
-- MAGIC display(type(max_count_df))
-- MAGIC display(max_count_df)

-- COMMAND ----------

select activity_date, date(to_date(activity_date, "yy-MM-dd:HH:mm:ss")) as date from jay_mehta_catalog.sas.day_count

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import expr, date_format
-- MAGIC
-- MAGIC activity_date_df = spark.sql("""select to_date(activity_date, "yy-MM-dd:HH:mm:ss") as activity_date from jay_mehta_catalog.sas.day_count""")
-- MAGIC activity_date_df = activity_date_df\
-- MAGIC     .withColumn("activity_date_1", expr("int(concat(substring(activity_date, 3, 2), substring(activity_date, 6, 2), substring(activity_date, 9, 2)))"))\
-- MAGIC     .withColumn("activity_date_2", date_format(activity_date_df["activity_date"], "ddMMMyyyy"))
-- MAGIC display(activity_date_df)
-- MAGIC type(activity_date_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.pandas as ps
-- MAGIC
-- MAGIC pandas_activity_date_df = activity_date_df.pandas_api()
-- MAGIC display(pandas_activity_date_df)
-- MAGIC type(pandas_activity_date_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #count_number = 1
-- MAGIC #max_count = sorted(max_count_df.select("max_day_count").rdd.flatMap(lambda x: x).collect())
-- MAGIC
-- MAGIC for i in range(0, len(pandas_activity_date_df)):
-- MAGIC   print(i)
-- MAGIC   temp_table_name = f"jay_mehta_catalog.sas.ch_nac_{i}"
-- MAGIC   print(temp_table_name)
-- MAGIC   activity_date = pandas_activity_date_df.loc[i, "activity_date"]
-- MAGIC   print(activity_date)
-- MAGIC   
-- MAGIC   spark.sql("""
-- MAGIC   create or replace table {temp_table_name} as 
-- MAGIC 	select distinct a.CITY_NAME
-- MAGIC ,a.CYCLE_CLOSE_DAY
-- MAGIC ,a.CUSTOMER_SEGMENT
-- MAGIC ,a.PRODUCT_SOURCE
-- MAGIC ,a.ATTR_ADDRESS_POSTAL_CODE
-- MAGIC ,a.ACTIVITY_TYPE
-- MAGIC ,a.ENTERPRISE_ID
-- MAGIC ,a.PRODUCT_LOB
-- MAGIC ,a.PRODUCT_PRICE_PLAN
-- MAGIC ,a.CUSTOMER_BRAND
-- MAGIC ,a.ACTIVITY_DATE
-- MAGIC ,a.CUSTOMER_MULTI_PRODUCT
-- MAGIC ,a.CUSTOMER_LOCATION_ID
-- MAGIC ,a.CUSTOMER_ID
-- MAGIC ,a.CYCLE_CODE
-- MAGIC ,a.CHANNEL_1
-- MAGIC ,a.CHANNEL_4
-- MAGIC ,a.PRODUCT_BRAND
-- MAGIC ,a.ATTR_ADDRESS_SERVICEBILITY
-- MAGIC ,a.CHANNEL_3
-- MAGIC ,a.ACTIVITY
-- MAGIC ,a.DISCONNECT_CHURN
-- MAGIC ,a.CR_KEY
-- MAGIC ,a.CUSTOMER_VALUE
-- MAGIC ,a.PROVINCE_CODE
-- MAGIC ,a.CHANNEL_2
-- MAGIC ,a.PROVINCE
-- MAGIC ,a.CUSTOMER_COMPANY
-- MAGIC ,a.ATTR_ADDRESS_CONTRACT_GROUP
-- MAGIC ,a.PRODUCT_CODE
-- MAGIC ,a.ACTIVITY_DEALER
-- MAGIC ,a.NEW_SALE
-- MAGIC ,a.PRODUCT_LOB_UNIT
-- MAGIC --,b.CR_KEY
-- MAGIC --,b.ENTERPRISE_ID
-- MAGIC ,b.CLOSING
-- MAGIC --,b.PRODUCT_CODE
-- MAGIC --,b.ACTIVITY_DATE
-- MAGIC ,b.PRODUCT_OFFER
-- MAGIC ,b.PRODUCT_DISCOUNT
-- MAGIC --,b.CUSTOMER_LOCATION_ID
-- MAGIC --,b.CUSTOMER_ID
-- MAGIC --,b.PRODUCT_PRICE_PLAN
-- MAGIC ,b.PRODUCT_QUANTITY
-- MAGIC ,b.PRODUCT_TIER
-- MAGIC ,b.PRODUCT_MSF
-- MAGIC --,b.PRODUCT_LOB
-- MAGIC ,b.PRODUCT_MSF_NET
-- MAGIC ,b.PRODUCT_DISCOUNT_CODE
-- MAGIC 	from 
-- MAGIC 		(select *
-- MAGIC 		 from jay_mehta_catalog.sas.ch_new_sales
-- MAGIC 		 where date(to_date(activity_date, "yy-MM-dd:HH:mm:ss"))='{activity_date}'
-- MAGIC 		)a
-- MAGIC 	left join 
-- MAGIC 		(select enterprise_id, customer_id, customer_location_id, product_lob,
-- MAGIC 				product_price_plan, product_offer, product_tier, product_code,
-- MAGIC 				product_discount_code, activity_date, cr_key,
-- MAGIC 				sum(product_quantity) as product_quantity,
-- MAGIC 				sum(closing) as closing, sum(product_msf) as product_msf, 
-- MAGIC 				sum(product_discount) as product_discount, 
-- MAGIC 				sum(product_msf_net) as product_msf_net
-- MAGIC 		from jay_mehta_catalog.sas.ch_closing 
-- MAGIC 		group by enterprise_id, customer_id, customer_location_id, product_lob,
-- MAGIC 				product_price_plan, product_offer, product_tier, product_code,
-- MAGIC 				product_discount_code, activity_date, cr_key
-- MAGIC 		)b	
-- MAGIC 	on 1=1
-- MAGIC   and date(to_date(a.activity_date, "yy-MM-dd:HH:mm:ss"))=FROM_UNIXTIME(UNIX_TIMESTAMP(b.activity_date, 'ddMMMyyyy:HH:mm:ss'), 'yyyy-MM-dd') 
-- MAGIC   and a.enterprise_id=b.enterprise_id
-- MAGIC  	and a.customer_location_id=b.customer_location_id
-- MAGIC 	and a.customer_id=b.customer_id	
-- MAGIC   and a.product_code=b.product_code
-- MAGIC 	and a.product_price_plan=b.product_price_plan;
-- MAGIC             """.format(temp_table_name=temp_table_name, activity_date=activity_date))
-- MAGIC   print(f"ending loop {i}")
-- MAGIC   if i == 0:
-- MAGIC     spark.sql("""
-- MAGIC               create or replace table jay_mehta_catalog.sas.ch_nac_rev as 
-- MAGIC               select *, {i} as item
-- MAGIC               from {temp_table_name}
-- MAGIC               """.format(temp_table_name=temp_table_name, i=i))
-- MAGIC   else:
-- MAGIC     spark.sql("""
-- MAGIC               insert into jay_mehta_catalog.sas.ch_nac_rev
-- MAGIC               select *, {i} as item
-- MAGIC               from {temp_table_name}
-- MAGIC               """.format(temp_table_name=temp_table_name, i=i))

-- COMMAND ----------

select * from jay_mehta_catalog.sas.ch_nac_rev

-- COMMAND ----------

select * from jay_mehta_catalog.sas.ch_nac_0

-- COMMAND ----------

select * from jay_mehta_catalog.sas.ch_nac_1

-- COMMAND ----------

select * from jay_mehta_catalog.sas.ch_nac_2

-- COMMAND ----------

select * from jay_mehta_catalog.sas.ch_nac_rev