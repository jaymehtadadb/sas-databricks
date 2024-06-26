-- Databricks notebook source
-- MAGIC %md
-- MAGIC     data _null_;
-- MAGIC        %global today;
-- MAGIC        tod_str = compress(put(dateTIME(),datetime14.), ':');
-- MAGIC        call symput('today', compress(tod_str));
-- MAGIC        
-- MAGIC     run;
-- MAGIC     
-- MAGIC     %put &today;
-- MAGIC     
-- MAGIC     proc printto log="/sasbprshare/Auto_log/Homes_Passed_Daily/HOMES_PASSED_DAILY_REPORT_&today..log" new;
-- MAGIC     run; 
-- MAGIC     
-- MAGIC     
-- MAGIC     	%let datetime_start = %sysfunc(TIME()) ;
-- MAGIC     	  %put START TIME: %sysfunc(datetime(),datetime14.);
-- MAGIC     
-- MAGIC     
-- MAGIC     
-- MAGIC     filename pwfile '/sasbprshare/RHEA/pd.txt';
-- MAGIC     data _null_;
-- MAGIC     infile pwfile truncover;
-- MAGIC     input line :$200.;
-- MAGIC     call symputx('odspass',line);
-- MAGIC     run;
-- MAGIC     
-- MAGIC     
-- MAGIC     
-- MAGIC     options compress=binary;
-- MAGIC     %let USER = XXXXXXX;      
-- MAGIC     %let pwd = &odspass;
-- MAGIC     
-- MAGIC     %let PATHODIN = exaodin;
-- MAGIC     %put &USER; 
-- MAGIC     %put &pwd; 
-- MAGIC     %put &pathODIN;
-- MAGIC     
-- MAGIC     options compress=yes reuse=yes;run;
-- MAGIC     
-- MAGIC     libname VZ '/app/sasdata/bpr/data/VZ/';
-- MAGIC     libname dup '/app/sasdata/bpr/data/Data/Test/daily_dup';
-- MAGIC     libname my_lib    '/app/sasdata/bpr/data/AnaR';
-- MAGIC     libname bprshare '/sasdatagen02/bpr/';
-- MAGIC     libname CHRIS '/app/sasdata/bpr/data/Data/Dup_HP_with_Penetration'; 
-- MAGIC     libname MARY '/app/sasdata/bpr/data/Data/Test';
-- MAGIC     libname abc '/sasbprshare/input/';
-- MAGIC     libname ods_ss oracle user="&USER" password="&pwd" path=EXAODIN schema=ods_ss;
-- MAGIC     /*
-- MAGIC     libname APP_IBRO oracle user="&USER"  password="&pwd" path="mdcocdm-scan.rci.rogers.com:1531/BDWMSPRD_SN" schema=APP_IBRO;
-- MAGIC     libname ENT oracle user="&USER"  password="&pwd" path="mdcocdm-scan.rci.rogers.com:1531/BDWMSPRD_SN" schema=ENTERPRISE;
-- MAGIC     */
-- MAGIC     libname APP_IBRO oracle user="&user"  password="&pwd" path="exa009mdc-scan2.rci.rogers.com:1531/BDWPROD_SN" schema=APP_IBRO;
-- MAGIC     libname ENT oracle user="&user"  password="&pwd" path="exa009mdc-scan2.rci.rogers.com:1531/BDWPROD_SN" schema=ENTERPRISE;
-- MAGIC     libname MARQUEE oracle user="&user"  password="&pwd" path="exa009mdc-scan2.rci.rogers.com:1531/BDWPROD_SN" schema=MARQUEE;
-- MAGIC     libname M_COS oracle user="&user"  password="&pwd" path="exa009mdc-scan2.rci.rogers.com:1531/BDWPROD_SN" schema=MARQUEE_COS;
-- MAGIC     
-- MAGIC     libname HPMain '/app/sasdata/bpr/data/Data/Home_Passed';
-- MAGIC     libname anar '/app/sasdata/bpr/data/AnaR';
-- MAGIC     libname Regional '/sasdatamrkt/data/Ali/Regional'; 
-- MAGIC     libname SAL_HP '/app/sasdata/bpr/data/bpr/Salman/HP';
-- MAGIC     /*
-- MAGIC     libname MARQUEE oracle user="&USER"  password="&pwd" path="mdcocdm-scan.rci.rogers.com:1531/BDWMSPRD_SN" schema=MARQUEE;
-- MAGIC     libname M_COS oracle user="&USER"  password="&pwd" path="mdcocdm-scan.rci.rogers.com:1531/BDWMSPRD_SN" schema=MARQUEE_COS;
-- MAGIC     */
-- MAGIC     
-- MAGIC     libname sandbox oracle user="FIN_BI"  password="sndbx_04_FIN_BI#"  path="exa009mdc-scan1.rci.rogers.com:1531/SNDBXPRD" schema=FIN_BI PRESERVE_COL_NAMES=YES; 
-- MAGIC     libname Dhruv '/app/sasdata/bpr/Dhruv';
-- MAGIC     libname CP '//sasdata04/prd/data/geodata';
-- MAGIC     libname Daily_HH    '/app/sasdata/bpr/data/bpr/Daily_HH';
-- MAGIC     libname CPTM oracle  user="&USER"  password="&pwd" path=BDWPROD  schema=CPTM;
-- MAGIC     libname Daily_HH    '/app/sasdata/bpr/data/bpr/Daily_HH';
-- MAGIC     
-- MAGIC     
-- MAGIC     /************************************************************************************/
-- MAGIC     /******************************SET UP DAILY PARAMETERS*******************************/
-- MAGIC     /************************************************************************************/
-- MAGIC     
-- MAGIC     %let I=0; %PUT &I ;
-- MAGIC     
-- MAGIC     data _null_;
-- MAGIC     	CALL SYMPUT('CURR_D',PUT(intnx('DAY',today(),-&I.-0,"SAME"),ddmmyyd10.));
-- MAGIC     	CALL SYMPUT('PREV_D',PUT(intnx('DAY',today(),-&I.-1,"SAME"),ddmmyyd10.));
-- MAGIC     	CALL SYMPUT('PROCESS_DAY',PUT(DHMS(intnx('DAY',today(),-&I.-1,"SAME"),0,0,0),DATETIME20.));
-- MAGIC     	CALL SYMPUT('me_date',PUT(DHMS(intnx('DAY',today()-1,0-&I.,"SAME"),0,0,0),DATETIME20.));
-- MAGIC     	CALL SYMPUT('me_date1',PUT(DHMS(intnx('DAY',today()-1,0-&I.,"SAME"),23,59,59),DATETIME20.));
-- MAGIC     	CALL SYMPUT('FILE_DATE',PUT(intnx('DAY',today()-1,-&I.,"SAME"),date9.));
-- MAGIC     	CALL SYMPUT('CUR_DATE_NOW',PUT(intnx('month',today()-1-&I.,0,"SAME"),date11.));
-- MAGIC     	CALL SYMPUT('monthend_date',PUT(DHMS(intnx('month',today()-1,0-&I.,"ending"),0,0,0),DATETIME20.));
-- MAGIC     	CALL SYMPUT('lastmonthend_date',PUT(DHMS(intnx('month',today()-1,-1-&I.,"ending"),0,0,0),DATETIME20.));
-- MAGIC     run;
-- MAGIC     
-- MAGIC     %put  &CURR_D &PREV_D &PROCESS_DAY &me_date &me_date1 &FILE_DATE &CUR_DATE_NOW &monthend_date &lastmonthend_date;
-- MAGIC     
-- MAGIC     data g;
-- MAGIC     set CPTM.VW_TABLE_LOAD_STATUS;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     create table table_status as
-- MAGIC     select *
-- MAGIC     from CPTM.VW_TABLE_LOAD_STATUS
-- MAGIC     where 
-- MAGIC     SCHEMA_NAME in
-- MAGIC     ( 'APP_IBRO', 'ENTERPRISE'
-- MAGIC     )
-- MAGIC     and 
-- MAGIC     TABLE_NAME in
-- MAGIC     ('IBRO_HOUSEHOLD_CLOSING', 'LOCATION_DIM'
-- MAGIC     )
-- MAGIC     ;
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     
-- MAGIC     *****checking CPTM.VW_TABLE_LOAD_STATUS *****************;
-- MAGIC     proc sql noprint;
-- MAGIC      select count(*) into :tb_load 
-- MAGIC      from table_status;
-- MAGIC      quit;
-- MAGIC     
-- MAGIC     %put &tb_load.; 
-- MAGIC     
-- MAGIC     ****************;
-- MAGIC     
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     create table TABLE_STATUS_1 as
-- MAGIC     select 
-- MAGIC     count(*) as not_started
-- MAGIC     from TABLE_STATUS 
-- MAGIC     where STATUS ne 'LOADED'
-- MAGIC     ;quit;
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     create table Latest_Report_Date_dup as
-- MAGIC     select distinct max(datepart(process_date)) as Latest_Report_Date format=date11.
-- MAGIC     from dup.Dup_final;
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     create table dup_date_diff as
-- MAGIC     select (Latest_Report_Date-"&CUR_DATE_NOW"d)*-1 as not_started /*changed - Larissa */
-- MAGIC     from Latest_Report_Date_dup;
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     data TABLE_STATUS_2;
-- MAGIC     set TABLE_STATUS_1 dup_date_diff;run;
-- MAGIC     
-- MAGIC     PROC SQL;
-- MAGIC     CREATE TABLE TABLE_STATUS_3 AS SELECT SUM(not_started) AS not_started FROM TABLE_STATUS_2;QUIT;
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     select not_started into : v
-- MAGIC     from TABLE_STATUS_3
-- MAGIC     ;
-- MAGIC     quit;
-- MAGIC     %put &v;
-- MAGIC     
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     create table Latest_Report_Date as
-- MAGIC     select distinct max(datepart(process_date)) as Latest_Report_Date format=date9.
-- MAGIC     from SANDBOX.HOMES_PASSED_DAILY;
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     create table Table_Loaded_Date as
-- MAGIC     select distinct max(report_date) as Table_Loaded_Date
-- MAGIC     from app_ibro.ibro_household_closing
-- MAGIC     /*where report_date in ( "&CUR_DATE_NOW."d, "&CUR_DATE_NOW."d-1)*/
-- MAGIC     where report_date = "&CUR_DATE_NOW."d
-- MAGIC     ;quit;
-- MAGIC     
-- MAGIC     
-- MAGIC     data compare;
-- MAGIC     merge Table_Loaded_Date(in=a) Latest_Report_Date(in=b);
-- MAGIC     data_gap = datepart(Table_loaded_Date) - Latest_Report_Date;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     select data_gap into : date
-- MAGIC     from compare
-- MAGIC     ;
-- MAGIC     quit;
-- MAGIC     %put &date;
-- MAGIC     /*if date = 0 then no need to run*/
-- MAGIC     
-- MAGIC     options symbolgen;
-- MAGIC     %macro SET_GO;
-- MAGIC     %global GO;
-- MAGIC      %if &tb_load. = 0 %then %let GO=*;
-- MAGIC         %else %if &v ne 0 %then %let GO=*;
-- MAGIC        %else %if &v = 0 and &date = 0 %then %let GO=*;
-- MAGIC        %else %if &date = . %then %let GO = *;
-- MAGIC        %else %if &tb_load. ne 0 and &v = 0 and &date ne 0 %then %let Go =;
-- MAGIC        %else %let Go = *;
-- MAGIC     
-- MAGIC     %mend SET_GO;
-- MAGIC     %SET_GO;
-- MAGIC     
-- MAGIC     
-- MAGIC     /*==============*/
-- MAGIC     %macro go_date;
-- MAGIC     
-- MAGIC     /*Homes Passed Script Starting*/
-- MAGIC     filename mymail51 email ("roy.wang@rci.rogers.com","Richard.Lin@rci.rogers.com")
-- MAGIC        subject="Homes Passed Daily STARTED";
-- MAGIC     options emailsys=smtp emailhost= smtprelay.rci.rogers.com emailport=25;
-- MAGIC     
-- MAGIC     data _null_;
-- MAGIC        file mymail51;
-- MAGIC        put 'Homes Passed Daily has started';
-- MAGIC        put 'the summary can be used to identify any issues';
-- MAGIC        put ':)';
-- MAGIC     run;
-- MAGIC     
-- MAGIC     proc sql; 
-- MAGIC     connect to oracle as myconn (user="&user"  password="&pwd" path="BDWPROD");
-- MAGIC     create table HP_TODAY as select * from connection to myconn 
-- MAGIC     (SELECT * FROM ( select * from (
-- MAGIC     SELECT lo.*, 
-- MAGIC     CASE WHEN CONTRACT_GROUP_CODE IN('1','4') THEN 1
-- MAGIC          when contract_type_code = '2N0' then 1
-- MAGIC          WHEN NUM_OF_SUITES IS NULL THEN 1 
-- MAGIC          WHEN NUM_OF_SUITES='00000' AND CONTRACT_GROUP_CODE<>'5' THEN 1
-- MAGIC       ELSE COALESCE(CASE WHEN regexp_like(NUM_OF_SUITES,'^[0-9]+$') THEN TO_NUMBER(NUM_OF_SUITES)
-- MAGIC                 ELSE NULL END ,0) END HOMES_PASSED_COUNT
-- MAGIC       ,ROW_NUMBER()OVER (PARTITION BY LOCATION_ID ORDER BY LOCATION_SOURCE, eff_date DESC) RNK
-- MAGIC     FROM ENTERPRISE.LOCATION_DIM lo
-- MAGIC     WHERE /*crnt_IND='Y'*/ eff_date <= To_Date (%bquote('&CURR_D. 23:59') , 'DD-MM-YYYY hh24:mi') 
-- MAGIC     and end_date>= To_Date (%bquote('&CURR_D. 00:00'), 'DD-MM-YYYY hh24:mi') 
-- MAGIC     and CONTRACT_TYPE_CODE not in ('3J1','3P0') and CONTRACT_GROUP_CODE ^= '3'
-- MAGIC     /* Proximity overlap */
-- MAGIC     AND LOCATION_SOURCE in ( 'SS') 
-- MAGIC     ) WHERE RNK=1) where homes_passed_flg = 'P' and HOMES_PASSED_COUNT>=1 and (dummy_addr_ind <> 'Y')
-- MAGIC     );DISCONNECT FROM MYCONN; QUIT;
-- MAGIC     
-- MAGIC     proc sql; 
-- MAGIC     connect to oracle as myconn (user="&user"  password="&pwd" path="BDWPROD");
-- MAGIC     create table HP_TODAY_FWA as select * from connection to myconn 
-- MAGIC     (SELECT * FROM ( select * from (
-- MAGIC     SELECT lo.*, 
-- MAGIC     CASE 
-- MAGIC     /*WHEN CONTRACT_GROUP_CODE IN('1','4') THEN 1*/
-- MAGIC     /*     when contract_type_code = '2N0' then 1*/
-- MAGIC          WHEN NUM_OF_SUITES IS NULL THEN 1 
-- MAGIC          WHEN NUM_OF_SUITES='00000' AND CONTRACT_GROUP_CODE<>'5' THEN 1
-- MAGIC       ELSE COALESCE(CASE WHEN regexp_like(NUM_OF_SUITES,'^[0-9]+$') THEN TO_NUMBER(NUM_OF_SUITES)
-- MAGIC                 ELSE NULL END ,0) END HOMES_PASSED_COUNT
-- MAGIC       ,ROW_NUMBER()OVER (PARTITION BY LOCATION_ID ORDER BY LOCATION_SOURCE, eff_date DESC) RNK
-- MAGIC     FROM ENTERPRISE.LOCATION_DIM lo
-- MAGIC     WHERE /*crnt_IND='Y'*/ eff_date <= To_Date (%bquote('&CURR_D. 23:59') , 'DD-MM-YYYY hh24:mi') 
-- MAGIC     and end_date>= To_Date (%bquote('&CURR_D. 00:00'), 'DD-MM-YYYY hh24:mi')
-- MAGIC     /*and CONTRACT_TYPE_CODE not in ('3J1')*/
-- MAGIC     AND LOCATION_SOURCE in ( 'FWA', 'FWACBU', 'FWASEA', 'FWARWV', 'FWAKWIC', 'WHIXCOUNTRY', 'WHIMNLDT','WHICRAVE'	)
-- MAGIC     ) WHERE RNK=1) where homes_passed_flg = 'P' and HOMES_PASSED_COUNT>=1
-- MAGIC     /*and (dummy_addr_ind <> 'Y')*/
-- MAGIC     );DISCONNECT FROM MYCONN; QUIT;
-- MAGIC     
-- MAGIC     
-- MAGIC     /*PROC SQL; UPDATE HP_TODAY set HOMES_PASSED_DUP_FLG='N' WHERE HOMES_PASSED_DUP_FLG='Y';*/
-- MAGIC     PROC SQL; UPDATE HP_TODAY set HOMES_PASSED_DUP_FLG='N';
-- MAGIC     UPDATE HP_TODAY set HOMES_PASSED_DUP_FLG='Y' WHERE SAM_KEY IN (SELECT SAM_KEY FROM DUP.Dup_final WHERE PROCESS_DATE="&PROCESS_DAY"DT)
-- MAGIC     ;QUIT;
-- MAGIC     
-- MAGIC     
-- MAGIC     /*PROC SQL;*/
-- MAGIC     /*UPDATE HP_TODAY set HOMES_PASSED_DUP_FLG='Y' WHERE SAM_KEY IN (SELECT SAM_KEY FROM sandbox.IBRO_HP_EXCLUDED_V1 );*/
-- MAGIC     /*quit;*/
-- MAGIC     
-- MAGIC     data HP_TODAY;
-- MAGIC     set HP_TODAY HP_TODAY_FWA;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     
-- MAGIC     PROC SQL;
-- MAGIC     /*SELECT SUM(HOMES_PASSED_COUNT) FROM NOW;*/
-- MAGIC     SELECT SUM(HOMES_PASSED_COUNT) as HP_today FROM HP_TODAY;
-- MAGIC     SELECT SUM(HOMES_PASSED_COUNT) as HP_today FROM HP_TODAY where HOMES_PASSED_DUP_FLG='N';
-- MAGIC     SELECT SUM(HOMES_PASSED_COUNT) as saved_table FROM SAL_HP.DAILY_HOMES_PASSED_BASE;
-- MAGIC     QUIT;
-- MAGIC     
-- MAGIC     proc sql; create table region as 
-- MAGIC     select distinct POSTAL_CODE, FSA, upcase(region_segment_cd) AS NEW_REGION, upcase(region_segment_nm) AS NEW_SUBREGION
-- MAGIC     from anar.Regions_V2;quit;
-- MAGIC     
-- MAGIC     proc sort data=region nodupkey; by POSTAL_CODE;run; 
-- MAGIC     
-- MAGIC     proc sql; create table HP_TODAY2 as 
-- MAGIC     SELECT "&PROCESS_DAY."DT AS PROCESS_DATE FORMAT=DATETIME20., a.*,B.NEW_REGION,B.NEW_SUBREGION,
-- MAGIC     CASE WHEN CONTRACT_GROUP_CODE='1' THEN 'SFU'
-- MAGIC     	WHEN CONTRACT_GROUP_CODE in ('2','3') THEN 'BULK'
-- MAGIC     	WHEN CONTRACT_GROUP_CODE='4' THEN 'MDU'
-- MAGIC     	ELSE 'COMMERCIAL' END AS HP_SEGMENT
-- MAGIC     from HP_TODAY a left join region b on a.postal_code=b.postal_code;
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     data HP_TODAY2;
-- MAGIC     set HP_TODAY2;
-- MAGIC     if LOCATION_SOURCE in ( 'FWA', 'FWACBU') then do; NEW_REGION = 'SWO' ; NEW_SUBREGION = 'GREATER HAMILTON AREA'  ; HOMES_PASSED_DUP_FLG = 'E'; end;
-- MAGIC     if LOCATION_SOURCE in ( 'FWARWV') then do; NEW_REGION = 'GTA' ; NEW_SUBREGION = 'NEWMARKET'  ;HOMES_PASSED_DUP_FLG = 'E'; end;
-- MAGIC     if LOCATION_SOURCE in ( 'FWASEA', 'WHIXCOUNTRY', 'WHIMNLDT' ) then do; NEW_REGION = 'NS' ; NEW_SUBREGION = 'NOVA SCOTIA'   ; HOMES_PASSED_DUP_FLG = 'E'; end;
-- MAGIC     if LOCATION_SOURCE in ( 'FWAKWIC') then do; NEW_REGION = 'SWO' ; NEW_SUBREGION = 'BRANTFORD'   ; HOMES_PASSED_DUP_FLG = 'E'; end;
-- MAGIC     if LOCATION_SOURCE in ( 'WHICRAVE') then do; NEW_REGION = 'NB' ; NEW_SUBREGION = 'SAINT JOHN'   ; HOMES_PASSED_DUP_FLG = 'E'; end;
-- MAGIC     if missing(HOMES_PASSED_DUP_FLG) eq 1 then HOMES_PASSED_DUP_FLG = 'E';
-- MAGIC     run;
-- MAGIC     
-- MAGIC     /************serviceability T logic*************/;
-- MAGIC     data HP_TODAY3 serviceability_T_HP;
-- MAGIC     set HP_TODAY2;
-- MAGIC     if serviceability_code eq 'T' then output serviceability_T_HP;
-- MAGIC     else output HP_TODAY3;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     data serviceability_T_HP;
-- MAGIC     set serviceability_T_HP;
-- MAGIC     format PRODUCT $3.;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     /***Get MS WHI subs*************/;
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     create table MS_WHI_product_codes as 
-- MAGIC     select * from  ent.PRODUCT_HIERARCHY  a where LEVEL_4_ID in ('141291')
-- MAGIC      and crnt_ind = 'Y'
-- MAGIC      and REVENUE_TSU = 'Y';
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     create table MS_FWA as 
-- MAGIC     select * from APP_IBRO.ibro_Subscriber_activity_hist
-- MAGIC     where activity_date eq "&FILE_DATE."d
-- MAGIC     and product_lob_unit in ( 'Y')
-- MAGIC     and product_code in (select LEVEL_5_PRODUCT_CODE from MS_WHI_product_codes)
-- MAGIC     and activity = 'CL'
-- MAGIC     order by customer_location_id;
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     /***Get SHM subs*************/
-- MAGIC     proc sql;
-- MAGIC     create table SHM_BASE as 
-- MAGIC     select * from APP_IBRO.ibro_Subscriber_activity_hist
-- MAGIC     where activity_date eq "&FILE_DATE."d
-- MAGIC     and product_lob_unit in ( 'Y')
-- MAGIC     and product_lob in ('500')
-- MAGIC     and activity = 'CL'
-- MAGIC     order by customer_location_id;
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     update serviceability_T_HP 
-- MAGIC      set PRODUCT = 'SHM'
-- MAGIC     where sam_key in (select customer_location_id from SHM_BASE);
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     update serviceability_T_HP 
-- MAGIC      set PRODUCT = 'WHI'
-- MAGIC     where sam_key in (select customer_location_id from MS_FWA);
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     data serviceability_T_HP2 (keep=PROCESS_DATE SAM_KEY POSTAL_CODE CONTRACT_GROUP_CODE HOMES_PASSED_DUP_FLG HOMES_PASSED_COUNT serviceability_code NEW_REGION NEW_SUBREGION PRODUCT);
-- MAGIC     set serviceability_T_HP;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     /****************Serv T history file **********************/
-- MAGIC     proc sql;
-- MAGIC     delete from SAL_HP.DAILY_SERV_T_BASE
-- MAGIC     where PROCESS_DATE="&PROCESS_DAY."DT;
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     proc append data=serviceability_T_HP2 base=SAL_HP.DAILY_SERV_T_BASE;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     PROC DELETE DATA=SANDBOX.DAILY_SERV_T_BASE; RUN;
-- MAGIC     
-- MAGIC     DATA SANDBOX.DAILY_SERV_T_BASE;
-- MAGIC     SET SAL_HP.DAILY_SERV_T_BASE;
-- MAGIC     RUN;
-- MAGIC     
-- MAGIC     /*************addn MS WHI HP back to HP base including dups , chnage dup plag to N**********************************/
-- MAGIC     data MS_FWA_HP;
-- MAGIC     set serviceability_T_HP;
-- MAGIC     where PRODUCT = 'WHI';
-- MAGIC     HOMES_PASSED_DUP_FLG = 'N';
-- MAGIC     run;
-- MAGIC     
-- MAGIC     data HP_TODAY4;
-- MAGIC     set HP_TODAY3 MS_FWA_HP;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     /***********Summary*************/;
-- MAGIC     
-- MAGIC     PROC SQL;
-- MAGIC     CREATE TABLE HP_TODAY_SUMMARY AS
-- MAGIC     SELECT PROCESS_DATE,
-- MAGIC     "HP_BASE" as SOURCE,
-- MAGIC     /*"EOP" AS METRIC, */
-- MAGIC     CONTRACT_GROUP_CODE,
-- MAGIC     HOMES_PASSED_DUP_FLG,
-- MAGIC     NEW_REGION,
-- MAGIC     NEW_SUBREGION,
-- MAGIC     HP_SEGMENT,
-- MAGIC     CASE WHEN CONTRACT_GROUP_CODE IN ('1','4') THEN 'CONNECTED HOME'
-- MAGIC      WHEN CONTRACT_GROUP_CODE IN ('5') THEN 'R4B'
-- MAGIC 	 ELSE HP_SEGMENT END AS FINAL_SEGMENT,
-- MAGIC     SUM(HOMES_PASSED_COUNT) AS HOMES_PASSED_COUNT
-- MAGIC     FROM HP_TODAY4
-- MAGIC     GROUP BY 1,2,3,4,5,6,7,8
-- MAGIC     ;QUIT;
-- MAGIC     
-- MAGIC     /**********Add external**************/
-- MAGIC     proc sql; 
-- MAGIC     /* cable cable */
-- MAGIC     insert into HP_TODAY_SUMMARY  values ("&PROCESS_DAY."DT,'HP_BASE','1'	,'E', 
-- MAGIC     'OTT',	'RURAL OTTAWA','SFU', 'CONNECTED HOME',  12536  );
-- MAGIC     /* Seaside & XCOUNTRY & Mainland*/
-- MAGIC     insert into HP_TODAY_SUMMARY  values ("&PROCESS_DAY."DT,'HP_BASE','1'	,'E', 
-- MAGIC     'NS', 'NOVA SCOTIA','SFU', 'CONNECTED HOME',  30536 );
-- MAGIC     /* Proximity Crave */
-- MAGIC     insert into HP_TODAY_SUMMARY  values ("&PROCESS_DAY."DT,'HP_BASE','1'	,'E', 
-- MAGIC     'NB', 'SAiNT JOHN','SFU', 'CONNECTED HOME',  1026 );
-- MAGIC     /* Proximity Crave */
-- MAGIC     insert into HP_TODAY_SUMMARY  values ("&PROCESS_DAY."DT,'HP_BASE','1'	,'E', 
-- MAGIC     'SWO', 'GUELPH','SFU', 'CONNECTED HOME',  83 );
-- MAGIC     QUIT;
-- MAGIC     
-- MAGIC     /*Ruralwave, KWIC */
-- MAGIC     data HP_TODAY_SUMMARY;
-- MAGIC     set HP_TODAY_SUMMARY;
-- MAGIC     if HOMES_PASSED_DUP_FLG eq 'E' and NEW_SUBREGION eq 'NEWMARKET' then HOMES_PASSED_COUNT = HOMES_PASSED_COUNT + 5210;
-- MAGIC     if HOMES_PASSED_DUP_FLG eq 'E' and NEW_SUBREGION eq 'BRANTFORD' then HOMES_PASSED_COUNT = HOMES_PASSED_COUNT + 2940;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     SELECT HOMES_PASSED_DUP_FLG,SUM(HOMES_PASSED_COUNT) as HP_today FROM HP_TODAY_SUMMARY
-- MAGIC     group by HOMES_PASSED_DUP_FLG;
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     
-- MAGIC     DATA OTHER_DATE_HP;
-- MAGIC     SET SAL_HP.DAILY_HOMES_PASSED_BASE;
-- MAGIC     IF PROCESS_DATE^="&PROCESS_DAY."DT;RUN;
-- MAGIC     
-- MAGIC     
-- MAGIC     DATA SAL_HP.DAILY_HOMES_PASSED_BASE;
-- MAGIC     SET OTHER_DATE_HP HP_TODAY_SUMMARY;RUN;
-- MAGIC     
-- MAGIC     proc sql; 
-- MAGIC     connect to oracle as myconn (user="&user"  password="&pwd" path="BDWPROD");
-- MAGIC     create table cr_bundle as select *  from connection to myconn (
-- MAGIC     select report_Date, customer_location_id, customer_account, customer_id, postal_code, substr(postal_code,1,3) as fsa, CONTRACT_GROUP_CODE, 
-- MAGIC     MULTI_SYSTEM, MULTI_BRAND, MULTI_SEGMENT, 
-- MAGIC     replace(replace(replace(MULTI_PRODUCT_NAME,'INT','BB'),'TVE','TV'),'TV-BB','BB-TV') AS MULTI_PRODUCT, sum(closing_hh) as CLOSING_HH
-- MAGIC     from app_ibro.ibro_household_closing 
-- MAGIC     where report_date = To_Date (%bquote('&CURR_D.'), 'DD-MM-YYYY')-1
-- MAGIC     and closing_hh <>0
-- MAGIC     GROUP BY 
-- MAGIC     report_Date, customer_location_id, customer_account, customer_id, postal_code, substr(postal_code,1,3) , 
-- MAGIC     replace(replace(replace(MULTI_PRODUCT_NAME,'INT','BB'),'TVE','TV'),'TV-BB','BB-TV'),CONTRACT_GROUP_CODE,
-- MAGIC     MULTI_SYSTEM, MULTI_BRAND, MULTI_SEGMENT
-- MAGIC     ) ; disconnect from myconn;
-- MAGIC     quit; 
-- MAGIC     
-- MAGIC     
-- MAGIC     proc sql; create table cr_bundle2 as 
-- MAGIC     SELECT "&PROCESS_DAY."DT AS PROCESS_DATE FORMAT=DATETIME20., a.*,B.NEW_REGION,B.NEW_SUBREGION,
-- MAGIC     CASE WHEN CONTRACT_GROUP_CODE='1' THEN 'SFU'
-- MAGIC     	WHEN CONTRACT_GROUP_CODE='5' THEN 'COMMERCIAL'
-- MAGIC     	WHEN CONTRACT_GROUP_CODE='4' THEN 'MDU'
-- MAGIC     	ELSE 'BULK' END AS HP_SEGMENT
-- MAGIC     from cr_bundle a left join region b on a.postal_code=b.postal_code;
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     
-- MAGIC     data cr_bundle2;
-- MAGIC     set cr_bundle2;
-- MAGIC     if MULTI_SYSTEM in ( 'FWASEA', 'SEA',  'WHIXCOUNTRY', 'XCOUNTRY', 'MNLDT', 'WHIMNLDT', 'WHICRAVE') then do;
-- MAGIC     NEW_REGION = 'NS';
-- MAGIC     NEW_SUBREGION = 'NOVA SCOTIA'; end;
-- MAGIC     
-- MAGIC     if MULTI_SYSTEM in ('FWAKWIC', 'KWIC') then do;
-- MAGIC     NEW_REGION = 'SWO';
-- MAGIC     NEW_SUBREGION = 'BRANTFORD'; end;
-- MAGIC     
-- MAGIC     if MULTI_SYSTEM in ('FWA', 'FWACBU', 'V21' )
-- MAGIC     or index(MULTI_SYSTEM,'SOURCE') >0  then do;
-- MAGIC     NEW_REGION = 'SWO';
-- MAGIC     NEW_SUBREGION = 'GREATER HAMILTON AREA'; end;
-- MAGIC     
-- MAGIC     if MULTI_SYSTEM in ('FWARWV' ) then do;
-- MAGIC     NEW_REGION = 'GTA';
-- MAGIC     NEW_SUBREGION = 'NEWMARKET'; end;
-- MAGIC     
-- MAGIC     if MULTI_SYSTEM in ('WHICRAVE' ) then do;
-- MAGIC     NEW_REGION = 'NB';
-- MAGIC     NEW_SUBREGION = 'SAINT JOHN'; end;
-- MAGIC     
-- MAGIC     if MULTI_SYSTEM in ('NETFLASH' ) then do;
-- MAGIC     NEW_REGION = 'SWO';
-- MAGIC     NEW_SUBREGION = 'GUELPH'; end;
-- MAGIC     
-- MAGIC     run;
-- MAGIC     
-- MAGIC     
-- MAGIC     PROC SQL;
-- MAGIC     CREATE TABLE HH_CLOSING_TD_SUMMARY AS
-- MAGIC     SELECT PROCESS_DATE,
-- MAGIC     "HH_CLOSING" as SOURCE,
-- MAGIC     /*"EOP" AS METRIC, */
-- MAGIC     CONTRACT_GROUP_CODE,
-- MAGIC     NEW_REGION,
-- MAGIC     NEW_SUBREGION,
-- MAGIC     HP_SEGMENT,
-- MAGIC     MULTI_SEGMENT,
-- MAGIC     CASE WHEN HP_SEGMENT IN ('SFU','MDU') THEN 'CONNECTED HOME'
-- MAGIC          WHEN HP_SEGMENT IN ('COMMERCIAL') THEN 'R4B'
-- MAGIC     	 ELSE HP_SEGMENT END AS FINAL_SEGMENT,
-- MAGIC     COALESCE(SUM(CLOSING_HH),0) AS CLOSING_HH
-- MAGIC     FROM cr_bundle2
-- MAGIC     GROUP BY 1,2,3,4,5,6,7,8
-- MAGIC     ;QUIT;
-- MAGIC     
-- MAGIC     PROC FREQ DATA=	HH_CLOSING_TD_SUMMARY; TABLE CONTRACT_GROUP_CODE*HP_SEGMENT*FINAL_SEGMENT /LIST MISSING; RUN;
-- MAGIC     
-- MAGIC     
-- MAGIC     DATA OTHER_DATE;
-- MAGIC     SET SAL_HP.DAILY_HOUSEHOLE_CLOSING;
-- MAGIC     IF PROCESS_DATE^="&PROCESS_DAY."DT;RUN;
-- MAGIC     
-- MAGIC     proc freq data=OTHER_DATE; table PROCESS_DATE; run;
-- MAGIC     
-- MAGIC     DATA SAL_HP.DAILY_HOUSEHOLE_CLOSING;
-- MAGIC     SET OTHER_DATE HH_CLOSING_TD_SUMMARY;RUN;
-- MAGIC     
-- MAGIC     proc freq data=SAL_HP.DAILY_HOUSEHOLE_CLOSING; table PROCESS_DATE; run;
-- MAGIC     
-- MAGIC     
-- MAGIC     PROC SQL;
-- MAGIC     CREATE TABLE HP_BASE_SUMMARY_OUTPUT AS
-- MAGIC     SELECT DATEPART(PROCESS_DATE) AS PROCESS_DATE FORMAT=DATE9.,
-- MAGIC     SOURCE,
-- MAGIC     HOMES_PASSED_DUP_FLG,
-- MAGIC     FINAL_SEGMENT,
-- MAGIC     SUM(HOMES_PASSED_COUNT) AS HOMES_PASSED_COUNT
-- MAGIC     FROM HP_TODAY_SUMMARY
-- MAGIC     WHERE HOMES_PASSED_DUP_FLG^='Y'
-- MAGIC     GROUP BY 1,2,3,4
-- MAGIC     ;QUIT;
-- MAGIC     
-- MAGIC     
-- MAGIC     PROC SQL;
-- MAGIC     CREATE TABLE HH_CLOSING_SUMMARY_OUTPUT AS
-- MAGIC     SELECT DATEPART(PROCESS_DATE) AS PROCESS_DATE FORMAT=DATE9.,
-- MAGIC     SOURCE,
-- MAGIC     FINAL_SEGMENT,
-- MAGIC     COALESCE(SUM(CLOSING_HH),0) AS CLOSING_HH
-- MAGIC     FROM HH_CLOSING_TD_SUMMARY
-- MAGIC     GROUP BY 1,2,3
-- MAGIC     ;QUIT;
-- MAGIC     
-- MAGIC     /*------------------------------------------------------------------------------------*/
-- MAGIC     /*GET RBS AND RBS DUP INFO FROM SANDBOX TABLE (THIS IS UPDATED MONTHLY)*/
-- MAGIC     /*------------------------------------------------------------------------------------*/
-- MAGIC     DATA RBS_IN_RECENT_2MTHS;
-- MAGIC     SET sal_hp.cr_rbs;
-- MAGIC     IF REPORT_DATE IN ("&monthend_date"dt, "&lastmonthend_date"dt);run;
-- MAGIC     
-- MAGIC     PROC SQL; SELECT MAX(REPORT_DATE) FORMAT=DATETIME20. INTO: MAX_DATE FROM RBS_IN_RECENT_2MTHS; QUIT;
-- MAGIC     
-- MAGIC     DATA MAX_ME_DATE;
-- MAGIC     SET RBS_IN_RECENT_2MTHS;
-- MAGIC     FORMAT TEMP_DATE DATETIME20.;
-- MAGIC     IF REPORT_DATE="&MAX_DATE"DT;
-- MAGIC     TEMP_DATE="&PROCESS_DAY."DT;
-- MAGIC     IF CONTRACT_GROUP_CODE='5' THEN HP_SEGMENT='COMMERCIAL';
-- MAGIC     ELSE IF CONTRACT_GROUP_CODE='1' THEN HP_SEGMENT='SFU';
-- MAGIC     ELSE IF CONTRACT_GROUP_CODE='4' THEN HP_SEGMENT='MDU';
-- MAGIC     ELSE HP_SEGMENT='BULK';
-- MAGIC     RUN;
-- MAGIC     
-- MAGIC     PROC SQL;
-- MAGIC     CREATE TABLE DAILY_DATE AS 
-- MAGIC     SELECT DISTINCT PROCESS_DATE FROM SAL_HP.DAILY_HOUSEHOLE_CLOSING 
-- MAGIC     WHERE YEAR(DATEPART(PROCESS_DATE))=YEAR(DATEPART("&PROCESS_DAY."DT)) AND MONTH(DATEPART(PROCESS_DATE))=MONTH(DATEPART("&PROCESS_DAY."DT));QUIT;
-- MAGIC     
-- MAGIC     PROC SQL;
-- MAGIC     CREATE TABLE POPULATE_RBS AS 
-- MAGIC     SELECT A.PROCESS_DATE, 
-- MAGIC     "HH_CLOSING" as SOURCE,
-- MAGIC     CONTRACT_GROUP_CODE,
-- MAGIC     NEW_REGION,
-- MAGIC     NEW_SUBREGION,
-- MAGIC     HP_SEGMENT,
-- MAGIC     MULTI_SEGMENT,
-- MAGIC     CASE WHEN HP_SEGMENT IN ('SFU','MDU') THEN 'CONNECTED HOME'
-- MAGIC          WHEN HP_SEGMENT IN ('COMMERCIAL') THEN 'R4B'
-- MAGIC     	 ELSE HP_SEGMENT END AS FINAL_SEGMENT,
-- MAGIC     COALESCE(SUM(CR_QTY),0) AS CLOSING_HH
-- MAGIC     FROM MAX_ME_DATE B
-- MAGIC     INNER JOIN DAILY_DATE A
-- MAGIC     ON YEAR(DATEPART(A.PROCESS_DATE))=YEAR(DATEPART(TEMP_DATE)) 
-- MAGIC     AND MONTH(DATEPART(A.PROCESS_DATE))=MONTH(DATEPART(TEMP_DATE))
-- MAGIC     GROUP BY 1,2,3,4,5,6,7,8;QUIT;
-- MAGIC     
-- MAGIC     
-- MAGIC     DATA OTHER_MONTH_RBS;
-- MAGIC     SET SAL_HP.DAILY_RBS_CLOSING;
-- MAGIC     IF NOT (YEAR(DATEPART(PROCESS_DATE))=YEAR(DATEPART("&PROCESS_DAY."DT)) 
-- MAGIC     AND MONTH(DATEPART(PROCESS_DATE))=MONTH(DATEPART("&PROCESS_DAY."DT)));RUN;
-- MAGIC     
-- MAGIC     DATA SAL_HP.DAILY_RBS_CLOSING;
-- MAGIC     SET OTHER_MONTH_RBS POPULATE_RBS;RUN;
-- MAGIC     
-- MAGIC     PROC DELETE DATA=SANDBOX.HOMES_PASSED_DAILY; RUN;
-- MAGIC     
-- MAGIC     DATA SANDBOX.HOMES_PASSED_DAILY;
-- MAGIC     SET SAL_HP.DAILY_HOUSEHOLE_CLOSING
-- MAGIC     	SAL_HP.DAILY_RBS_CLOSING
-- MAGIC     	SAL_HP.DAILY_HOMES_PASSED_BASE;RUN;
-- MAGIC     
-- MAGIC     PROC FREQ DATA=SANDBOX.HOMES_PASSED_DAILY; TABLE PROCESS_DATE*SOURCE*homes_passed_dup_flg /LIST MISSING; weight homes_passed_count; RUN;
-- MAGIC     
-- MAGIC     
-- MAGIC     /** new dashboard*/
-- MAGIC     data test_yesterday;
-- MAGIC     set SANDBOX.HOMES_PASSED_DAILY;
-- MAGIC         where datepart(process_date) = "&FILE_DATE"d - 1;
-- MAGIC     	if  SOURCE = 'HP_BASE' then BOP = HOMES_PASSED_COUNT;
-- MAGIC     	if  SOURCE = 'HH_CLOSING' then BOP = CLOSING_HH;
-- MAGIC      run;
-- MAGIC     
-- MAGIC      data test_today;
-- MAGIC      set SANDBOX.HOMES_PASSED_DAILY;
-- MAGIC       where process_date =   "&PROCESS_DAY."dt ;
-- MAGIC     	if  SOURCE = 'HP_BASE' then EOP = HOMES_PASSED_COUNT;
-- MAGIC     	if  SOURCE = 'HH_CLOSING' then EOP = CLOSING_HH;
-- MAGIC      run;
-- MAGIC     
-- MAGIC     data test_yesterday;
-- MAGIC     set test_yesterday;
-- MAGIC     process_date = "&PROCESS_DAY."DT;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     proc sort data=test_today ;
-- MAGIC       by process_date SOURCE CONTRACT_GROUP_CODE NEW_REGION NEW_SUBREGION HP_SEGMENT	MULTI_SEGMENT	FINAL_SEGMENT HOMES_PASSED_DUP_FLG;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     proc sort data=test_yesterday;
-- MAGIC       by process_date SOURCE CONTRACT_GROUP_CODE NEW_REGION NEW_SUBREGION HP_SEGMENT	MULTI_SEGMENT	FINAL_SEGMENT HOMES_PASSED_DUP_FLG;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     data test_today_2;
-- MAGIC       merge test_today test_yesterday;
-- MAGIC       by process_date SOURCE CONTRACT_GROUP_CODE NEW_REGION NEW_SUBREGION HP_SEGMENT	MULTI_SEGMENT	FINAL_SEGMENT HOMES_PASSED_DUP_FLG;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     
-- MAGIC     %macro zerodata ( data=&syslast ) ;
-- MAGIC           %let data = &data ;
-- MAGIC           %if %sysfunc(exist(&data)) %then %do ;
-- MAGIC              data &data ( drop = __i ) ;
-- MAGIC                 set &data ;
-- MAGIC                 array __nums (*) _numeric_ ;
-- MAGIC                 do __i = 1 to dim ( __nums ) ;
-- MAGIC                    if (__nums [__i]) = . then __nums[__i] = 0 ;
-- MAGIC                 end ;
-- MAGIC              run ;
-- MAGIC           %end ;
-- MAGIC           %else %put ERROR: (ZERODATA) dataset &data does not exist ;
-- MAGIC       %mend zerodata ;
-- MAGIC     
-- MAGIC     %zerodata (data=test_today_2)
-- MAGIC     ;
-- MAGIC     
-- MAGIC     data test_today_2;
-- MAGIC     set test_today_2;
-- MAGIC     NET = EOP - BOP;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     data EOP (drop=EOP HOMES_PASSED_COUNT CLOSING_HH);
-- MAGIC     set test_today;
-- MAGIC     category = 'EOP';
-- MAGIC     rename EOP = Summary;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     data BOP (drop=BOP HOMES_PASSED_COUNT CLOSING_HH);
-- MAGIC     set test_yesterday;
-- MAGIC     category = 'BOP';
-- MAGIC     rename BOP = Summary;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     data NET (drop=BOP EOP NET HOMES_PASSED_COUNT CLOSING_HH);
-- MAGIC     set test_today_2;
-- MAGIC     category = 'NET';
-- MAGIC     rename NET = Summary;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     data SUMMARY;
-- MAGIC     set EOP BOP NET;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     /* Manual fix for QUEBEC*/
-- MAGIC     proc sql;
-- MAGIC     update SUMMARY
-- MAGIC     set  NEW_SUBREGION = 'EDMUNDSTON', NEW_REGION  = 'NB'
-- MAGIC     where  NEW_SUBREGION = 'QUEBEC';
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     /* Manual fix for QUEBEC for address with wrong pc*/
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     update SUMMARY
-- MAGIC     set NEW_SUBREGION = "ST JOHN'S",  NEW_REGION = 'NL'
-- MAGIC     where  new_region = 'NT';
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     delete  from SAL_HP.DAILY_HP_HH_RBS
-- MAGIC     where process_date =   "&PROCESS_DAY."dt;
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     data SAL_HP.DAILY_HP_HH_RBS;
-- MAGIC     set SAL_HP.DAILY_HP_HH_RBS SUMMARY;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     
-- MAGIC     PROC DELETE DATA=SANDBOX.HOMES_PASSED_DAILY_NEW; RUN;
-- MAGIC     
-- MAGIC     DATA SANDBOX.HOMES_PASSED_DAILY_NEW;
-- MAGIC     SET SAL_HP.DAILY_HP_HH_RBS;
-- MAGIC     RUN;
-- MAGIC     
-- MAGIC     /*==============================================*/
-- MAGIC     
-- MAGIC     *****************************New Adds Removals logic******************************;
-- MAGIC     *********************************************************************************;
-- MAGIC     proc sql; 
-- MAGIC     connect to oracle as myconn (user="&user"  password="&pwd" path="BDWPROD");
-- MAGIC     create table HP_YESTERDAY as select * from connection to myconn 
-- MAGIC     (SELECT * FROM ( select * from (
-- MAGIC     SELECT lo.*, 
-- MAGIC     CASE WHEN CONTRACT_GROUP_CODE IN('1','4') THEN 1
-- MAGIC          when contract_type_code = '2N0' then 1
-- MAGIC          WHEN NUM_OF_SUITES IS NULL THEN 1 
-- MAGIC          WHEN NUM_OF_SUITES='00000' AND CONTRACT_GROUP_CODE<>'5' THEN 1
-- MAGIC       ELSE COALESCE(CASE WHEN regexp_like(NUM_OF_SUITES,'^[0-9]+$') THEN TO_NUMBER(NUM_OF_SUITES)
-- MAGIC                 ELSE NULL END ,0) END HOMES_PASSED_COUNT
-- MAGIC       ,ROW_NUMBER()OVER (PARTITION BY LOCATION_ID ORDER BY LOCATION_SOURCE, eff_date DESC) RNK
-- MAGIC     FROM ENTERPRISE.LOCATION_DIM lo
-- MAGIC     WHERE /*crnt_IND='Y'*/ eff_date <= To_Date (%bquote('&PREV_D. 23:59') , 'DD-MM-YYYY hh24:mi') 
-- MAGIC     and end_date>= To_Date (%bquote('&PREV_D. 00:00'), 'DD-MM-YYYY hh24:mi') and CONTRACT_TYPE_CODE not in ('3J1','3P0') and CONTRACT_GROUP_CODE ^= '3'
-- MAGIC     AND LOCATION_SOURCE  = 'SS' ) WHERE RNK=1) where homes_passed_flg = 'P' and HOMES_PASSED_COUNT>=1 and (dummy_addr_ind <> 'Y')
-- MAGIC     order by sam_key
-- MAGIC     );DISCONNECT FROM MYCONN; QUIT;
-- MAGIC     
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     create table serv_t_SHM as 
-- MAGIC     select * from SAL_HP.DAILY_SERV_T_BASE where datepart(process_date)  =  "&FILE_DATE."d -1
-- MAGIC     and PRODUCT ne 'WHI' ;
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     delete from HP_YESTERDAY where sam_key in (select sam_key from serv_t_SHM);
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     proc sort data=HP_TODAY4 ;
-- MAGIC     by sam_key;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     
-- MAGIC     data adds removals;
-- MAGIC     merge HP_TODAY4(in=td) HP_YESTERDAY (in=ytd);
-- MAGIC     by sam_key;
-- MAGIC     if td and not ytd then output adds;
-- MAGIC     if  ytd and not td then output removals;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     
-- MAGIC     data Daily_HH.HP_ADDS 
-- MAGIC     (keep=Report_Date sam_key BULK_PROFILE_NAME CONTRACT_GROUP_CODE CONTRACT_TYPE_CODE HOMES_PASSED_COUNT SERVICEABILITY_CODE CABLE_TYPE
-- MAGIC     RPATS_NUMBER RPATS_DESC PROJ_TYPE_IND  FIRST_FLIP_DATE HOMES_PASSED_DATE EFF_DATE  END_DATE 
-- MAGIC     street_number  street_name STREET_TYPE COMPASS_DIR_CODE APARTMENT_NUMBER CITY_NAME DWELLING_TYPE_CODE postal_code Address_Key  NEW_ADDRESS_KEY);
-- MAGIC     Report_Date = "&CUR_DATE_NOW.";
-- MAGIC     set adds;
-- MAGIC     where substr(sam_key,1,1) ne '-';
-- MAGIC     NEW_ADDRESS_KEY = company||' '||trim(street_number)||' '||trim(street_name)||' '||trim(STREET_TYPE)||' '||postal_code;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     
-- MAGIC     data Daily_HH.HP_REMOVALS 
-- MAGIC     (keep=Report_Date sam_key BULK_PROFILE_NAME CONTRACT_GROUP_CODE CONTRACT_TYPE_CODE HOMES_PASSED_COUNT SERVICEABILITY_CODE CABLE_TYPE
-- MAGIC     RPATS_NUMBER RPATS_DESC PROJ_TYPE_IND  FIRST_FLIP_DATE HOMES_PASSED_DATE EFF_DATE  END_DATE 
-- MAGIC     street_number  street_name STREET_TYPE COMPASS_DIR_CODE APARTMENT_NUMBER CITY_NAME DWELLING_TYPE_CODE postal_code Address_Key  NEW_ADDRESS_KEY);
-- MAGIC     Report_Date = "&CUR_DATE_NOW.";
-- MAGIC     set removals;
-- MAGIC     NEW_ADDRESS_KEY = company||' '||trim(street_number)||' '||trim(street_name)||' '||trim(STREET_TYPE)||' '||postal_code;
-- MAGIC     run;
-- MAGIC     
-- MAGIC     proc sql;
-- MAGIC     create table  Daily_HH.dups_last30_days as
-- MAGIC     select process_date, sum(homes_passed_count) as DUp_HP_count from DUP.Dup_final
-- MAGIC     where datepart(process_date) GE "&FILE_DATE."d - 30
-- MAGIC     group by process_date
-- MAGIC     order by process_date;
-- MAGIC     quit;
-- MAGIC     
-- MAGIC     
-- MAGIC     
-- MAGIC     PROC EXPORT DATA= Daily_HH.HP_ADDS
-- MAGIC                 OUTFILE= "/app/sasdata/bpr/data/bpr/Daily_HH/HP_ADDS_REMOVALS_DAILY.xls"
-- MAGIC                 DBMS=xls REPLACE;
-- MAGIC     			SHEET="HP_ADDS";
-- MAGIC     RUN;
-- MAGIC     
-- MAGIC     
-- MAGIC     PROC EXPORT DATA= Daily_HH.HP_REMOVALS
-- MAGIC                 OUTFILE= "/app/sasdata/bpr/data/bpr/Daily_HH/HP_ADDS_REMOVALS_DAILY.xls"
-- MAGIC                 DBMS=xls REPLACE;
-- MAGIC     			SHEET="HP_REMOVALS";
-- MAGIC     RUN;
-- MAGIC     
-- MAGIC     
-- MAGIC     
-- MAGIC     PROC EXPORT DATA= Daily_HH.dups_last30_days
-- MAGIC                 OUTFILE= "/app/sasdata/bpr/data/bpr/Daily_HH/HP_ADDS_REMOVALS_DAILY.xls"
-- MAGIC                 DBMS=xls REPLACE;
-- MAGIC     			SHEET="DUPS_30d";
-- MAGIC     RUN;
-- MAGIC     
-- MAGIC     /*==============================================*/
-- MAGIC     
-- MAGIC     filename mymail email ("roy.wang@rci.rogers.com", "Richard.Lin@rci.rogers.com","Patrick.Pang@rci.rogers.com")
-- MAGIC        subject="Homes Passed Daily is READY";
-- MAGIC     options emailsys=smtp emailhost= smtprelay.rci.rogers.com emailport=25;
-- MAGIC     
-- MAGIC     data _null_;
-- MAGIC        file mymail;
-- MAGIC        put 'Homes Passed Daily is done';
-- MAGIC        put 'the summary can be used to identify any issues';
-- MAGIC        put ':)';
-- MAGIC     run;
-- MAGIC     
-- MAGIC     
-- MAGIC     
-- MAGIC     
-- MAGIC     ods package(newzip) open nopf;
-- MAGIC     ods package(newzip) add file="/app/sasdata/bpr/data/bpr/Daily_HH/HP_ADDS_REMOVALS_DAILY.xls";
-- MAGIC     ods package(newzip) publish archive 
-- MAGIC       properties(
-- MAGIC        archive_name="HP_ADDS_REMOVALS_DAILY.zip" 
-- MAGIC        archive_path="/app/sasdata/bpr/data/bpr/Daily_HH/"
-- MAGIC       );
-- MAGIC     ods package(newzip) close;
-- MAGIC     
-- MAGIC     
-- MAGIC     filename mymail23 email ("BIConnectedHome@rci.rogers.com" )
-- MAGIC     
-- MAGIC      subject="HP Adds/Removals &CUR_DATE_NOW."
-- MAGIC         attach="/app/sasdata/bpr/data/bpr/Daily_HH/HP_ADDS_REMOVALS_DAILY.zip";
-- MAGIC     
-- MAGIC     data _null_;
-- MAGIC           file mymail23;
-- MAGIC                    
-- MAGIC           put 'Attached are HP Adds/Removals lists for today';
-- MAGIC          put 'Thanks';
-- MAGIC     run;
-- MAGIC     
-- MAGIC     
-- MAGIC     
-- MAGIC     
-- MAGIC     
-- MAGIC     
-- MAGIC     ***********************END*********************************;
-- MAGIC     
-- MAGIC     %mend go_date;
-- MAGIC     &go%go_date;
-- MAGIC     
-- MAGIC       %put END TIME: %sysfunc(datetime(),datetime14.);
-- MAGIC     	  %put PROCESSING TIME:  %sysfunc(putn(%sysevalf(%sysfunc(TIME())-&datetime_start.),mmss.)) (mm:ss) ;
-- MAGIC     
-- MAGIC     

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %pip install pytz

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime, timedelta, time
-- MAGIC import calendar
-- MAGIC import pytz
-- MAGIC from pyspark.sql.functions import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Set the value of I
-- MAGIC I = 0
-- MAGIC
-- MAGIC est_timezone = pytz.timezone("US/Eastern")
-- MAGIC # Get the current date
-- MAGIC today = datetime.now(est_timezone).date()
-- MAGIC
-- MAGIC # Calculate the current date with adjustments
-- MAGIC curr_d_unformatted = today - timedelta(days=I)
-- MAGIC curr_d = curr_d_unformatted.strftime("%d%m%Y")
-- MAGIC
-- MAGIC # Calculate the previous date with adjustments
-- MAGIC prev_d_unformatted = today - timedelta(days=I + 1)
-- MAGIC prev_d = prev_d_unformatted.strftime("%d%m%Y")
-- MAGIC
-- MAGIC # Calculate PROCESS_DAY
-- MAGIC process_day = datetime(today.year, today.month, today.day)
-- MAGIC process_day_new = curr_d_unformatted.strftime("%d%m%Y")
-- MAGIC # Calculate me_date
-- MAGIC me_date = datetime.combine(today - timedelta(days=I), time.min)
-- MAGIC
-- MAGIC # Calculate me_date1
-- MAGIC me_date1 = datetime.combine(today - timedelta(days=I), time.max)
-- MAGIC
-- MAGIC # Calculate FILE_DATE
-- MAGIC file_date = (today - timedelta(days=I)).strftime("%d%m%Y")
-- MAGIC
-- MAGIC # Calculate CUR_DATE_NOW
-- MAGIC cur_date_now = (today - timedelta(days=I)).replace(day=1).strftime("%Y%m%d")
-- MAGIC
-- MAGIC # Calculate monthend_date (current month's end date)
-- MAGIC _, last_day_of_month = calendar.monthrange(today.year, today.month)
-- MAGIC monthend_date = datetime(today.year, today.month, last_day_of_month)
-- MAGIC
-- MAGIC # Calculate lastmonthend_date (previous month's end date)
-- MAGIC lastmonthend_date = datetime(today.year, today.month, 1) - timedelta(days=1)
-- MAGIC lastmonthend_date = datetime.combine(lastmonthend_date, time.min)
-- MAGIC
-- MAGIC # Print the results
-- MAGIC print("CURR_D:", curr_d)
-- MAGIC print("PREV_D:", prev_d)
-- MAGIC print("PROCESS_DAY:", process_day)
-- MAGIC print("me_date:", me_date)
-- MAGIC print("me_date1:", me_date1)
-- MAGIC print("FILE_DATE:", file_date)
-- MAGIC print("CUR_DATE_NOW:", cur_date_now)
-- MAGIC print("monthend_date:", monthend_date)
-- MAGIC print("lastmonthend_date:", lastmonthend_date)
-- MAGIC

-- COMMAND ----------

select *
from jay_mehta_catalog.sas.vw_table_load_status

-- COMMAND ----------

create or replace temporary view table_status as
select *
from jay_mehta_catalog.sas.vw_table_load_status
where SCHEMA_NAME in ( 'APP_IBRO', 'ENTERPRISE')
and TABLE_NAME in ('IBRO_HOUSEHOLD_CLOSING', 'LOCATION_DIM')

-- COMMAND ----------

select * from table_status

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### spark.sql always returns spark dataframe
-- MAGIC ###### To convert dataframe to variable, you can use first()[0] or collect()[][]

-- COMMAND ----------

-- MAGIC %py
-- MAGIC tb_load = spark.sql("""
-- MAGIC select count(*) from table_status
-- MAGIC """).first()[0]

-- COMMAND ----------

-- MAGIC %py
-- MAGIC tb_load

-- COMMAND ----------

create or replace temporary view TABLE_STATUS_1 as
select 
count(*) as not_started
from table_status 
where STATUS <> 'LOADED'

-- COMMAND ----------

select * from TABLE_STATUS_1

-- COMMAND ----------

select * from jay_mehta_catalog.sas.dup_final

-- COMMAND ----------

create or replace temporary view Latest_Report_Date_dup as
SELECT DISTINCT  TO_DATE(MAX(FROM_UNIXTIME(UNIX_TIMESTAMP(PROCESS_DATE, 'ddMMMyyyy:HH:mm:ss'), 'yyyy-MM-dd'))) AS Latest_Report_Date
FROM jay_mehta_catalog.sas.dup_final

-- COMMAND ----------

select * from Latest_Report_Date_dup

-- COMMAND ----------

-- MAGIC %py
-- MAGIC print(cur_date_now)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC create or replace temporary view dup_date_diff as
-- MAGIC select datediff(Latest_Report_Date, to_date({cur_date_now}, "yyyyMMdd")) * -1 as not_started
-- MAGIC from Latest_Report_Date_dup
-- MAGIC """)

-- COMMAND ----------

select * from dup_date_diff

-- COMMAND ----------

create or replace temporary view TABLE_STATUS_2 as
select * from TABLE_STATUS_1
union all
select * from dup_date_diff d

-- COMMAND ----------

select * from TABLE_STATUS_2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC v = spark.sql("select sum(not_started) as v from TABLE_STATUS_2").collect()[0][0]

-- COMMAND ----------

-- MAGIC %py
-- MAGIC print(v)

-- COMMAND ----------

select * from jay_mehta_catalog.sas.homes_passed_daily

-- COMMAND ----------

create or replace temporary view Latest_Report_Date as
select distinct max(date_format(cast(unix_timestamp(process_date, 'ddMMMyyyy:HH:mm') as timestamp),'ddMMMyyyy')) as Latest_Report_Date
from jay_mehta_catalog.sas.homes_passed_daily

-- COMMAND ----------

select * from Latest_Report_Date

-- COMMAND ----------

select report_date as Table_Loaded_Date from jay_mehta_catalog.sas.ibro_household_closing;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(cur_date_now)

-- COMMAND ----------

select * from jay_mehta_catalog.sas.ibro_household_closing

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""
-- MAGIC create or replace temporary view Table_Loaded_Date as
-- MAGIC select distinct max(report_date) as Table_Loaded_Date
-- MAGIC from jay_mehta_catalog.sas.ibro_household_closing
-- MAGIC where cast(date_format(report_date, "yyyyMMdd") as string) = {cur_date_now}
-- MAGIC """)

-- COMMAND ----------

select * from Table_Loaded_Date

-- COMMAND ----------

-- MAGIC %py
-- MAGIC date = spark.sql("""
-- MAGIC select DATEDIFF(date_format(Table_Loaded_Date,"yyyy-MM-dd"), date_format(to_date(Latest_Report_Date,"ddMMMyyyy"), "yyyy-MM-dd")) as data_gap
-- MAGIC from Table_Loaded_Date a
-- MAGIC inner join Latest_Report_Date b
-- MAGIC on 1=1
-- MAGIC --and a.Table_Loaded_Date = b.Latest_Report_Date
-- MAGIC """).collect()[0][0]

-- COMMAND ----------

-- MAGIC %py
-- MAGIC display(date)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC print(tb_load)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC print(v)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC print(date)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This is where a new notebook is triggered for the rest of the flow

-- COMMAND ----------

-- MAGIC %py
-- MAGIC if tb_load != 0 and v == 0 and date != 0:
-- MAGIC   print("Executing the entire script")
-- MAGIC   dbutils.notebook.run("./08 - hp_script-main", timeout_seconds=6000)
-- MAGIC elif tb_load == 0: 
-- MAGIC   dbutils.notebook.exit("Terminating notebook because tb_load is 0.")
-- MAGIC elif v != 0:
-- MAGIC   dbutils.notebook.exit("Terminating notebook because v is not 0.")
-- MAGIC elif v == 0 and date == 0:
-- MAGIC   dbutils.notebook.exit("Terminating notebook because both v and date are 0.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Below code is same as in the notebook 08 - hp_script-main. While testing the flow, all the below cells can be deleted

-- COMMAND ----------

select * from jay_mehta_catalog.sas.LOCATION_DIM 

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""create or replace table HP_TODAY as
-- MAGIC SELECT * FROM ( select * from (
-- MAGIC SELECT lo.*, 
-- MAGIC CASE WHEN CONTRACT_GROUP_CODE IN('1','4') THEN 1
-- MAGIC      when contract_type_code = '2N0' then 1
-- MAGIC      WHEN NUM_OF_SUITES IS NULL THEN 1 
-- MAGIC      WHEN NUM_OF_SUITES='00000' AND CONTRACT_GROUP_CODE<>'5' THEN 1
-- MAGIC   ELSE COALESCE(CASE WHEN regexp_like(NUM_OF_SUITES,'^[0-9]+$') THEN cast(NUM_OF_SUITES as double)
-- MAGIC             ELSE NULL END ,0) END HOMES_PASSED_COUNT
-- MAGIC   ,ROW_NUMBER()OVER (PARTITION BY LOCATION_ID ORDER BY LOCATION_SOURCE, eff_date DESC) RNK
-- MAGIC FROM jay_mehta_catalog.sas.LOCATION_DIM lo
-- MAGIC WHERE 1=1
-- MAGIC and eff_date <= TO_TIMESTAMP ('{curr_d}', 'ddMMyyyy') 
-- MAGIC --and end_date>= TO_TIMESTAMP ('{curr_d}', 'ddMMyyyy') 
-- MAGIC and CONTRACT_TYPE_CODE not in ('3J1','3P0') and CONTRACT_GROUP_CODE <> '3'
-- MAGIC /* Proximity overlap */
-- MAGIC AND LOCATION_SOURCE in ( 'SS') 
-- MAGIC ) WHERE RNK=1) where homes_passed_flg = 'P' and HOMES_PASSED_COUNT>=1 and (dummy_addr_ind <> 'Y')
-- MAGIC """)

-- COMMAND ----------

select * from HP_TODAY

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""create or replace temporary view HP_TODAY_FWA as
-- MAGIC SELECT * FROM ( select * from (
-- MAGIC SELECT lo.*, 
-- MAGIC CASE 
-- MAGIC      WHEN NUM_OF_SUITES IS NULL THEN 1 
-- MAGIC      WHEN NUM_OF_SUITES='00000' AND CONTRACT_GROUP_CODE<>'5' THEN 1
-- MAGIC   ELSE COALESCE(CASE WHEN regexp_like(NUM_OF_SUITES,'^[0-9]+$') THEN cast(NUM_OF_SUITES as double)
-- MAGIC             ELSE NULL END ,0) END HOMES_PASSED_COUNT
-- MAGIC   ,ROW_NUMBER()OVER (PARTITION BY LOCATION_ID ORDER BY LOCATION_SOURCE, eff_date DESC) RNK
-- MAGIC FROM jay_mehta_catalog.sas.LOCATION_DIM lo
-- MAGIC WHERE eff_date <= TO_TIMESTAMP ('{curr_d}', 'ddMMyyyy') 
-- MAGIC --and end_date>= TO_TIMESTAMP ('{curr_d}', 'ddMMyyyy') 
-- MAGIC AND LOCATION_SOURCE in ( 'FWA', 'FWACBU', 'FWASEA', 'FWARWV', 'FWAKWIC', 'WHIXCOUNTRY', 'WHIMNLDT','WHICRAVE'	)
-- MAGIC ) WHERE RNK=1) where homes_passed_flg = 'P' and HOMES_PASSED_COUNT>=1
-- MAGIC """)

-- COMMAND ----------

select * from HP_TODAY_FWA

-- COMMAND ----------

UPDATE HP_TODAY set HOMES_PASSED_DUP_FLG='N'

-- COMMAND ----------

SELECT SAM_KEY, PROCESS_DATE, to_date(PROCESS_DATE, 'ddMMMyyyy:HH:mm:ss') FROM jay_mehta_catalog.sas.dup_final 

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""
-- MAGIC UPDATE HP_TODAY set HOMES_PASSED_DUP_FLG='Y' WHERE SAM_KEY IN (SELECT SAM_KEY FROM jay_mehta_catalog.sas.dup_final 
-- MAGIC WHERE to_date(PROCESS_DATE, 'ddMMMyyyy:HH:mm:ss')=to_date('{process_day}','yyyy-MM-dd HH:mm:ss')
-- MAGIC )""")

-- COMMAND ----------

CREATE OR REPLACE TABLE HP_TODAY AS
SELECT * FROM HP_TODAY
UNION ALL
SELECT * FROM HP_TODAY_FWA;

-- COMMAND ----------

select * from HP_TODAY

-- COMMAND ----------

SELECT SUM(HOMES_PASSED_COUNT) as HP_today FROM HP_TODAY;

-- COMMAND ----------

SELECT SUM(HOMES_PASSED_COUNT) as HP_today FROM HP_TODAY where HOMES_PASSED_DUP_FLG='N';

-- COMMAND ----------

create
or replace temporary view region as
select
  distinct POSTAL_CODE,
  FSA,
  upper(region_segment_cd) AS NEW_REGION,
  upper(region_segment_nm) AS NEW_SUBREGION
from
  jay_mehta_catalog.sas.region

-- COMMAND ----------

SELECT DISTINCT *
FROM region
ORDER BY POSTAL_CODE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""create or replace table HP_TODAY2 as 
-- MAGIC SELECT to_date('{process_day}','yyyy-MM-dd HH:mm:ss') AS PROCESS_DATE
-- MAGIC ,a.LOCATION_KEY
-- MAGIC ,a.LOCATION_SOURCE
-- MAGIC ,a.LOCATION_ID
-- MAGIC ,a.SAM_KEY
-- MAGIC ,a.ADDRESS_ID
-- MAGIC ,a.COMPANY
-- MAGIC ,a.STREET_NUMBER
-- MAGIC ,a.STREET_NAME
-- MAGIC ,a.STREET_TYPE
-- MAGIC ,a.COMPASS_DIR_CODE
-- MAGIC ,a.APARTMENT_NUMBER
-- MAGIC ,a.CITY_NAME
-- MAGIC ,a.PROVINCE_CODE
-- MAGIC ,a.PROVINCE
-- MAGIC ,a.POSTAL_CODE
-- MAGIC ,a.ADDRESS_KEY
-- MAGIC ,a.REGION_MAP_ID
-- MAGIC ,a.CONTRACT_TYPE_CODE
-- MAGIC ,a.CONTRACT_GROUP_CODE
-- MAGIC -- adding case when to manually update the sample data 
-- MAGIC ,case when location_key = '134234990' then 'T' else a.SERVICEABILITY_CODE end as SERVICEABILITY_CODE
-- MAGIC ,a.HOMES_PASSED_FLG
-- MAGIC ,a.HOMES_PASSED_DATE
-- MAGIC ,a.CRNT_IND
-- MAGIC ,a.EFF_DATE
-- MAGIC ,a.END_DATE
-- MAGIC ,a.ETL_INSRT_RUN_ID
-- MAGIC ,a.ETL_INSRT_DT
-- MAGIC ,a.ETL_UPDT_RUN_ID
-- MAGIC ,a.ETL_UPDT_DT
-- MAGIC ,a.REGION_POSTAL_ID
-- MAGIC ,a.MUNICIPALITY_CODE
-- MAGIC ,a.MAP_AREA_CODE
-- MAGIC ,a.BULK_PROFILE_NAME
-- MAGIC ,a.BULK_PROFILE_EXP_DATE
-- MAGIC ,a.NUM_OF_SUITES
-- MAGIC ,a.IN_TERRITORY_IND
-- MAGIC ,a.ENERGIZED_DATE
-- MAGIC ,a.DUMMY_ADDR_IND
-- MAGIC ,a.ON_PLANT_IND
-- MAGIC ,a.HOUSE_SUFFIX_CODE
-- MAGIC ,'E' as HOMES_PASSED_DUP_FLG
-- MAGIC ,a.PLANT_SHUB
-- MAGIC ,a.DWELLING_TYPE_CODE
-- MAGIC ,a.SRC_EFF_DATE
-- MAGIC ,a.SRC_END_DATE
-- MAGIC ,a.TOPOLOGY
-- MAGIC ,a.NETWORK_TYPE
-- MAGIC ,a.CABLE_CONDITION
-- MAGIC ,a.CABLE_TYPE
-- MAGIC ,a.FEED_TYPE
-- MAGIC ,a.FIBERCONDITION
-- MAGIC ,a.PHUB
-- MAGIC ,a.PONHUB
-- MAGIC ,a.TOPOLOGY_DESCRIPTION
-- MAGIC ,a.RPATS_NUMBER
-- MAGIC ,a.RPATS_DESC
-- MAGIC ,a.LONGITUDE
-- MAGIC ,a.LATITUDE
-- MAGIC ,a.FRANCHISE_AREA_CDE
-- MAGIC ,a.PROJ_TYPE_IND
-- MAGIC ,a.FIRST_FLIP_DATE
-- MAGIC ,a.RPAT_IN_SERVICE_DATE
-- MAGIC ,a.ONT
-- MAGIC ,a.ONT_SERIAL_NUMBER
-- MAGIC ,a.PONNODE
-- MAGIC ,a.PONNAP
-- MAGIC ,a.PON_RESOLUTION_DATE
-- MAGIC ,a.PROFILE_ID
-- MAGIC ,a.UPLIFT_IND
-- MAGIC ,a.ADM_PROJECT_NUMBER
-- MAGIC ,a.HOMES_PASSED_COUNT
-- MAGIC ,a.RNK
-- MAGIC ,"SHM" as PRODUCT
-- MAGIC ,CASE WHEN LOCATION_SOURCE in ('FWA', 'FWACBU') THEN 'SWO' 
-- MAGIC WHEN LOCATION_SOURCE in ('FWARWV') THEN 'GTA'
-- MAGIC WHEN LOCATION_SOURCE in ('FWASEA', 'WHIXCOUNTRY', 'WHIMNLDT') THEN 'NS'
-- MAGIC WHEN LOCATION_SOURCE in ('FWAKWIC') THEN 'SWO'
-- MAGIC WHEN LOCATION_SOURCE in ('WHICRAVE') THEN 'NB'
-- MAGIC END AS NEW_REGION,
-- MAGIC CASE WHEN LOCATION_SOURCE in ('FWA', 'FWACBU') THEN 'GREATER HAMILTON AREA' 
-- MAGIC WHEN LOCATION_SOURCE in ('FWARWV') THEN 'NEWMARKET'
-- MAGIC WHEN LOCATION_SOURCE in ('FWASEA', 'WHIXCOUNTRY', 'WHIMNLDT') THEN 'NOVA SCOTIA'
-- MAGIC WHEN LOCATION_SOURCE in ('FWAKWIC') THEN 'BRANTFORD'
-- MAGIC WHEN LOCATION_SOURCE in ('WHICRAVE') THEN 'SAINT JOHN'
-- MAGIC END AS NEW_SUBREGION,
-- MAGIC CASE WHEN CONTRACT_GROUP_CODE='1' THEN 'SFU'
-- MAGIC 	WHEN CONTRACT_GROUP_CODE in ('2','3') THEN 'BULK'
-- MAGIC 	WHEN CONTRACT_GROUP_CODE='4' THEN 'MDU'
-- MAGIC 	ELSE 'COMMERCIAL' END AS HP_SEGMENT
-- MAGIC from HP_TODAY a left join region b on a.postal_code=b.postal_code;
-- MAGIC """)

-- COMMAND ----------

select * from HP_TODAY2

-- COMMAND ----------

create or replace table serviceability_T_HP as
select * from HP_TODAY2 where serviceability_code = "T"

-- COMMAND ----------

select * from serviceability_T_HP

-- COMMAND ----------

create or replace temporary view HP_TODAY3 as
select * from HP_TODAY2 where serviceability_code <> "T"

-- COMMAND ----------

select * from HP_TODAY3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Get MS WHI subs

-- COMMAND ----------

create or replace temporary view MS_WHI_product_codes as 
select * from  jay_mehta_catalog.sas.PRODUCT_HIERARCHY a where LEVEL_4_ID in (141291)
 and crnt_ind = 'Y'
 and REVENUE_TSU = 'Y';

-- COMMAND ----------

select * from MS_WHI_product_codes

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql("""create or replace temporary view MS_FWA as 
-- MAGIC select * from jay_mehta_catalog.sas.ibro_Subscriber_activities
-- MAGIC where 1=1
-- MAGIC and date_format(activity_date, "yyyy-MM-dd") = to_date({file_date},"ddMMyyyy")
-- MAGIC and product_lob_unit in ( 'Y')
-- MAGIC and product_code in (select LEVEL_5_PRODUCT_CODE from MS_WHI_product_codes)
-- MAGIC and activity = 'CL'
-- MAGIC order by customer_location_id""".format(file_date=file_date))

-- COMMAND ----------

select * from MS_FWA

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Get SHM subs

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""create or replace temporary view SHM_BASE as 
-- MAGIC select * from jay_mehta_catalog.sas.ibro_Subscriber_activities
-- MAGIC where date_format(activity_date, "yyyy-MM-dd") = to_date({file_date},"ddMMyyyy")
-- MAGIC and product_lob_unit in ( 'Y')
-- MAGIC and product_lob in ('500')
-- MAGIC and activity = 'CL'
-- MAGIC order by customer_location_id""")

-- COMMAND ----------

select * from SHM_BASE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### couldn't find "product" column in serviceability_T_HP

-- COMMAND ----------

update serviceability_T_HP 
set PRODUCT = 'SHM'
where sam_key in (select customer_location_id from SHM_BASE);

-- COMMAND ----------

create or replace temporary view serviceability_T_HP2 as 
select PROCESS_DATE, SAM_KEY, POSTAL_CODE, CONTRACT_GROUP_CODE, HOMES_PASSED_DUP_FLG, HOMES_PASSED_COUNT, serviceability_code, NEW_REGION, NEW_SUBREGION, PRODUCT 
from serviceability_T_HP

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Serv T history file

-- COMMAND ----------

select PROCESS_DATE, to_date(PROCESS_DATE,"ddMMMyyyy:HH:mm"), * from jay_mehta_catalog.sas.DAILY_SERV_T_BASE

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""delete from jay_mehta_catalog.sas.DAILY_SERV_T_BASE
-- MAGIC where to_date(PROCESS_DATE,"ddMMMyyyy:HH:mm")=to_date('{process_day}','yyyy-MM-dd HH:mm:ss')
-- MAGIC """)

-- COMMAND ----------

SELECT * FROM jay_mehta_catalog.sas.DAILY_SERV_T_BASE

-- COMMAND ----------

INSERT INTO TABLE jay_mehta_catalog.sas.DAILY_SERV_T_BASE
SELECT DATE_FORMAT(PROCESS_DATE, "ddMMMyyyy:HH:mm"),	
SAM_KEY,	
POSTAL_CODE,	
CONTRACT_GROUP_CODE,	
SERVICEABILITY_CODE,	
HOMES_PASSED_DUP_FLG,	
HOMES_PASSED_COUNT,	
NEW_REGION,	
NEW_SUBREGION,	
PRODUCT  
FROM serviceability_T_HP2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### addn MS WHI HP back to HP base including dups , chnage dup plag to N

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW MS_FWA_HP AS
SELECT *
FROM serviceability_T_HP
WHERE PRODUCT = 'WHI' AND HOMES_PASSED_DUP_FLG = 'N'

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW HP_TODAY4 AS
SELECT * FROM HP_TODAY3
UNION
SELECT * FROM MS_FWA_HP

-- COMMAND ----------

select * from HP_TODAY4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Summary

-- COMMAND ----------

CREATE or REPLACE TABLE HP_TODAY_SUMMARY AS
SELECT PROCESS_DATE,
"HP_BASE" as SOURCE,
/*"EOP" AS METRIC, */
CONTRACT_GROUP_CODE,
HOMES_PASSED_DUP_FLG,
NEW_REGION,
NEW_SUBREGION,
HP_SEGMENT,
CASE WHEN CONTRACT_GROUP_CODE IN ('1','4') THEN 'CONNECTED HOME'
     WHEN CONTRACT_GROUP_CODE IN ('5') THEN 'R4B'
	 ELSE HP_SEGMENT END AS FINAL_SEGMENT,
SUM(HOMES_PASSED_COUNT) AS HOMES_PASSED_COUNT
FROM HP_TODAY4
GROUP BY 1,2,3,4,5,6,7,8

-- COMMAND ----------

select * from HP_TODAY_SUMMARY

-- COMMAND ----------

-- MAGIC %py
-- MAGIC print(process_day)
-- MAGIC process_day_formatted = process_day.strftime("%Y-%m-%d")
-- MAGIC print(process_day_formatted)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # cable cable
-- MAGIC spark.sql(f"""insert into HP_TODAY_SUMMARY  values ('{process_day_formatted}','HP_BASE','1'	,'E', 
-- MAGIC 'OTT',	'RURAL OTTAWA','SFU', 'CONNECTED HOME',  13112)""".format(process_day=process_day))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # cable cable
-- MAGIC #spark.sql(f"""insert into HP_TODAY_SUMMARY  values ('{process_day_formatted}','HP_BASE','1'	,'E', 
-- MAGIC #'OTT',	'RURAL OTTAWA','SFU', 'CONNECTED HOME',  13112)""")
-- MAGIC
-- MAGIC # Seaside & XCOUNTRY & Mainland
-- MAGIC spark.sql(f"""insert into HP_TODAY_SUMMARY  values ('{process_day_formatted}','HP_BASE','1'	,'E', 
-- MAGIC 'NS', 'NOVA SCOTIA','SFU', 'CONNECTED HOME',  30570 )""")
-- MAGIC
-- MAGIC # Proximity Crave 
-- MAGIC spark.sql(f"""insert into HP_TODAY_SUMMARY  values ('{process_day_formatted}','HP_BASE','1'	,'E', 
-- MAGIC 'NB', 'SAiNT JOHN','SFU', 'CONNECTED HOME',  1026 )""")
-- MAGIC
-- MAGIC # Proximity Crave 
-- MAGIC spark.sql(f"""insert into HP_TODAY_SUMMARY  values ('{process_day_formatted}','HP_BASE','1'	,'E', 
-- MAGIC 'SWO', 'GUELPH','SFU', 'CONNECTED HOME',  83 )""")

-- COMMAND ----------

select * from HP_TODAY_SUMMARY

-- COMMAND ----------

CREATE OR REPLACE TABLE HP_TODAY_SUMMARY AS 
SELECT PROCESS_DATE,
SOURCE,
CONTRACT_GROUP_CODE,
HOMES_PASSED_DUP_FLG,
NEW_REGION,
NEW_SUBREGION,
HP_SEGMENT,
FINAL_SEGMENT,
CASE WHEN HOMES_PASSED_DUP_FLG = "E" AND NEW_SUBREGION = "NEWMARKET" THEN HOMES_PASSED_COUNT + 5209
WHEN HOMES_PASSED_DUP_FLG = "E" AND NEW_SUBREGION = "BRANTFORD" THEN HOMES_PASSED_COUNT + 2928
ELSE HOMES_PASSED_COUNT END AS HOMES_PASSED_COUNT
FROM HP_TODAY_SUMMARY

-- COMMAND ----------

select * from HP_TODAY_SUMMARY

-- COMMAND ----------

SELECT HOMES_PASSED_DUP_FLG,SUM(HOMES_PASSED_COUNT) as HP_today FROM HP_TODAY_SUMMARY
group by HOMES_PASSED_DUP_FLG;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Skipping the next step as we are assuming SAL_HP.DAILY_HOMES_PASSED_BASE is empty

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW DAILY_HOMES_PASSED_BASE AS
--SELECT * FROM OTHER_DATE_HP
--UNION ALL
SELECT * FROM HP_TODAY_SUMMARY;

-- COMMAND ----------

select date_format(report_date, "yyyy-MM-dd"), * from jay_mehta_catalog.sas.ibro_household_closing

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""CREATE or REPLACE TABLE cr_bundle AS
-- MAGIC SELECT
-- MAGIC   report_Date,
-- MAGIC   customer_location_id,
-- MAGIC   customer_account,
-- MAGIC   customer_id,
-- MAGIC   postal_code,
-- MAGIC   substr(postal_code, 1, 3) AS fsa,
-- MAGIC   CONTRACT_GROUP_CODE,
-- MAGIC   MULTI_SYSTEM,
-- MAGIC   MULTI_BRAND,
-- MAGIC   MULTI_SEGMENT,
-- MAGIC   REPLACE(REPLACE(REPLACE(MULTI_PRODUCT_NAME, 'INT', 'BB'), 'TVE', 'TV'), 'TV-BB', 'BB-TV') AS MULTI_PRODUCT,
-- MAGIC   SUM(closing_hh) AS CLOSING_HH
-- MAGIC FROM
-- MAGIC   jay_mehta_catalog.sas.ibro_household_closing
-- MAGIC WHERE 1=1
-- MAGIC   AND date_format(report_date, "yyyy-MM-dd") = date_sub(to_date('{curr_d}', "ddMMyyyy"), 1)
-- MAGIC   AND closing_hh <> 0
-- MAGIC GROUP BY
-- MAGIC   report_Date,
-- MAGIC   customer_location_id,
-- MAGIC   customer_account,
-- MAGIC   customer_id,
-- MAGIC   postal_code,
-- MAGIC   substr(postal_code, 1, 3),
-- MAGIC   REPLACE(REPLACE(REPLACE(MULTI_PRODUCT_NAME, 'INT', 'BB'), 'TVE', 'TV'), 'TV-BB', 'BB-TV'),
-- MAGIC   CONTRACT_GROUP_CODE,
-- MAGIC   MULTI_SYSTEM,
-- MAGIC   MULTI_BRAND,
-- MAGIC   MULTI_SEGMENT""")
-- MAGIC

-- COMMAND ----------

select * from cr_bundle

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""CREATE or REPLACE TABLE cr_bundle2 as 
-- MAGIC SELECT to_date('{process_day}','yyyy-MM-dd HH:mm:ss') AS PROCESS_DATE, a.*,
-- MAGIC CASE
-- MAGIC     WHEN MULTI_SYSTEM IN ('FWASEA', 'SEA', 'WHIXCOUNTRY', 'XCOUNTRY', 'MNLDT', 'WHIMNLDT', 'WHICRAVE') THEN 'NS'
-- MAGIC     WHEN MULTI_SYSTEM IN ('FWAKWIC', 'KWIC') THEN 'SWO'
-- MAGIC     WHEN MULTI_SYSTEM IN ('FWA', 'FWACBU', 'V21') OR POSITION('SOURCE' IN MULTI_SYSTEM) > 0 THEN 'SWO'
-- MAGIC     WHEN MULTI_SYSTEM IN ('FWARWV') THEN 'GTA'
-- MAGIC     WHEN MULTI_SYSTEM IN ('WHICRAVE') THEN 'NB'
-- MAGIC     WHEN MULTI_SYSTEM IN ('NETFLASH') THEN 'SWO'
-- MAGIC     ELSE NULL
-- MAGIC   END AS NEW_REGION,
-- MAGIC CASE
-- MAGIC     WHEN MULTI_SYSTEM IN ('FWASEA', 'SEA', 'WHIXCOUNTRY', 'XCOUNTRY', 'MNLDT', 'WHIMNLDT', 'WHICRAVE') THEN 'NOVA SCOTIA'
-- MAGIC     WHEN MULTI_SYSTEM IN ('FWAKWIC', 'KWIC') THEN 'BRANTFORD'
-- MAGIC     WHEN MULTI_SYSTEM IN ('FWA', 'FWACBU', 'V21') OR POSITION('SOURCE' IN MULTI_SYSTEM) > 0 THEN 'GREATER HAMILTON AREA'
-- MAGIC     WHEN MULTI_SYSTEM IN ('FWARWV') THEN 'NEWMARKET'
-- MAGIC     WHEN MULTI_SYSTEM IN ('WHICRAVE') THEN 'SAINT JOHN'
-- MAGIC     WHEN MULTI_SYSTEM IN ('NETFLASH') THEN 'GUELPH'
-- MAGIC     ELSE NULL
-- MAGIC   END AS NEW_SUBREGION,
-- MAGIC CASE WHEN CONTRACT_GROUP_CODE='1' THEN 'SFU'
-- MAGIC 	WHEN CONTRACT_GROUP_CODE='5' THEN 'COMMERCIAL'
-- MAGIC 	WHEN CONTRACT_GROUP_CODE='4' THEN 'MDU'
-- MAGIC 	ELSE 'BULK' END AS HP_SEGMENT
-- MAGIC from cr_bundle a left join region b on a.postal_code=b.postal_code""")

-- COMMAND ----------

select * from cr_bundle2

-- COMMAND ----------

CREATE or REPLACE TABLE HH_CLOSING_TD_SUMMARY AS
SELECT PROCESS_DATE,
"HH_CLOSING" as SOURCE,
CONTRACT_GROUP_CODE,
NEW_REGION,
NEW_SUBREGION,
HP_SEGMENT,
MULTI_SEGMENT,
CASE WHEN HP_SEGMENT IN ('SFU','MDU') THEN 'CONNECTED HOME'
     WHEN HP_SEGMENT IN ('COMMERCIAL') THEN 'R4B'
	 ELSE HP_SEGMENT END AS FINAL_SEGMENT,
COALESCE(SUM(CLOSING_HH),0) AS CLOSING_HH
FROM cr_bundle2
GROUP BY 1,2,3,4,5,6,7,8

-- COMMAND ----------

select * from HH_CLOSING_TD_SUMMARY

-- COMMAND ----------

CREATE or REPLACE TABLE HH_CLOSING_TD_SUMMARY_FREQ AS
SELECT
  CONTRACT_GROUP_CODE,
  HP_SEGMENT,
  FINAL_SEGMENT,
  COUNT(*) AS frequency
FROM
  HH_CLOSING_TD_SUMMARY
GROUP BY
  CONTRACT_GROUP_CODE, HP_SEGMENT, FINAL_SEGMENT

-- COMMAND ----------

SELECT * FROM HH_CLOSING_TD_SUMMARY_FREQ

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Skipping the next step as we are assuming SAL_HP.DAILY_HOUSEHOLE_CLOSING is empty

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW  DAILY_HOUSEHOLD_CLOSING AS
--SELECT * FROM OTHER_DATE
--UNION ALL
SELECT * FROM HH_CLOSING_TD_SUMMARY;

-- COMMAND ----------

CREATE or REPLACE TABLE HP_BASE_SUMMARY_OUTPUT AS
SELECT 
PROCESS_DATE,
SOURCE,
HOMES_PASSED_DUP_FLG,
FINAL_SEGMENT,
SUM(HOMES_PASSED_COUNT) AS HOMES_PASSED_COUNT
FROM HP_TODAY_SUMMARY
WHERE HOMES_PASSED_DUP_FLG <> 'Y'
GROUP BY 1,2,3,4

-- COMMAND ----------

SELECT * FROM HP_BASE_SUMMARY_OUTPUT

-- COMMAND ----------

CREATE OR REPLACE TABLE HH_CLOSING_SUMMARY_OUTPUT AS
SELECT PROCESS_DATE,
SOURCE,
FINAL_SEGMENT,
COALESCE(SUM(CLOSING_HH),0) AS CLOSING_HH
FROM HH_CLOSING_TD_SUMMARY
GROUP BY 1,2,3

-- COMMAND ----------

SELECT * FROM HH_CLOSING_SUMMARY_OUTPUT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### GET RBS AND RBS DUP INFO FROM SANDBOX TABLE (THIS IS UPDATED MONTHLY)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""CREATE OR REPLACE TABLE RBS_IN_RECENT_2MTHS AS
-- MAGIC SELECT *
-- MAGIC FROM jay_mehta_catalog.sas.cr_rbs
-- MAGIC WHERE to_date(REPORT_DATE, "ddMMMyyyy:HH:mm") IN (to_date('{monthend_date}',"yyyy-MM-dd HH:mm:SS"), to_date('{lastmonthend_date}',"yyyy-MM-dd HH:mm:SS"))
-- MAGIC """)

-- COMMAND ----------

select * from RBS_IN_RECENT_2MTHS

-- COMMAND ----------

-- MAGIC %py
-- MAGIC max_date = spark.sql("""select max(report_date) as max_date from RBS_IN_RECENT_2MTHS""").collect()[0][0]

-- COMMAND ----------

-- MAGIC %py
-- MAGIC print(max_date)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE TABLE MAX_ME_DATE AS
-- MAGIC SELECT
-- MAGIC   *,
-- MAGIC   CASE
-- MAGIC     WHEN CONTRACT_GROUP_CODE = '5' THEN 'COMMERCIAL'
-- MAGIC     WHEN CONTRACT_GROUP_CODE = '1' THEN 'SFU'
-- MAGIC     WHEN CONTRACT_GROUP_CODE = '4' THEN 'MDU'
-- MAGIC     ELSE 'BULK'
-- MAGIC   END AS HP_SEGMENT
-- MAGIC FROM (
-- MAGIC   SELECT
-- MAGIC     *,
-- MAGIC     to_date('{process_day}','yyyy-MM-dd HH:mm:ss') AS TEMP_DATE
-- MAGIC   FROM RBS_IN_RECENT_2MTHS
-- MAGIC   WHERE REPORT_DATE = '{max_date}'
-- MAGIC   )
-- MAGIC """)

-- COMMAND ----------

select * from MAX_ME_DATE

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""CREATE OR REPLACE TABLE DAILY_DATE AS 
-- MAGIC SELECT DISTINCT PROCESS_DATE FROM DAILY_HOUSEHOLD_CLOSING 
-- MAGIC WHERE YEAR(PROCESS_DATE)=YEAR(to_date('{process_day}','yyyy-MM-dd HH:mm:ss')) 
-- MAGIC AND MONTH(PROCESS_DATE)=MONTH(to_date('{process_day}','yyyy-MM-dd HH:mm:ss'))""")

-- COMMAND ----------

select * from DAILY_DATE

-- COMMAND ----------

CREATE OR REPLACE TABLE jay_mehta_catalog.sas.POPULATE_RBS AS 
SELECT A.PROCESS_DATE, 
"HH_CLOSING" as SOURCE,
CONTRACT_GROUP_CODE,
NEW_REGION,
NEW_SUBREGION,
HP_SEGMENT,
MULTI_SEGMENT,
CASE WHEN HP_SEGMENT IN ('SFU','MDU') THEN 'CONNECTED HOME'
     WHEN HP_SEGMENT IN ('COMMERCIAL') THEN 'R4B'
	 ELSE HP_SEGMENT END AS FINAL_SEGMENT,
COALESCE(SUM(CR_QTY),0) AS CLOSING_HH
FROM MAX_ME_DATE B
INNER JOIN DAILY_DATE A
ON YEAR(A.PROCESS_DATE)=YEAR(TEMP_DATE)
AND MONTH(A.PROCESS_DATE)=MONTH(TEMP_DATE)
GROUP BY 1,2,3,4,5,6,7,8

-- COMMAND ----------

select * from jay_mehta_catalog.sas.POPULATE_RBS

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW DAILY_RBS_CLOSING AS
SELECT * FROM jay_mehta_catalog.sas.POPULATE_RBS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Cannot traceback HOMES_PASSED_COUNT. Need some inputs. The script needs to be updated for the missing column

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW HOMES_PASSED_DAILY AS
SELECT * FROM DAILY_HOUSEHOLD_CLOSING
UNION
SELECT * FROM DAILY_RBS_CLOSING
UNION
SELECT * FROM DAILY_HOMES_PASSED_BASE;

-- COMMAND ----------

select * from HOMES_PASSED_DAILY

-- COMMAND ----------

SELECT
  PROCESS_DATE,
  SOURCE,
  COUNT(*) AS frequency
FROM HOMES_PASSED_DAILY
GROUP BY PROCESS_DATE, SOURCE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### New dashboard

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW homes_passed_daily_view AS
SELECT * FROM HOMES_PASSED_DAILY;

-- COMMAND ----------

select * from homes_passed_daily_view

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW test_yesterday AS
-- MAGIC SELECT
-- MAGIC   *,
-- MAGIC   CASE
-- MAGIC     --WHEN process_date = date_sub(to_date('{file_date}',"ddMMyyyy"), 1)
-- MAGIC       --AND SOURCE = 'HP_BASE' THEN HOMES_PASSED_COUNT
-- MAGIC     WHEN process_date = date_sub(to_date('{file_date}',"ddMMyyyy"), 1)
-- MAGIC       AND SOURCE = 'HH_CLOSING' THEN CLOSING_HH
-- MAGIC     ELSE NULL  -- Adjust as needed if you want a default value for other cases
-- MAGIC   END AS BOP
-- MAGIC FROM homes_passed_daily_view
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""CREATE OR REPLACE TEMPORARY VIEW test_today AS
-- MAGIC SELECT
-- MAGIC   *,
-- MAGIC   CASE
-- MAGIC     --WHEN DATE_TRUNC(process_date, 'DD') = to_date('{process_day}')
-- MAGIC       --AND SOURCE = 'HP_BASE' THEN HOMES_PASSED_COUNT
-- MAGIC     WHEN DATE_TRUNC(process_date, 'DD') = to_date('{process_day}')
-- MAGIC       AND SOURCE = 'HH_CLOSING' THEN CLOSING_HH
-- MAGIC     ELSE NULL
-- MAGIC   END AS EOP
-- MAGIC FROM homes_passed_daily_view
-- MAGIC """)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_test_today_2 AS
SELECT
  t1.*
  ,t2.BOP
FROM
  test_today t1
JOIN
  test_yesterday t2
ON
  t1.process_date = t2.process_date
  AND t1.SOURCE = t2.SOURCE
  AND t1.CONTRACT_GROUP_CODE = t2.CONTRACT_GROUP_CODE
  AND t1.NEW_REGION = t2.NEW_REGION
  AND t1.NEW_SUBREGION = t2.NEW_SUBREGION
  AND t1.HP_SEGMENT = t2.HP_SEGMENT
  AND t1.MULTI_SEGMENT = t2.MULTI_SEGMENT
  AND t1.FINAL_SEGMENT = t2.FINAL_SEGMENT
  --AND t1.HOMES_PASSED_DUP_FLG = t2.HOMES_PASSED_DUP_FLG;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC def null_to_zero(df):
-- MAGIC     numeric_columns = [c for c, t in df.dtypes if t == 'int' or t == 'double' or t == 'bigint']
-- MAGIC     for column in numeric_columns:
-- MAGIC         df = df.withColumn(column, when(col(column).isNull(), 0).otherwise(col(column)))
-- MAGIC     return df

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df = spark.table("vw_test_today_2")
-- MAGIC final_df = null_to_zero(df)
-- MAGIC final_df.createOrReplaceTempView("test_today_2")

-- COMMAND ----------

select * from test_today_2

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW test_today_2_with_net AS
SELECT
  *,
  EOP - BOP AS NET
FROM
  test_today_2;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW EOP AS
SELECT
  process_date,
  SOURCE,
  CONTRACT_GROUP_CODE,
  NEW_REGION,
  NEW_SUBREGION,
  HP_SEGMENT,
  MULTI_SEGMENT,
  FINAL_SEGMENT,
  --HOMES_PASSED_DUP_FLG,
  EOP AS Summary,
  'EOP' AS category
FROM
  test_today;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW BOP AS
SELECT
  process_date,
  SOURCE,
  CONTRACT_GROUP_CODE,
  NEW_REGION,
  NEW_SUBREGION,
  HP_SEGMENT,
  MULTI_SEGMENT,
  FINAL_SEGMENT,
  --HOMES_PASSED_DUP_FLG,
  BOP AS Summary,
  'BOP' AS category
FROM
  test_yesterday;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW NET AS
SELECT
  process_date,
  SOURCE,
  CONTRACT_GROUP_CODE,
  NEW_REGION,
  NEW_SUBREGION,
  HP_SEGMENT,
  MULTI_SEGMENT,
  FINAL_SEGMENT,
  --HOMES_PASSED_DUP_FLG,
  NET AS Summary,
  'NET' AS category
FROM
  test_today_2_with_net;

-- COMMAND ----------

CREATE OR REPLACE TABLE SUMMARY AS
SELECT * FROM EOP
UNION ALL
SELECT * FROM BOP
UNION ALL
SELECT * FROM NET

-- COMMAND ----------

update SUMMARY set  NEW_SUBREGION = 'EDMUNDSTON', NEW_REGION  = 'NB'
where  NEW_SUBREGION = 'QUEBEC';

-- COMMAND ----------

update SUMMARY
set NEW_SUBREGION = "ST JOHN'S",  NEW_REGION = 'NL'
where  new_region = 'NT';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### New Adds Removals logic

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""
-- MAGIC create or replace table HP_YESTERDAY as select * from 
-- MAGIC (SELECT * FROM ( select * from (
-- MAGIC SELECT lo.*, 
-- MAGIC CASE WHEN CONTRACT_GROUP_CODE IN('1','4') THEN 1
-- MAGIC      when contract_type_code = '2N0' then 1
-- MAGIC      WHEN NUM_OF_SUITES IS NULL THEN 1 
-- MAGIC      WHEN NUM_OF_SUITES='00000' AND CONTRACT_GROUP_CODE<>'5' THEN 1
-- MAGIC   ELSE COALESCE(CASE WHEN regexp_like(NUM_OF_SUITES,'^[0-9]+$') THEN cast(NUM_OF_SUITES as double)
-- MAGIC             ELSE NULL END ,0) END HOMES_PASSED_COUNT
-- MAGIC   ,ROW_NUMBER()OVER (PARTITION BY LOCATION_ID ORDER BY LOCATION_SOURCE, eff_date DESC) RNK
-- MAGIC FROM jay_mehta_catalog.sas.LOCATION_DIM lo
-- MAGIC WHERE
-- MAGIC eff_date <= TO_TIMESTAMP ('{prev_d}', 'ddMMyyyy')
-- MAGIC --and end_date>= TO_TIMESTAMP ('{prev_d}', 'ddMMyyyy')
-- MAGIC and CONTRACT_TYPE_CODE not in ('3J1','3P0') and CONTRACT_GROUP_CODE <> '3'
-- MAGIC AND LOCATION_SOURCE  = 'SS' ) WHERE RNK=1) where homes_passed_flg = 'P' and HOMES_PASSED_COUNT>=1 and (dummy_addr_ind <> 'Y')
-- MAGIC order by sam_key
-- MAGIC )""")

-- COMMAND ----------

select * from HP_YESTERDAY

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""create or replace table HP_YESTERDAY as select * from (select * from (
-- MAGIC SELECT lo.*, 
-- MAGIC CASE WHEN CONTRACT_GROUP_CODE IN('1','4') THEN 1
-- MAGIC      when contract_type_code = '2N0' then 1
-- MAGIC      WHEN NUM_OF_SUITES IS NULL THEN 1 
-- MAGIC      WHEN NUM_OF_SUITES='00000' AND CONTRACT_GROUP_CODE<>'5' THEN 1
-- MAGIC   ELSE COALESCE(CASE WHEN regexp_like(NUM_OF_SUITES,'^[0-9]+$') THEN cast(NUM_OF_SUITES as double)
-- MAGIC             ELSE NULL END ,0) END HOMES_PASSED_COUNT
-- MAGIC   ,ROW_NUMBER()OVER (PARTITION BY LOCATION_ID ORDER BY LOCATION_SOURCE, eff_date DESC) RNK
-- MAGIC FROM jay_catalog.sas.LOCATION_DIM lo
-- MAGIC WHERE eff_date <= TO_TIMESTAMP ('{prev_d}', 'ddMMyyyy') 
-- MAGIC --and end_date>= to_date ({prev_d}) 
-- MAGIC and CONTRACT_TYPE_CODE not in ('3J1','3P0') and CONTRACT_GROUP_CODE <> '3'
-- MAGIC AND LOCATION_SOURCE  = 'SS' ) WHERE RNK=1) where homes_passed_flg = 'P' and HOMES_PASSED_COUNT>=1 and (dummy_addr_ind <> 'Y')
-- MAGIC order by sam_key
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC print(file_date)

-- COMMAND ----------

select * from jay_mehta_catalog.sas.DAILY_SERV_T_BASE

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql("""create or replace table serv_t_SHM as 
-- MAGIC select * from jay_mehta_catalog.sas.DAILY_SERV_T_BASE where to_date(PROCESS_DATE,"ddMMMyyyy:HH:mm")  =  date_sub(to_date('{file_date}',"ddMMyyyy"), 1)
-- MAGIC and PRODUCT <> 'WHI'
-- MAGIC """)

-- COMMAND ----------

select * from serv_t_SHM

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW adds AS
SELECT td.*
FROM HP_TODAY4 td
LEFT JOIN HP_YESTERDAY ytd ON td.sam_key = ytd.sam_key
WHERE ytd.sam_key IS NULL

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW removals AS
SELECT ytd.*
FROM HP_YESTERDAY ytd
LEFT JOIN HP_TODAY4 td ON ytd.sam_key = td.sam_key
WHERE td.sam_key IS NULL

-- COMMAND ----------

-- MAGIC %py
-- MAGIC print(cur_date_now)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""CREATE OR REPLACE TEMPORARY VIEW Daily_HH_HP_ADDS AS
-- MAGIC SELECT
-- MAGIC   to_date({cur_date_now},'yyyyMMdd') as Report_Date,
-- MAGIC   sam_key,
-- MAGIC   BULK_PROFILE_NAME,
-- MAGIC   CONTRACT_GROUP_CODE,
-- MAGIC   CONTRACT_TYPE_CODE,
-- MAGIC   HOMES_PASSED_COUNT,
-- MAGIC   SERVICEABILITY_CODE,
-- MAGIC   CABLE_TYPE,
-- MAGIC   RPATS_NUMBER,
-- MAGIC   RPATS_DESC,
-- MAGIC   PROJ_TYPE_IND,
-- MAGIC   FIRST_FLIP_DATE,
-- MAGIC   HOMES_PASSED_DATE,
-- MAGIC   EFF_DATE,
-- MAGIC   END_DATE,
-- MAGIC   street_number,
-- MAGIC   street_name,
-- MAGIC   STREET_TYPE,
-- MAGIC   COMPASS_DIR_CODE,
-- MAGIC   APARTMENT_NUMBER,
-- MAGIC   CITY_NAME,
-- MAGIC   DWELLING_TYPE_CODE,
-- MAGIC   postal_code,
-- MAGIC   Address_Key,
-- MAGIC   CONCAT(company, ' ', TRIM(street_number), ' ', TRIM(street_name), ' ', TRIM(STREET_TYPE), ' ', postal_code) as NEW_ADDRESS_KEY
-- MAGIC FROM adds
-- MAGIC WHERE substr(sam_key, 1, 1) <> '-'
-- MAGIC   """)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""CREATE OR REPLACE TEMPORARY VIEW Daily_HH_HP_REMOVALS AS
-- MAGIC SELECT
-- MAGIC   to_date({cur_date_now},'yyyyMMdd') as Report_Date,
-- MAGIC   sam_key,
-- MAGIC   BULK_PROFILE_NAME,
-- MAGIC   CONTRACT_GROUP_CODE,
-- MAGIC   CONTRACT_TYPE_CODE,
-- MAGIC   HOMES_PASSED_COUNT,
-- MAGIC   SERVICEABILITY_CODE,
-- MAGIC   CABLE_TYPE,
-- MAGIC   RPATS_NUMBER,
-- MAGIC   RPATS_DESC,
-- MAGIC   PROJ_TYPE_IND,
-- MAGIC   FIRST_FLIP_DATE,
-- MAGIC   HOMES_PASSED_DATE,
-- MAGIC   EFF_DATE,
-- MAGIC   END_DATE,
-- MAGIC   street_number,
-- MAGIC   street_name,
-- MAGIC   STREET_TYPE,
-- MAGIC   COMPASS_DIR_CODE,
-- MAGIC   APARTMENT_NUMBER,
-- MAGIC   CITY_NAME,
-- MAGIC   DWELLING_TYPE_CODE,
-- MAGIC   postal_code,
-- MAGIC   Address_Key,
-- MAGIC   CONCAT(company, ' ', TRIM(street_number), ' ', TRIM(street_name), ' ', TRIM(STREET_TYPE), ' ', postal_code) AS NEW_ADDRESS_KEY
-- MAGIC FROM removals""")
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC print(file_date)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""CREATE OR REPLACE TEMPORARY VIEW Daily_HH_dups_last30_days AS
-- MAGIC SELECT
-- MAGIC   process_date,
-- MAGIC   SUM(homes_passed_count) AS DUp_HP_count
-- MAGIC FROM
-- MAGIC   jay_mehta_catalog.sas.Dup_final
-- MAGIC WHERE
-- MAGIC   DATE(process_date) >= date_sub(to_date({file_date}, "ddMMyyyy"), 30)
-- MAGIC GROUP BY
-- MAGIC   process_date
-- MAGIC ORDER BY
-- MAGIC   process_date
-- MAGIC """)

-- COMMAND ----------

