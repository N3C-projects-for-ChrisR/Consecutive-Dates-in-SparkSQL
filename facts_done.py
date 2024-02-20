#!/usr/bin/env python3
# Python layered over Spark SQL to show how to use window functions to
# find patients with runs of consecutive days of a particular attribute value.
# In this case we're looking for patients with 10-day runs of vaccine_administration == 1

# This is done so you can run it locally on a mac or pc using vanilla spark instead
# of in the enclave because it's usually faster. Past experience shows that it is
# the same dialect of SQL and it should run fine in the enclave.

import os
import shutil

try:
   shutil.rmtree("spark-warehouse/learn_spark_db.db/facts")
   print("INFO: deleted existing learn_spark_db database, so it can be installed fresh.")
except:
    print("INFO: nothing deleted, because nothing has been created yet.")
 
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName('learn_spark') \
    .getOrCreate()

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")
#spark.sql("DROP TABLE if exists learn_spark_db.facts")
csv_file = "facts.csv"
schema=" person_id INT, event_date DATE, obesity INT, pregnancy INT, tobacco INT, vaccine_administration int"
facts_df = spark.read.csv(csv_file, schema=schema)
facts_df.write.mode("overwrite").saveAsTable("facts")



sql='''
    WITH BASE as (
        SELECT person_id, event_date,

           datediff(event_date, 
               LAG(event_date) OVER (PARTITION BY person_id ORDER by event_date) 
           ) as gap,

           CASE WHEN datediff(event_date, 
                        LAG(event_date) OVER (PARTITION BY person_id ORDER by event_date) 
                     ) == 1 THEN 1
                ELSE 0
                END as within_run,

           CASE WHEN datediff(event_date, 
                        LAG(event_date) OVER (PARTITION BY person_id ORDER by event_date) 
                     ) == 1 THEN 0
                ELSE  1
                END as between_run 

        FROM facts
        WHERE vaccine_administration = 1
        ORDER by person_id, event_date 
    ),
    STEP_2 as (
        SELECT *,  
            sum(between_run) over (partition by person_id order by event_date) as run_number
        FROM BASE  
    ), 
    STEP_3 as (
        SELECT *,
            sum(within_run) over (partition by person_id, run_number order by event_date ) as step_number
        FROM STEP_2
    ) 

    SELECT * FROM STEP_3

    --SELECT person_id, event_date
    --FROM STEP_3
    --WHERE step_number = 9
'''

df = spark.sql(sql)
df.show(100);

spark.stop()


