# Databricks notebook source
# MAGIC %md
# MAGIC ###Calling Config ntbk

# COMMAND ----------

# MAGIC %run "../formula1_project_full_load/config"

# COMMAND ----------

# MAGIC %run "../formula1_project_full_load/functions"

# COMMAND ----------

dbutils.widgets.text("source","")
source_input=dbutils.widgets.get("source")

# COMMAND ----------

dbutils.widgets.text("file_date","")
file_date=dbutils.widgets.get("file_date")

# COMMAND ----------

file_date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed_incre_ld_delta_mrg

# COMMAND ----------

# MAGIC %sql
# MAGIC USE f1_processed_incre_ld_delta_mrg;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data from RAW layer and loading in Processed Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results File JSON With partition - Incremental load using delta_mrg
# MAGIC

# COMMAND ----------

from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType
from pyspark.sql.functions import col,current_timestamp,to_timestamp,lit,concat

# COMMAND ----------

results_schema=StructType(fields=[StructField("resultId",IntegerType(),False),
                       StructField("raceId",IntegerType(),True),
                       StructField("driverId",IntegerType(),True),
                       StructField("constructorId",IntegerType(),True),
                       StructField("number",IntegerType(),True),
                       StructField("grid",IntegerType(),True),
                       StructField("position",IntegerType(),True),
                       StructField("positionText",StringType(),True),
                       StructField("positionOrder",IntegerType(),True),
                       StructField("points",FloatType(),True),
                       StructField("laps",IntegerType(),True),
                       StructField("time",StringType(),True),
                       StructField("milliseconds",IntegerType(),True),
                       StructField("fastestLap",IntegerType(),True),
                       StructField("rank",IntegerType(),True),
                       StructField("fastestLapTime",StringType(),True),
                       StructField("fastestLapSpeed",FloatType(),True),
                       StructField("statusId",StringType(),True)])
                       
results_raw_df=spark.read.format("json")\
    .schema(results_schema)\
    .load(f"abfss://raw-incremental-load@f1learningdl.dfs.core.windows.net/{file_date}/results.json")

# COMMAND ----------

results_df=results_raw_df.withColumnRenamed("resultId","result_id")\
    .withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("positionText","position_text")\
    .withColumnRenamed("positionOrder","position_order")\
    .withColumnRenamed("fastestLap","fastest_lap")\
    .withColumnRenamed("fastestLapTime","fastest_lap_time")\
    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
    .withColumn("ingestion_date",current_timestamp())

results_final_df=results_df.drop('statusId')

results_final_df=add_source_col(results_final_df,source_input)
results_final_df=results_final_df.withColumn("file_date",lit(file_date))


# COMMAND ----------

#to remove duplicates
results_final_df=results_final_df.dropDuplicates(["race_id","driver_id"])

# COMMAND ----------

#setting the config for dynamic partitioning so that performance is better and spark knows which partition to update 
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", True)

#importing deltatable 
from delta.tables import DeltaTable

#this creates table in the first load then the if stmt works in the subsequent loads checking whether to update or insert new records with the primary key or keys(if it is composite) and partition column
#upsert,delete are possible only in delta tables not in parquet files
if spark.catalog.tableExists("f1_processed_incre_ld_delta_mrg.results"): 
    deltaTable = DeltaTable.forName(spark, "f1_processed_incre_ld_delta_mrg.results")
    deltaTable.alias("tgt")\
        .merge(
            results_final_df.alias("src"),
            "tgt.result_id = src.result_id AND tgt.race_id=src.race_id")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    results_final_df.write.mode("overwrite").partitionBy("race_id").saveAsTable("f1_processed_incre_ld_delta_mrg.results")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,driver_id,count(*)
# MAGIC from f1_processed_incre_ld_delta_mrg.results
# MAGIC GROUP BY race_id,driver_id
# MAGIC HAVING count(*)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,count(*) from f1_processed_incre_ld_delta_mrg.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pitstops file MultiLine JSON - Incremental load Method2 with functions

# COMMAND ----------

pitstops_schema=StructType(fields=[StructField("raceId",IntegerType(),True),
                       StructField("driverId",IntegerType(),True),
                       StructField("stop",IntegerType(),True),
                       StructField("lap",IntegerType(),True),
                       StructField("time",StringType(),True),
                       StructField("duration",StringType(),True),
                       StructField("milliseconds",IntegerType(),True)])

pitstops_raw_df=spark.read.format("json")\
    .schema(pitstops_schema)\
    .option("multiLine",True)\
    .load(f"abfss://raw-incremental-load@f1learningdl.dfs.core.windows.net/{file_date}/pit_stops.json")
    

# COMMAND ----------

pitstops_final_df=pitstops_raw_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumn("ingestion_date",current_timestamp())

pitstops_final_df=add_source_col(pitstops_final_df,source_input)
pitstops_final_df=pitstops_final_df.withColumn("file_date",lit(file_date))

# COMMAND ----------

merge_condition="tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id=src.race_id"
delta_merge(pitstops_final_df,'race_id','f1_processed_incre_ld_delta_mrg','pitstops',merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,count(*) from f1_processed_incre_ld_delta_mrg.pitstops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lap times CSV folder
# MAGIC

# COMMAND ----------

laptimes_schema=StructType(fields=[StructField("raceId",IntegerType(),True),
                       StructField("driverId",IntegerType(),True),
                       StructField("lap",IntegerType(),True),
                       StructField("position",IntegerType(),True),
                       StructField("time",StringType(),True),
                       StructField("milliseconds",IntegerType(),True)])
                    
laptimes_raw_df=spark.read.format("csv")\
    .schema(laptimes_schema)\
    .option("header",True)\
    .load(f"abfss://raw-incremental-load@f1learningdl.dfs.core.windows.net/{file_date}/lap_times")

# COMMAND ----------

laptimes_final_df=laptimes_raw_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumn("ingestion_date",current_timestamp())

laptimes_final_df=add_source_col(laptimes_final_df,source_input)
laptimes_final_df=laptimes_final_df.withColumn("file_date",lit(file_date))

# COMMAND ----------

merge_condition="tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id=src.race_id"
delta_merge(laptimes_final_df,'race_id','f1_processed_incre_ld_delta_mrg','lap_times',merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,count(*) from f1_processed_incre_ld_delta_mrg.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Qualifying Multiline JSON Folder

# COMMAND ----------

qualifying_schema=StructType(fields=[StructField("qualifyId",IntegerType(),True),
                       StructField("raceId",IntegerType(),True),
                       StructField("driverId",IntegerType(),True),
                       StructField("constructorId",IntegerType(),True),
                       StructField("number",IntegerType(),True),
                       StructField("position",IntegerType(),True),
                       StructField("q1",StringType(),True),
                       StructField("q2",StringType(),True),
                       StructField("q3",StringType(),True)])

qualifying_raw_df=spark.read.format("json")\
    .schema(qualifying_schema)\
    .option("multiLine",True)\
    .load(f"abfss://raw-incremental-load@f1learningdl.dfs.core.windows.net/{file_date}/qualifying")

# COMMAND ----------

qualifying_final_df=qualifying_raw_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("qualifyId","qualify_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumn("ingestion_date",current_timestamp())

qualifying_final_df=add_source_col(qualifying_final_df,source_input)
qualifying_final_df=qualifying_final_df.withColumn("file_date",lit(file_date))

# COMMAND ----------

merge_condition="tgt.qualify_id = src.qualify_id AND tgt.race_id=src.race_id"
delta_merge(qualifying_final_df,'race_id','f1_processed_incre_ld_delta_mrg','qualifying',merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,count(*) from f1_processed_incre_ld_delta_mrg.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC