# Databricks notebook source
# MAGIC %md
# MAGIC ###Calling Config ntbk

# COMMAND ----------

# MAGIC %run "./config"

# COMMAND ----------

# MAGIC %run "./functions"

# COMMAND ----------

dbutils.widgets.text("source","")
source_input=dbutils.widgets.get("source")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data from RAW layer and loading in Processed Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Circuits file CSV

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType
from pyspark.sql.functions import col,current_timestamp,to_timestamp,lit,concat

# COMMAND ----------

#circuits file
circuits_schema=StructType(fields=[StructField("circuitId",IntegerType(),False),
                       StructField("circuitRef",StringType(),True),
                       StructField("name",StringType(),True),
                       StructField("location",StringType(),True),
                       StructField("country",StringType(),True),
                       StructField("lat",DoubleType(),True),
                       StructField("lng",DoubleType(),True),
                       StructField("alt",IntegerType(),True),
                       StructField("url",StringType(),True)])

circuits_raw_df=spark.read.format("csv")\
    .schema(circuits_schema)\
    .option("header",True)\
    .load("abfss://raw@f1learningdl.dfs.core.windows.net/raw/circuits.csv")

#display(circuits_raw_df)

    

# COMMAND ----------

circuits_df=circuits_raw_df.select(col("circuitId").alias("circuit_id"),
                                   col("circuitRef").alias("circuit_ref"),
                                   col("name"),
                                   col("location"),
                                   col("country"),
                                   col("lat").alias("latitude"),
                                   col("lng").alias("longitude"),
                                   col("alt").alias("altitude"))

circuits_final_df=circuits_df.withColumn("ingestion_date",current_timestamp())
circuits_final_df=add_source_col(circuits_final_df,source_input)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/circuits")
processed_circuits_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/circuits")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Races file CSV

# COMMAND ----------

#races file
races_schema=StructType(fields=[StructField("raceid",IntegerType(),False),
                       StructField("year",IntegerType(),True),
                       StructField("round",IntegerType(),True),
                       StructField("circuitid",IntegerType(),True),
                       StructField("name",StringType(),True),
                       StructField("date",StringType(),True),
                       StructField("time",StringType(),True),
                       StructField("url",StringType(),True)])

races_raw_df=spark.read.format("csv")\
    .schema(races_schema)\
    .option("header",True)\
    .load("abfss://raw@f1learningdl.dfs.core.windows.net/raw/races.csv")



    

# COMMAND ----------

races_df=races_raw_df.withColumn("ingestion_date",current_timestamp())\
    .withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss"))

races_final_df=races_df.select(col("raceid").alias("race_id"),
                             col("year").alias("race_year"),
                             col("round"),
                             col("circuitid").alias("circuit_id"),
                             col("name"),
                             col("race_timestamp"),
                             col("ingestion_date"))
races_final_df=add_source_col(races_final_df,source_input)

# COMMAND ----------

races_final_df.write.mode("overwrite").parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/races")
processed_races_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/races")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Constructors file JSON

# COMMAND ----------

constructors_schema="constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

constructor_raw_df=spark.read.format("json")\
    .schema(constructors_schema)\
    .load("abfss://raw@f1learningdl.dfs.core.windows.net/raw/constructors.json")

display(constructor_raw_df)


# COMMAND ----------

constructor_df=constructor_raw_df.drop('url')
constructor_final_df=constructor_df.withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("constructorRef","constructor_ref")\
    .withColumn("ingestion_date",current_timestamp())
    
constructor_final_df=add_source_col(constructor_final_df,source_input)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/constructors")
processed_constructors_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/constructors")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Drivers file Nested JSON

# COMMAND ----------

name_schema=StructType([StructField("forename", StringType(), True), StructField("surname", StringType(), True)])
drivers_schema=StructType(fields=[StructField("driverId",IntegerType(),False),
                       StructField("driverRef",StringType(),True),
                       StructField("number",IntegerType(),True),
                       StructField("code",StringType(),True),
                       StructField("name",name_schema),
                       StructField("dob",DateType(),True),
                       StructField("nationality",StringType(),True),
                       StructField("url",StringType(),True)])

drivers_raw_df=spark.read.format("json")\
    .schema(drivers_schema)\
    .load("abfss://raw@f1learningdl.dfs.core.windows.net/raw/drivers.json")


# COMMAND ----------

drivers_df=drivers_raw_df.drop('url')
drivers_final_df=drivers_df.withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("driverRef","driver_ref")\
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))

drivers_final_df=add_source_col(drivers_final_df,source_input)

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/drivers")
processed_drivers_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/drivers")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Results File JSON With partition

# COMMAND ----------

from pyspark.sql.types import FloatType

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
    .load("abfss://raw@f1learningdl.dfs.core.windows.net/raw/results.json")

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


# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/results")
processed_results_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/results")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Pitstops file MultiLine JSON

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
    .load("abfss://raw@f1learningdl.dfs.core.windows.net/raw/pit_stops.json")
    

# COMMAND ----------

pitstops_final_df=pitstops_raw_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumn("ingestion_date",current_timestamp())

pitstops_final_df=add_source_col(pitstops_final_df,source_input)

# COMMAND ----------

pitstops_final_df.write.mode("overwrite").parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/pitstops")

processed_pitstops_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/pitstops")

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
    .load("abfss://raw@f1learningdl.dfs.core.windows.net/raw/lap_times")

# COMMAND ----------

laptimes_final_df=laptimes_raw_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumn("ingestion_date",current_timestamp())

laptimes_final_df=add_source_col(laptimes_final_df,source_input)

# COMMAND ----------

laptimes_final_df.write.mode("overwrite").parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/lap_times")

processed_laptimes_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/lap_times")

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
    .load("abfss://raw@f1learningdl.dfs.core.windows.net/raw/qualifying")

# COMMAND ----------

qualifying_final_df=qualifying_raw_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("qualifyId","qualify_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumn("ingestion_date",current_timestamp())

qualifying_final_df=add_source_col(qualifying_final_df,source_input)

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/qualifying")

processed_qualifying_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/qualifying")