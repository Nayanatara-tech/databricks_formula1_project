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

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed_incre_ld;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data from RAW layer and loading in Processed Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Circuits file CSV - Full load

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
    .load(f"abfss://raw-incremental-load@f1learningdl.dfs.core.windows.net/{file_date}/circuits.csv")

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
circuits_final_df=circuits_final_df.withColumn("file_date",lit(file_date))

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("abfss://processed-incremental-load@f1learningdl.dfs.core.windows.net/processed/circuits")
processed_circuits_df=spark.read.parquet("abfss://processed-incremental-load@f1learningdl.dfs.core.windows.net/processed/circuits")

processed_circuits_df.write.mode("overwrite").saveAsTable("f1_processed_incre_ld.circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Converting parquet files to delta
# MAGIC to convert table - CONVERT INTO DELTA tbl_name;
# MAGIC in this code already all the tables are stored as delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`abfss://processed-incremental-load@f1learningdl.dfs.core.windows.net/processed/circuits`
# MAGIC
# MAGIC --now you will ahve delta logs in adls for the above path folder

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED f1_processed_incre_ld.circuits

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
    .load(f"abfss://raw-incremental-load@f1learningdl.dfs.core.windows.net/{file_date}/races.csv")



    

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
races_final_df=races_final_df.withColumn("file_date",lit(file_date))

# COMMAND ----------

races_final_df.write.mode("overwrite").parquet("abfss://processed-incremental-load@f1learningdl.dfs.core.windows.net/processed/races")
processed_races_df=spark.read.parquet("abfss://processed-incremental-load@f1learningdl.dfs.core.windows.net/processed/races")

processed_races_df.write.mode("overwrite").saveAsTable("f1_processed_incre_ld.races")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Constructors file JSON

# COMMAND ----------

constructors_schema="constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

constructor_raw_df=spark.read.format("json")\
    .schema(constructors_schema)\
    .load(f"abfss://raw-incremental-load@f1learningdl.dfs.core.windows.net/{file_date}/constructors.json")


# COMMAND ----------

constructor_df=constructor_raw_df.drop('url')
constructor_final_df=constructor_df.withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("constructorRef","constructor_ref")\
    .withColumn("ingestion_date",current_timestamp())
    
constructor_final_df=add_source_col(constructor_final_df,source_input)
constructor_final_df=constructor_final_df.withColumn("file_date",lit(file_date))

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("abfss://processed-incremental-load@f1learningdl.dfs.core.windows.net/processed/constructors")
processed_constructors_df=spark.read.parquet("abfss://processed-incremental-load@f1learningdl.dfs.core.windows.net/processed/constructors")
processed_constructors_df.write.mode("overwrite").saveAsTable("f1_processed_incre_ld.constructors")


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
    .load(f"abfss://raw-incremental-load@f1learningdl.dfs.core.windows.net/{file_date}/drivers.json")


# COMMAND ----------

drivers_df=drivers_raw_df.drop('url')
drivers_final_df=drivers_df.withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("driverRef","driver_ref")\
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))

drivers_final_df=add_source_col(drivers_final_df,source_input)
drivers_final_df=drivers_final_df.withColumn("file_date",lit(file_date))

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("abfss://processed-incremental-load@f1learningdl.dfs.core.windows.net/processed/drivers")
processed_drivers_df=spark.read.parquet("abfss://processed-incremental-load@f1learningdl.dfs.core.windows.net/processed/drivers")
processed_drivers_df.write.mode("overwrite").saveAsTable("f1_processed_incre_ld.drivers")

