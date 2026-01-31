# Databricks notebook source
# MAGIC %run "./config"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading from processed layer

# COMMAND ----------

processed_pitstops_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/pitstops")
processed_laptimes_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/lap_times")
processed_qualifying_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/qualifying")


# COMMAND ----------

processed_circuits_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/circuits")\
    .withColumnRenamed("location","circuit_location")

processed_races_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/races")\
    .withColumnRenamed("name","race_name")\
    .withColumnRenamed("number","driver_number")\
    .withColumnRenamed("race_timestamp","race_date")

processed_constructors_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/constructors").withColumnRenamed("name","team")

processed_drivers_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/drivers")\
    .withColumnRenamed("number","driver_number")\
    .withColumnRenamed("name","driver_name")\
    .withColumnRenamed("nationality","driver_nationality")

processed_results_df=spark.read.parquet("abfss://processed@f1learningdl.dfs.core.windows.net/processed/results")\
    .withColumnRenamed("time","races_time")



# COMMAND ----------

from pyspark.sql.functions import current_timestamp,desc

# COMMAND ----------

races_circuits_df=processed_races_df.join(processed_circuits_df,processed_races_df.circuit_id==processed_circuits_df.circuit_id)\
    .select(processed_races_df.race_id,processed_races_df.race_year,processed_races_df.race_name,processed_races_df.race_date,processed_circuits_df.circuit_location)

race_results_df=processed_results_df.join(races_circuits_df,processed_results_df.race_id==races_circuits_df.race_id)\
    .join(processed_drivers_df,processed_results_df.driver_id==processed_drivers_df.driver_id)\
    .join(processed_constructors_df,processed_results_df.constructor_id==processed_constructors_df.constructor_id)

race_results_df=race_results_df.select("race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","races_time","points","position")\
    .withColumn("created_date",current_timestamp())

race_results_df.write.mode("overwrite").parquet("abfss://presentation@f1learningdl.dfs.core.windows.net/presentation/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum,when,count,col,rank
from pyspark.sql.window import Window

# COMMAND ----------

driver_standings_df=race_results_df.groupBy("race_year","driver_name","driver_nationality","team").agg(sum("points").alias("total_points"),count(when(col("position")==1, True)).alias("wins"))

# COMMAND ----------

windowspec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
driver_standings_df=driver_standings_df.withColumn("rank",rank().over(windowspec))
display(driver_standings_df)

# COMMAND ----------

driver_standings_df.write.mode("overwrite").format("parquet").save("abfss://presentation@f1learningdl.dfs.core.windows.net/presentation/driver_standings")

# COMMAND ----------

constructor_standings_df=race_results_df.groupBy("race_year","team").agg(sum("points").alias("total_points"),count(when(col("position")==1, True)).alias("wins"))
windowspec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
constructor_standings_df=constructor_standings_df.withColumn("rank",rank().over(windowspec))
display(constructor_standings_df)

# COMMAND ----------

constructor_standings_df.write.mode("overwrite").format("parquet").save("abfss://presentation@f1learningdl.dfs.core.windows.net/presentation/constructor_standings")