# Databricks notebook source
# MAGIC %run "./data_ingestion"

# COMMAND ----------

# MAGIC %run "./Transformations"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ### save as managed tables in hive metastore

# COMMAND ----------

circuits_raw_df.write.mode("overwrite").saveAsTable("f1_raw.circuits")
races_raw_df.write.mode("overwrite").saveAsTable("f1_raw.races")
constructor_raw_df.write.mode("overwrite").saveAsTable("f1_raw.constructors")
drivers_raw_df.write.mode("overwrite").saveAsTable("f1_raw.drivers")
results_raw_df.write.mode("overwrite").saveAsTable("f1_raw.results")
laptimes_raw_df.write.mode("overwrite").saveAsTable("f1_raw.lap_times")
pitstops_raw_df.write.mode("overwrite").saveAsTable("f1_raw.pit_stops")
qualifying_raw_df.write.mode("overwrite").saveAsTable("f1_raw.qualifying")


# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED f1_raw.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed;

# COMMAND ----------

processed_circuits_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("f1_processed.circuits")
processed_races_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("f1_processed.races")
processed_constructors_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("f1_processed.constructors")
processed_drivers_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("f1_processed.drivers")
processed_results_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("f1_processed.results")
processed_pitstops_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("f1_processed.pitstops")
processed_laptimes_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("f1_processed.laptimes")
processed_qualifying_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_presentation;

# COMMAND ----------

race_results_df.write.mode("overwrite").saveAsTable("f1_presentation.race_results")
driver_standings_df.write.mode("overwrite").saveAsTable("f1_presentation.driver_standings")
constructor_standings_df.write.mode("overwrite").saveAsTable("f1_presentation.constructor_standings")