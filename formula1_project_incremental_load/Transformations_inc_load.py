# Databricks notebook source
# MAGIC %run "../formula1_project_full_load/config"

# COMMAND ----------

# MAGIC %run "../formula1_project_full_load/functions"

# COMMAND ----------

dbutils.widgets.text("file_date","")
file_date=dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_presantation_incre_ld;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading from processed layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Races

# COMMAND ----------

processed_circuits_df=spark.sql("SELECT * FROM f1_processed_incre_ld.circuits")
processed_circuits_df=processed_circuits_df.withColumnRenamed("location","circuit_location")

processed_races_df=spark.sql("SELECT * FROM f1_processed_incre_ld.races")
processed_races_df=processed_races_df.withColumnRenamed("name","race_name")\
    .withColumnRenamed("number","driver_number")\
    .withColumnRenamed("race_timestamp","race_date")

processed_constructors_df=spark.sql("SELECT * FROM f1_processed_incre_ld.constructors")
processed_constructors_df=processed_constructors_df.withColumnRenamed("name","team")

processed_drivers_df=spark.sql("SELECT * FROM f1_processed_incre_ld.drivers")
processed_drivers_df=processed_drivers_df.withColumnRenamed("number","driver_number")\
    .withColumnRenamed("name","driver_name")\
    .withColumnRenamed("nationality","driver_nationality")

processed_results_df=spark.sql("SELECT * FROM f1_processed_incre_ld.results")
processed_results_df=processed_results_df.withColumnRenamed("time","races_time").filter(f"file_date='{file_date}'").withColumnRenamed("race_id","results_race_id").withColumnRenamed("file_date","results_file_date")



# COMMAND ----------

from pyspark.sql.functions import current_timestamp,desc

# COMMAND ----------

races_circuits_df=processed_races_df.join(processed_circuits_df,processed_races_df.circuit_id==processed_circuits_df.circuit_id)\
    .select(processed_races_df.race_id,processed_races_df.race_year,processed_races_df.race_name,processed_races_df.race_date,processed_circuits_df.circuit_location)

race_results_df=processed_results_df.join(races_circuits_df,processed_results_df.results_race_id==races_circuits_df.race_id)\
    .join(processed_drivers_df,processed_results_df.driver_id==processed_drivers_df.driver_id)\
    .join(processed_constructors_df,processed_results_df.constructor_id==processed_constructors_df.constructor_id)

race_results_df=race_results_df.select("race_id","results_file_date","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","races_time","points","position")\
    .withColumn("created_date",current_timestamp())\
    .withColumnRenamed("results_file_date","file_date")

    
    


# COMMAND ----------

overwrite_data_incre_load(race_results_df,'race_id','f1_presantation_incre_ld','race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,count(*) from f1_presantation_incre_ld.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Driver standings

# COMMAND ----------

race_results_final_df=spark.sql("SELECT * FROM f1_presantation_incre_ld.race_results")
race_results_list=race_results_final_df.filter(f"file_date='{file_date}'").select("race_year").distinct().collect()
race_year_list=[]
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)
    
race_results_df=race_results_final_df.filter(f"race_year in ({', '.join(map(str, race_year_list))})")


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

overwrite_data_incre_load(driver_standings_df,'race_year','f1_presantation_incre_ld','driver_standings')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Constructor Standings

# COMMAND ----------

constructor_standings_df=race_results_df.groupBy("race_year","team").agg(sum("points").alias("total_points"),count(when(col("position")==1, True)).alias("wins"))
windowspec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
constructor_standings_df=constructor_standings_df.withColumn("rank",rank().over(windowspec))
display(constructor_standings_df)

# COMMAND ----------

overwrite_data_incre_load(constructor_standings_df,'race_year','f1_presantation_incre_ld','constructor_standings')
