# Databricks notebook source
# MAGIC %sql
# MAGIC USE f1_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.calcualted_results;
# MAGIC CREATE TABLE f1_presentation.calcualted_results AS
# MAGIC SELECT 
# MAGIC races.race_year,
# MAGIC constructors.name as team_name,
# MAGIC drivers.name as driver_name,
# MAGIC results.position,
# MAGIC results.points,
# MAGIC 11-results.position as calculated_points
# MAGIC FROM 
# MAGIC results join drivers on results.driver_id=drivers.driver_id
# MAGIC join constructors on results.constructor_id=constructors.constructor_id
# MAGIC join races on results.race_id=races.race_id
# MAGIC where results.position<=10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.calcualted_results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dominant driver analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT driver_name,
# MAGIC count(*) as total_races,
# MAGIC sum(calculated_points) as total_points,
# MAGIC avg(calculated_points) as avg_points
# MAGIC FROM f1_presentation.calcualted_results
# MAGIC GROUP BY driver_name
# MAGIC HAVING count(*)>=50
# MAGIC order by avg_points desc

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW driver_rank_vw AS
# MAGIC SELECT driver_name,
# MAGIC count(*) as total_races,
# MAGIC sum(calculated_points) as total_points,
# MAGIC avg(calculated_points) as avg_points,
# MAGIC rank() OVER(ORDER BY avg(calculated_points) DESC) AS driver_rank
# MAGIC FROM f1_presentation.calcualted_results
# MAGIC GROUP BY driver_name
# MAGIC HAVING count(*)>=50
# MAGIC order by avg_points desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year,driver_name,
# MAGIC count(*) as total_races,
# MAGIC sum(calculated_points) as total_points,
# MAGIC avg(calculated_points) as avg_points
# MAGIC FROM f1_presentation.calcualted_results
# MAGIC where driver_name in (select driver_name from driver_rank_vw where driver_rank<=10)
# MAGIC GROUP BY race_year,driver_name
# MAGIC order by race_year ,avg_points desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dominant team analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team_name,
# MAGIC count(*) as total_races,
# MAGIC sum(calculated_points) as total_points,
# MAGIC avg(calculated_points) as avg_points
# MAGIC FROM f1_presentation.calcualted_results
# MAGIC GROUP BY team_name
# MAGIC HAVING count(*)>=100
# MAGIC order by avg_points desc

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW team_rank_vw AS
# MAGIC SELECT team_name,
# MAGIC count(*) as total_races,
# MAGIC sum(calculated_points) as total_points,
# MAGIC avg(calculated_points) as avg_points,
# MAGIC rank() OVER(ORDER BY avg(calculated_points) DESC) AS team_rank
# MAGIC FROM f1_presentation.calcualted_results
# MAGIC GROUP BY team_name
# MAGIC HAVING count(*)>=100
# MAGIC order by avg_points desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year,team_name,
# MAGIC count(*) as total_races,
# MAGIC sum(calculated_points) as total_points,
# MAGIC avg(calculated_points) as avg_points
# MAGIC FROM f1_presentation.calcualted_results
# MAGIC where team_name in (select team_name from team_rank_vw where team_rank<=10)
# MAGIC GROUP BY race_year,team_name
# MAGIC order by race_year ,avg_points desc