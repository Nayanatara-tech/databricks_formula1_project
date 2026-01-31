# Databricks notebook source
dbutils.widgets.text("file_date","")
file_date=dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating target table

# COMMAND ----------

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS f1_presentation_incre_ld_delta_mrg.calcualted_results
            (race_year INT,
            race_id INT,
            team_name STRING,
            driver_id INT,
            driver_name STRING,
            position INT,
            points INT,
            calculated_points INT,
            created_date TIMESTAMP,
            updated_date TIMESTAMP)"""
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ### creating temp view with the records to updated or insert

# COMMAND ----------

spark.sql(f"""
        CREATE or REPLACE TEMP VIEW update_results AS
        SELECT 
        races.race_year,
        races.race_id,
        constructors.name as team_name,
        drivers.driver_id,
        drivers.name as driver_name,
        results.position,
        results.points,
        11-results.position as calculated_points
        FROM 
        f1_processed_incre_ld_delta_mrg.results results join f1_processed_incre_ld.drivers drivers on results.driver_id=drivers.driver_id
        join f1_processed_incre_ld.constructors constructors on results.constructor_id=constructors.constructor_id
        join f1_processed_incre_ld.races races on results.race_id=races.race_id
        where results.position<=10
        and results.file_date='{file_date}'
         """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge operation upsert using sql syntax

# COMMAND ----------

spark.sql(f"""
          MERGE INTO f1_presentation_incre_ld_delta_mrg.calcualted_results tgt
          USING update_results src
          ON tgt.race_id=src.race_id and tgt.driver_id=src.driver_id
          WHEN MATCHED THEN
          UPDATE SET 
                tgt.updated_date= current_timestamp(),
                tgt.position=src.position,
                tgt.points=src.points,
                tgt.calculated_points=src.calculated_points
          WHEN NOT MATCHED
          THEN INSERT 
          (race_year,race_id,team_name,driver_id,driver_name,position,points,calculated_points,created_date)
          VALUES (race_year,race_id,team_name,driver_id,driver_name,position,points,calculated_points,current_timestamp())""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### checks

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM update_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM f1_presentation_incre_ld_delta_mrg.calcualted_results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking history,time travel,versioning and vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_presentation_incre_ld_delta_mrg.calcualted_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation_incre_ld_delta_mrg.calcualted_results VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation_incre_ld_delta_mrg.calcualted_results TIMESTAMP AS OF '2026-01-18T15:18:45.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_presentation_incre_ld_delta_mrg.calcualted_results
# MAGIC --IF YOU GIVE RETAIN 0 HOURS IT WOULD WIPE OFF THE HISTORY IMMEDIATELY ELSE BY DEFAULT IT WILL DELETE BEFORE 7 DAYS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation_incre_ld_delta_mrg.calcualted_results TIMESTAMP AS OF '2026-01-18T15:18:45.000+00:00'