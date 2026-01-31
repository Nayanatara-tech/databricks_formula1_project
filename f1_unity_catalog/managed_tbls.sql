-- Databricks notebook source
DROP TABLE IF EXISTS f1_dev.silver.drivers;

CREATE TABLE IF NOT EXISTS f1_dev.silver.drivers 
AS SELECT * FROM f1_dev.bronze.drivers

-- COMMAND ----------

SELECT * FROM f1_dev.silver.drivers 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_dev.silver.results;

CREATE TABLE IF NOT EXISTS f1_dev.silver.results 
AS SELECT * FROM f1_dev.bronze.results

-- COMMAND ----------

DROP TABLE IF EXISTS f1_dev.gold.driver_wins;

CREATE TABLE IF NOT EXISTS f1_dev.gold.driver_wins 
AS (SELECT 
  d.driverid,
  COUNT(*) AS wins
FROM f1_dev.silver.drivers d join f1_dev.silver.results r ON r.driverid = d.driverid
WHERE r.position = 1
GROUP BY d.driverid)
