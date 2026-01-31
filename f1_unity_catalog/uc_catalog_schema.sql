-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS f1_dev MANAGED LOCATION 'abfss://bronze@f1learningdl.dfs.core.windows.net/';

-- COMMAND ----------

USE CATALOG f1_dev;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION 'abfss://bronze@f1learningdl.dfs.core.windows.net/';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION 'abfss://silver@f1learningdl.dfs.core.windows.net/';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION 'abfss://gold@f1learningdl.dfs.core.windows.net/';