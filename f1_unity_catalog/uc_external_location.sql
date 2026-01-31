-- Databricks notebook source
CREATE EXTERNAL LOCATION IF NOT EXISTS adls_bronze
URL "abfss://bronze@f1learningdl.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL uc_adls_credential);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS adls_silver
URL "abfss://silver@f1learningdl.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL uc_adls_credential);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS adls_gold
URL "abfss://gold@f1learningdl.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL uc_adls_credential);