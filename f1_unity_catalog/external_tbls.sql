-- Databricks notebook source
DROP TABLE IF EXISTS f1_dev.bronze.drivers;

CREATE TABLE IF NOT EXISTS f1_dev.bronze.drivers (
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path 'abfss://bronze@f1learningdl.dfs.core.windows.net/drivers.json');

-- COMMAND ----------

DROP TABLE IF EXISTS f1_dev.bronze.results;

CREATE TABLE IF NOT EXISTS f1_dev.bronze.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points FLOAT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId INT
)
USING json
OPTIONS (path 'abfss://bronze@f1learningdl.dfs.core.windows.net/results.json')