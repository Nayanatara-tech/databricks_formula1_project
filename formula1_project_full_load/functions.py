# Databricks notebook source
from pyspark.sql.functions import lit

# COMMAND ----------

def add_source_col(df,source):
    return df.withColumn("source",lit(source))

# COMMAND ----------

def col_rearrange(input_df,partition_col):
    col_list=[]
    for col in input_df.columns:
        if col != partition_col:
            col_list.append(col)
    col_list.append(partition_col)
    return input_df.select(col_list)

# COMMAND ----------

def overwrite_data_incre_load(input_df,partition_col,db_name,tbl_name):
  
  df=col_rearrange(input_df,partition_col)

  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

  if spark.catalog.tableExists(f"{db_name}.{tbl_name}"): #BY DEFAULT INSERTINTO TAKES THE LAST COLUMN IN THE DATAFRAME AS partition column
      df.write.mode("overwrite").insertInto(f"{db_name}.{tbl_name}")  # overwrites only partitions present in the DF - a table should be already present for this that is why we have the else part in which it creates for the first load
  else:
      df.write.mode("overwrite").partitionBy(partition_col).saveAsTable(f"{db_name}.{tbl_name}")


# COMMAND ----------

def delta_merge(input_df,partition_col,db_name,tbl_name,merge_condition):

    #setting the config for dynamic partitioning so that performance is better and spark knows which partition to update 
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", True)

    #importing deltatable 
    from delta.tables import DeltaTable

    #this creates table in the first load then the if stmt works in the subsequent loads checking whether to update or insert new records with the primary key and partition column
    #upsert,delete are possible only in delta tables not in parquet files
    if spark.catalog.tableExists(f"{db_name}.{tbl_name}"): 
        deltaTable = DeltaTable.forName(spark, f"{db_name}.{tbl_name}")
        deltaTable.alias("tgt")\
            .merge(
                input_df.alias("src"),
                merge_condition)\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_col).saveAsTable(f"{db_name}.{tbl_name}")
