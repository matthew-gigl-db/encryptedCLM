# Databricks notebook source
# MAGIC %sql 
# MAGIC
# MAGIC use catalog mgiglia;
# MAGIC use schema hv_claims;
# MAGIC
# MAGIC create volume if not exists landing;

# COMMAND ----------

path = "/Volumes/mgiglia/synthea/synthetic_files_raw/output/csv/2024_06_28T13_54_15Z/"
date = "2024_06_28T13_54_15Z"

# COMMAND ----------

from pyspark.sql.functions import input_file_name, lit, col, expr

# Read the raw CSV file into a Spark DataFrame
df = spark.read.text(f"{path}patients.csv", wholetext=True) \
  .withColumn("metadata", col("_metadata")) \
  .withColumn("encrypted", expr("mgiglia.hv_claims.encrypt_text(value, secret('encryptionCLM-demo', 'encryption_key'))"))

display(df)



# COMMAND ----------

encrypted_string = df.select("encrypted").collect()[0][0]


# Write the string to a text file
dbutils.fs.put(f"/Volumes/mgiglia/hv_claims/landing/patients/patients_{date}.txt", encrypted_string, overwrite=True)

# COMMAND ----------

# MAGIC %sh 
# MAGIC cat /Volumes/mgiglia/hv_claims/landing/patients/patients_2024_06_28T13_42_59Z.txt
