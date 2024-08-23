# Databricks notebook source
# DBTITLE 1,Set Catalog and Schema
# MAGIC %sql
# MAGIC use catalog mgiglia;
# MAGIC use schema hv_claims;

# COMMAND ----------

# DBTITLE 1,Set Volume
volume = "/Volumes/mgiglia/hv_claims/landing/patients/"

# COMMAND ----------

# DBTITLE 1,List Files In the Volume
import subprocess

cmd = f"ls -alt {volume}"
result = subprocess.run(cmd, shell=True, capture_output=True)
print(result.stdout.decode("utf-8"))

# COMMAND ----------

# DBTITLE 1,Cat Contents of a File
cmd = f"cat {volume}patients_2024_06_28T13_54_15Z.txt"
result = subprocess.run(cmd, shell=True, capture_output=True)
print(result.stdout.decode("utf-8"))

# COMMAND ----------

# DBTITLE 1,Read the Files into a Spark Dataframe
from pyspark.sql.functions import col, lit, current_timestamp

df = (
  spark
    .read.text(volume, wholetext=True)
    .withColumn("volume", lit(volume))
    .withColumn("file_metadata", col("_metadata"))
    .withColumn("file_name", col("file_metadata.file_name"))
    .withColumn("ingest_timestamp", current_timestamp())
    .select("volume", "file_name", "ingest_timestamp", "file_metadata", "value")
)

display(df)

# COMMAND ----------

# DBTITLE 1,Create Delta Table
df.write.mode("overwrite").saveAsTable("mgiglia.hv_claims.encrytped_source_bronze")

# COMMAND ----------

# DBTITLE 1,Select from the Table
# MAGIC %sql
# MAGIC SELECT * FROM encrytped_source_bronze;

# COMMAND ----------

# DBTITLE 1,Retrieve Encryption Key
key = dbutils.secrets.get('encryptionCLM-demo', 'encryption_key')
key

# COMMAND ----------

# DBTITLE 1,Review Available UDFs
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   * 
# MAGIC FROM 
# MAGIC   information_schema.routines 
# MAGIC WHERE  
# MAGIC   specific_schema = 'hv_claims'
# MAGIC ;

# COMMAND ----------

display(_sqldf.select("specific_name", "routine_definition", "external_language"))

# COMMAND ----------

# DBTITLE 1,Use  the Decrypt Text SQL UDF to Return the Data
from pyspark.sql.functions import expr

df = (spark
      .table("mgiglia.hv_claims.encrytped_source_bronze")
      .withColumn("decrypted_value", expr("decrypt_text(value, secret('encryptionCLM-demo', 'encryption_key'))"))
    )
display(df)

# COMMAND ----------

# DBTITLE 1,Parse CSV Data Into Rows
from pyspark.sql.functions import explode, split, col

df_csv = (
  df.withColumn("csv_data", split(col("decrypted_value"), "\n"))
  .withColumn("csv_data", explode("csv_data"))
  .filter(~col("csv_data").startswith("Id,BIRTHDATE"))
)

display(df_csv)

# COMMAND ----------

# DBTITLE 1,Define DDL for the CSV Data
ddl = f"""struct<patient_id:string,birth_date:date,death_date:date,ssn:string,drivers:string,passport:string,prefix:string,first:string,middle:string,last:string,suffix:string,maiden:string,marital:string,race:string,ethnicity:string,gender:string,birth_place:string,address:string,city:string,state:string,county:string,fips:string,zip:string,lat:double,lon:double,healthcare_expenses:double,healthcare_coverage:double,income:int>"""

# COMMAND ----------

# DBTITLE 1,Define the Explode and Split Function
def explode_and_split(df):
  newDF = df
  for colName in df.columns:
    colType = df.schema[colName].dataType
    if isinstance(colType, ArrayType):
      newDF = newDF.withColumn(colName, explode_outer(col(colName)))
      # newDF = explodeAndSplit(newDF)  # Recurse if column is an array
    elif isinstance(colType, StructType):
      for field in colType.fields:
          fieldName = field.name
          newDF = newDF.withColumn(f"{fieldName}", col(f"{colName}.{fieldName}"))
      newDF = newDF.drop(colName)
      # newDF = explodeAndSplit(newDF)  # Recurse if column is a struct
  return newDF

# COMMAND ----------

# DBTITLE 1,Convert CSV Data into a Dataframe
from pyspark.sql.functions import from_csv
from pyspark.sql.types import *


data = df_csv.withColumn("data", from_csv(col("csv_data"), schema=ddl)).alias("data").select("data")
data = explode_and_split(data)
display(data)
