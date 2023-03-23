# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Demo 
# MAGIC ***
# MAGIC 1. Create source data as JSON (usually get it via data ingestion process as a batch data or stream data)
# MAGIC 2. Read source data to spark dataframe
# MAGIC 3. Transformed data and simplify the data schema
# MAGIC 4. Persist the simplified version of data 
# MAGIC 5. Read the data from storage

# COMMAND ----------

import pyspark.pandas as ps
import pandas as pd
import json
from pyspark.sql.functions import explode

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Test Data with complex json nested structure 
# MAGIC ***

# COMMAND ----------

# create payloads 
payload1 = {"EmpId": "A01", "IsPermanent": True, "Department": [{"DepartmentID": "D1", "DepartmentName": "Data Science"}]}
payload2 = {"EmpId": "A02", "IsPermanent": False, "Department": [{"DepartmentID": "D2", "DepartmentName": "Application"}]}
payload3 = {"EmpId": "A03", "IsPermanent": True, "Department": [{"DepartmentID": "D1", "DepartmentName": "Data Science"}]}
payload4 = {"EmpId": "A04", "IsPermanent": False, "Department": [{"DepartmentID": "D2", "DepartmentName": "Application"}]}

# create data structure
data =[
  {"EventID": 1, "Payload": payload1},
  {"EventID": 2, "Payload": payload2},
  {"EventID": 3, "Payload": payload3},
  {"EventID": 4, "Payload": payload4}
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Get data to the spark dataframe
# MAGIC ***

# COMMAND ----------

# dump data to json 
jsonData = json.dumps(data)

# append json data to list 
jsonDataList = []
jsonDataList.append(jsonData)

# parallelize json data
jsonRDD = sc.parallelize(jsonDataList)

# store data to spark dataframe 
df = spark.read.json(jsonRDD)

# Show data
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Check original data schema 
# MAGIC ***

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Explode data to make data schema simple
# MAGIC ***

# COMMAND ----------

df1 = df.selectExpr("EventID", "Payload.EmpId", "Payload.IsPermanent","explode(Payload.Department) as Department")
df1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Further simplifies the schema
# MAGIC ***

# COMMAND ----------

df2 = df1.selectExpr("EventID", "EmpId", "IsPermanent","Department.DepartmentID", "Department.DepartmentName")
df2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Save the transformed data to the storage
# MAGIC ***

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

tbl_name = "/delta/employee_tbl"

# Perform write operation
(df2.write
  .mode("overwrite")
  .format("delta")
  .option("overwriteSchema", "true")
  .save(tbl_name))

# List all delta tables
display(dbutils.fs.ls("./delta/"))

# COMMAND ----------

# display data inside the employee_tbl
display(spark.read.load("/delta/employee_tbl"))

# COMMAND ----------

display(dbutils.fs.ls("./delta/employee_tbl/"))

# COMMAND ----------

display(dbutils.fs.ls("./delta/employee_tbl/_delta_log"))

# COMMAND ----------

dbutils.fs.rm("./delta/employee_tbl/",True)
