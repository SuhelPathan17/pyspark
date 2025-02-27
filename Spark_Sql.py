#!/usr/bin/env python
# coding: utf-8
 
# In[31]:
 
 
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import expr, col, column, round, desc
from pyspark.sql.functions import lit, asc, count, sum, when, udf
from pyspark.sql.functions import max, avg, dense_rank, rank, row_number
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
import pandas as pd
 
 
# In[3]:
 
 
spark = SparkSession \
        .builder \
        .appName("Basic DF Operations") \
        .config("spark.master", "local[*]") \
        .getOrCreate()
 
 
# In[4]:
 
 
inputFilePath = "C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\data_git\\users.json"
 
 
# In[56]:
 
 
# Creating DataFrame
#read gives reader
#df1 = spark.read.format("json").load(inputFilePath)
 
#Another Way
#df1 = spark.read.load(inputFilePath, format="json")
 
#commenly used
 
df1 = spark.read.json(inputFilePath)
# types of objects in DF are row
df1.collect()
 
 
# In[57]:
 
 
df1.schema
 
 
# In[ ]:
 
 
 
 
 
# In[8]:
 
 
df1.dtypes
 
 
# In[9]:
 
 
# columns are in alphabetical order in DF
df1.columns
 
 
# In[10]:
 
 
# readable format
 
df1.show()
 
 
# In[11]:
 
 
df1.printSchema()
 
 
# In[12]:
 
 
# Transform DataFrame
# 1. Transformation Using DF Transformation methods
 
# 1. select
#df2 = df1.select("userid", "name", "age", "*")
df2 = df1.select("userid", "name", "age", "gender", "phone")
df2.show()
 
 
# In[13]:
 
 
# 2. where / filter  3. orderBy  
df2 = df1.select("userid", "name", "age", "gender", "phone") \
        .where("age is not null and gender='Male'") \
        .orderBy("gender","age")
df2.show()
 
 
# In[45]:
 
 
# 4. groupBy => does not return dataframe(use or apply aggreagtion to get DF)L
 
df2 = df1.select("userid", "name", "age", "gender", "phone") \
        .where("age is not null") \
        .orderBy("gender","age") \
        .groupBy("age") \
        .count().limit(4)
df2.show()
 
 
 
# In[14]:
 
 
# 2. Transformation Using SQL
spark.catalog
   
 
 
# In[15]:
 
 
spark.catalog.currentDatabase()
 
 
# In[16]:
 
 
spark.catalog.listTables()
 
 
# In[18]:
 
 
# creating in memory temprory view
df1.createOrReplaceTempView('users')
spark.catalog.listTables()
 
 
# In[19]:
 
 
# creating DF from table
df1 =spark.table("users")
 
 
# In[21]:
 
 
df1 = spark.sql("select * from users")
df1.show()
 
 
# In[23]:
 
 
query = """select age, count(1) as count from users
            where age is not null
            group by age  
            order by age  
            limit 4"""
df3 = spark.sql(query)
df3.show()
 
 
# In[46]:
 
 
# Saving DF's
output_path = "C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\output\\DF"
df3.write.format('json').save(output_path)
 
 
# In[47]:
 
 
df3.rdd.getNumPartitions()
 
 
# In[48]:
 
 
# Save Modes : control the behavior when writing to existing dir's.
df3.write.format('json').save(output_path, mode = 'Ignore')  # donot write to dir only ignore error
df3.write.format('json').save(output_path, mode = 'Append')  # append data in dir
 
 
# In[49]:
 
 
df3.write.format('json').save(output_path, mode = 'Overwrite')  # Overwrites data in dir
 
 
# In[51]:
 
 
# Local Tempviews and Global Tempviews
 
spark2 = spark.newSession()
df4 = spark2.sql(query)
 
 
# In[52]:
 
 
spark.catalog.listTables()
 
 
# In[53]:
 
 
spark2.catalog.listTables()  # spark2 session does not have users table due to local temp view
 
 
# In[59]:
 
 
df4 = spark2.read.json(inputFilePath)
df4.createOrReplaceTempView("users")
df4 = spark2.sql(query)
df4.show()
 
 
# In[71]:
 
 
# Dropping Temp View
spark2.catalog.dropTempView("users")
spark.catalog.dropTempView("users")
 
 
# In[ ]:
 
 
df4 = spark2.sql(query)  # won't run
 
 
# In[64]:
 
 
df4.createOrReplaceTempView("users2")
query = """select age, count(1) as count from users2
            where age is not null
            group by age  
            order by age  
            limit 4"""
df4 = spark2.sql(query)
df4.show()
 
 
# In[72]:
 
 
spark2.catalog.dropTempView("users2")
# Global Temp View : create tables at application level and acceseble through multiple sessions.
df1.createOrReplaceGlobalTempView("gusers")
query = """select age, count(1) as count from global_temp.gusers
            where age is not null
            group by age  
            order by age  
            limit 4"""
df2 = spark.sql(query)
df2.show()
 
 
# In[73]:
 
 
df2 = spark2.sql(query)
df2.show()
 
 
# In[74]:
 
 
rdd1 = df2.rdd
rdd1.collect()
 
 
# In[ ]:
 
 
 
 
 
 