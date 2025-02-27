#!/usr/bin/env python
# coding: utf-8
 
# In[71]:
 
 
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import expr, col, column, round, desc
from pyspark.sql.functions import lit, asc, count, sum, when, udf
from pyspark.sql.functions import max, avg, dense_rank, rank, row_number, split, explode
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
import pandas as pd
 
 
# In[2]:
 
 
spark = SparkSession \
        .builder \
        .appName("Basic DF Operations") \
        .config("spark.master", "local[*]") \
        .getOrCreate()
 
 
# In[16]:
 
 
inputFilePath = "C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\data_git\\flight-data\\json\\2015-summary.json"
 
 
 
# In[17]:
 
 
df1 = spark.read.json(inputFilePath)
df1.count()
 
 
# In[6]:
 
 
df1.show()  # shows top 20 rows and len of column data is 20
 
 
# In[7]:
 
 
df1.show(30)
 
 
# In[8]:
 
 
df1.show(30, False) # do not truncate len of columns
 
 
# In[9]:
 
 
df1.show(3,False,True)  # columns are displayed vertically
 
 
# In[10]:
 
 
# column names comes as alphabetical order in case of json
df1.printSchema()
 
 
# In[11]:
 
 
df1.rdd.getNumPartitions()
 
 
# In[19]:
 
 
df2 = df1.where("count > 100")
df2.count()
 
 
# In[21]:
 
 
# Writing into json format
outputFilePath = "C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\output\\json"
df2.write.json(outputFilePath)
 
 
# In[22]:
 
 
# Writing into parquet
outputFilePath = "C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\output\\parquet"
df2.write.parquet(outputFilePath)
 
 
# In[25]:
 
 
# reading from parquet
inputFilePath = "C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\output\\parquet"
df1 = spark.read.parquet(inputFilePath)
df1.count()
 
 
# In[26]:
 
 
# Writing into orc
outputFilePath = "C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\output\\orc"
df1.write.orc(outputFilePath)
 
 
# In[27]:
 
 
# Reading from orc
inputFilePath = "C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\output\\orc"
df1 = spark.read.orc(inputFilePath)
 
 
# In[28]:
 
 
# Default saving and reading format : Parquet   # Not preferred
FilePath = "C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\output"
df2.write.save(FilePath)
df2 = spark.read.load(FilePath)
 
 
# In[29]:
 
 
# Working with csv's
InputFilePath ="C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\data_git\\flight-data\\csv\\2015-summary.csv"
   
 
 
# In[31]:
 
 
df1 = spark.read.csv(InputFilePath)
df1.count()   # count increased
 
 
# In[32]:
 
 
df1.show()
 
 
# In[35]:
 
 
df1 = spark.read.csv(InputFilePath, header = True)
df1.show()
 
 
# In[36]:
 
 
df1.count()
 
 
# In[37]:
 
 
# CSV refer all data as string format
df1.printSchema()
 
 
# In[39]:
 
 
# Specifying inferSchema = True
df1 = spark.read.csv(InputFilePath, header = True, inferSchema = True)
df1.printSchema()
 
 
# In[46]:
 
 
OutputFilePath ="C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\output\\csv"
df1.write.csv(OutputFilePath)
 
   
 
 
# In[47]:
 
 
df1.write.mode("overwrite").csv(OutputFilePath, header = True)
 
 
# In[48]:
 
 
# Writing into delimited format we use csv but we have to specify delimitor explicetly
df1.write.mode("overwrite").csv(OutputFilePath, header = True, sep="\t")
 
 
# In[52]:
 
 
# reading from text file
InputFilePath ="C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\data_git\\wordcount.txt"
df1 = spark.read.text(InputFilePath)
df1.show(30, False)
 
 
# In[53]:
 
 
df1.count()
 
 
# In[54]:
 
 
df1.printSchema()
 
 
# In[56]:
 
 
c1 = df1.value
type(c1)
 
 
# In[59]:
 
 
df2 = df1.select(split(df1.value, " "))
df2.show(30, False)
 
 
# In[77]:
 
 
# explode is like flatMap
df2 = df1.select(explode(split(df1.value, " "))).groupBy("col").count()
df2.show()
 
 
# In[78]:
 
 
df2.count()
 
 
# In[82]:
 
 
OutputFilePath ="C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\output\\text"
df2.write.csv(OutputFilePath)
 
 
# In[ ]:
 
 
 
 
 
# In[ ]:
 
 
 
 
 
# In[ ]:
 
 
 
 
 
# In[ ]:
 
 
 
 
 
# In[ ]:
 
 
 
 
 
# In[ ]:
 
 
 
 
 
# In[ ]:
 
 
 
 
 
# In[ ]:
 
 
 
 
 
# In[ ]:
 
 
 
 
 
# In[ ]:
 
 
 
 
 
# In[ ]:
 
 
 
 
 
# In[ ]:
 
 
 
 
 
# In[ ]:
 
 
 
 
 
# In[ ]:
 
 
 
 
 
 