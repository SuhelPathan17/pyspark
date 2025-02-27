#!/usr/bin/env python
# coding: utf-8
 
# In[1]:
 
 
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
 
 
# In[3]:
 
 
# Select Statement
 
inputFilePath = "C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\data_git\\flight-data\\json\\2015-summary.json"
df1 = spark.read.json(inputFilePath)
df1.show(5)
 
 
# In[4]:
 
 
df1.printSchema()
 
 
# In[5]:
 
 
df2 = df1.select("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME", "count")
df2.show(5)
 
 
# In[6]:
 
 
# col, column, expr return column objects ie. pyspark.sql.column.Column
# df1.ORIGIN_COUNTRY_NAME
#df1["ORIGIN_COUNTRY_NAME"]
df2 = df1.select(col("ORIGIN_COUNTRY_NAME").alias("origin"),column("DEST_COUNTRY_NAME").alias("destination"),
                 expr("count").cast("int"), expr("count+10 as Newcount"), expr("count > 200 as HighFrequency"),
                expr("ORIGIN_COUNTRY_NAME = DEST_COUNTRY_NAME as domestic"))
df2.show()
 
 
# In[7]:
 
 
df2.printSchema()
 
 
# In[8]:
 
 
# shortcut
 
df2 = df1.selectExpr("ORIGIN_COUNTRY_NAME as origin",
                    "DEST_COUNTRY_NAME as destination",
                     "count",
                     "count+10 as Newcount", "count > 200 as HighFrequency",
                    "ORIGIN_COUNTRY_NAME = DEST_COUNTRY_NAME as domestic")
df2.show()
 
 
# In[9]:
 
 
# Where/filter
df3 = df2.where("count > 500 and domestic = False")
df3.show()
 
 
# In[10]:
 
 
# Where/filter
df3 = df2.filter("count > 500 and domestic = False")
df3.show()
 
 
# In[11]:
 
 
# orderBy/sort
df3 = df2.orderBy("count", "origin")
df3.show()
 
 
# In[12]:
 
 
df3 = df2.sort("count", "origin")
df3.show()
 
 
# In[13]:
 
 
# In descending order
df3 = df2.sort(desc("count"), asc("origin"))
df3.show()
 
 
# In[14]:
 
 
# group By : apply aggregation function after group by like sum, count, max, min, avg
 
df2.printSchema()
 
 
# In[15]:
 
 
df3 = df2.groupBy("HighFrequency","domestic").count()
df3.show()
 
 
# In[16]:
 
 
df3 = df2.groupBy("HighFrequency","domestic").sum("count")
df3.show()
 
 
# In[18]:
 
 
# multiple aggregations
df3 = df2.groupBy("HighFrequency","domestic").agg(sum("count").alias("sum"),max("count").alias("max"))
df3.show()
 
 
# In[20]:
 
 
# multiple aggregations
df3 = df2.groupBy("HighFrequency","domestic").agg(sum("count").alias("sum"),round(avg("count"),2).alias("avg"))
df3.show()
 
 
# In[21]:
 
 
# withColumn : add additional col, modify content of existing col.
# df.withColumn(str,column_object)
df1.printSchema()
 
 
# In[29]:
 
 
df3 = df1.withColumn("newcount", col("count")+10)  \
        .withColumn("HighFrequency", expr("count > 200"))  \
        .withColumn("domestic",expr("ORIGIN_COUNTRY_NAME=DEST_COUNTRY_NAME"))
df3.show()
 
 
# In[31]:
 
 
# modify existing column
df3 = df3.withColumn("count", expr("count * 10"))
df3.show(5)
 
 
# In[32]:
 
 
df3.printSchema()
 
 
# In[33]:
 
 
df3 = df3.withColumn("count", col("count").cast("int"))
df3.printSchema()
 
 
# In[35]:
 
 
# lit Method : gives column object with constant value
df3 = df3.withColumn("country", lit("India"))
df3.show(5)
 
 
# In[37]:
 
 
# withColumnRenamed : rename existing column
df3 = df3.withColumnRenamed("DEST_COUNTRY_NAME","destination")
df3 = df3.withColumnRenamed("ORIGIN_COUNTRY_NAME","origin")
df3.show(5)
 
 
# In[38]:
 
 
# drop : it will only exclude columns from new df (not deleting)
 
df2.printSchema()
 
 
# In[39]:
 
 
df3 = df2.drop("HighFrequency","Newcount")
df3.printSchema()
 
 
# In[40]:
 
 
# dropna = exclude rows with null values in o/p dataframes. we can use subset arg. to specify perticular null column
# dropDuplicates = exclude duplicate rows in o/p df
# distinct = returns unique rows of df
 
 
# In[41]:
 
 
# RandomSplit : Check notes
df1.count()
 
 
# In[44]:
 
 
df_list = df1.randomSplit([1.0,1.0])
type(df_list)
 
 
# In[45]:
 
 
print(df_list[0].count(), df_list[1].count())
 
 
# In[49]:
 
 
df_list = df1.randomSplit([0.4,0.3,0.3])
print(df_list[0].count(), df_list[1].count(), df_list[2].count())
 
 
# In[60]:
 
 
df2, df3, df4 = df1.randomSplit([0.4,0.3,0.3], seed = 10)
print(df2.count(),df3.count(),df4.count())
 
 
# In[62]:
 
 
# sample
# 1. Withreplacement :  possible to have duplicates
df2 = df1.sample(True,0.6)
df2.show()
 
 
# In[63]:
 
 
df2 = df1.sample(True,1.6)
df2.show()
 
 
# In[64]:
 
 
df2.count()
 
 
# In[67]:
 
 
# 2.Without Replacement : No duplicates unless thre are no duplicates in df
df2 = df1.sample(False,0.6)
df2.show()
 
 
# In[69]:
 
 
# union : to perform union on df. shema should be compatible
df2 = df1.where("count > 1000")
df2.count()
 
 
# In[73]:
 
 
df2.rdd.getNumPartitions()
 
 
# In[74]:
 
 
df3 = df1.where("DEST_COUNTRY_NAME = 'India'")
df3.count()
 
 
# In[75]:
 
 
df3.rdd.getNumPartitions()
 
 
# In[93]:
 
 
df4 = df3.union(df2)
df4.count()
 
 
# In[94]:
 
 
df4.rdd.getNumPartitions()
 
 
# In[79]:
 
 
df4.write.csv("C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\output\\csv_union")
 
 
# In[95]:
 
 
# subtract : removes any row that is there in 2nd dataframe
df5 = df4.subtract(df3)
df5.count()
 
 
# In[96]:
 
 
# intersect : common unique rows from 2 df's
 
df6 = df4.intersect(df3)
df6.count()
 
 
# In[97]:
 
 
df6.rdd.getNumPartitions()
 
 
# In[92]:
 
 
spark.conf.get("spark.sql.shuffle.partitions")
 
 
# In[98]:
 
 
# Repartition : changes no. of partititons
df1.show(5)
 
 
# In[100]:
 
 
df1.rdd.getNumPartitions()
 
 
# In[102]:
 
 
df2 = df1.repartition(4)
df2.rdd.getNumPartitions()
 
 
# In[104]:
 
 
df3 = df2.repartition(2)
df3.rdd.getNumPartitions()
 
 
# In[105]:
 
 
df1.printSchema()
 
 
# In[108]:
 
 
df5 = df1.repartition(col("DEST_COUNTRY_NAME"))
df5.rdd.getNumPartitions()
 
 
# In[ ]:
 
 
 
 
 
 