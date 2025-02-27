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
 
 
# In[6]:
 
 
# Using SQL Approach
employee = spark.createDataFrame([
                            (1,"Raju",25,101),
                            (2,"Ramesh",26,101),
                            (3,"Amrita",30,102),
                            (4,"Madhu",32,102),
                            (5,"Aditya",28,102),
                            (6,"Pravin",28,10000)]).toDF("id","name","age","deptid")
employee.printSchema()
 
 
# In[8]:
 
 
employee.show()
 
 
# In[10]:
 
 
department = spark.createDataFrame([
                            (101,"IT",1),
                            (102,"ITES",1),
                            (103,"Operation",1),
                            (104,"HRD",2)]).toDF("id","deptid","locationid")
department.printSchema()
 
 
# In[11]:
 
 
department.show()
 
 
# In[12]:
 
 
spark.catalog.listTables()
 
 
# In[15]:
 
 
employee.createOrReplaceTempView("emp")
department.createOrReplaceTempView("dept")
 
 
# In[16]:
 
 
spark.catalog.listTables()
 
 
# In[19]:
 
 
Query = '''select emp.*, dept.* from emp join dept on emp.deptid = dept.id '''
JoinedDF = spark.sql(Query)
JoinedDF.show()
 
 
# In[20]:
 
 
Query = '''select emp.*, dept.* from emp left join dept on emp.deptid = dept.id '''
JoinedDF = spark.sql(Query)
JoinedDF.show()
 
 
# In[21]:
 
 
Query = '''select emp.*, dept.* from emp Right join dept on emp.deptid = dept.id '''
JoinedDF = spark.sql(Query)
JoinedDF.show()
 
 
# In[22]:
 
 
Query = '''select emp.*, dept.* from emp cross join dept  '''
JoinedDF = spark.sql(Query)
JoinedDF.show()
 
 
# In[23]:
 
 
JoinedDF.count()
 
 
# In[25]:
 
 
Query = '''select emp.*, dept.* from emp full join dept on emp.deptid = dept.id '''
JoinedDF = spark.sql(Query)
JoinedDF.show()
 
 
# In[26]:
 
 
Query = '''select emp.*, dept.* from emp inner join dept on emp.deptid = dept.id '''
JoinedDF = spark.sql(Query)
JoinedDF.show()
 
 
# In[28]:
 
 
Query = '''select emp.* from emp left semi join dept on emp.deptid = dept.id '''
JoinedDF = spark.sql(Query)
JoinedDF.show()
 
 
# In[29]:
 
 
Query = '''select emp.* from emp left anti join dept on emp.deptid = dept.id '''
JoinedDF = spark.sql(Query)
JoinedDF.show()
 
 
# In[32]:
 
 
# get rid of in-mememory tables
spark.catalog.dropTempView("emp")
spark.catalog.dropTempView("dept")
 
spark.catalog.listTables()
 
 
# In[35]:
 
 
# Using DF Transformation
# join_condition : employee.deptid == department.id
join_column =  employee["deptid"] == department["id"]
JoinedDF = employee.join(department,join_column)
JoinedDF.show()
 
 
# In[36]:
 
 
join_column =  employee["deptid"] == department["id"]
JoinedDF = employee.join(department,join_column,"left")
JoinedDF.show()
 
 
# In[37]:
 
 
join_column =  employee["deptid"] == department["id"]
JoinedDF = employee.join(department,join_column,"right")
JoinedDF.show()
 
 
# In[39]:
 
 
join_column =  employee["deptid"] == department["id"]
JoinedDF = employee.join(department,join_column,"full")
JoinedDF.show()
 
 
# In[40]:
 
 
join_column =  employee["deptid"] == department["id"]
JoinedDF = employee.join(department,join_column,"left_semi")
JoinedDF.show()
 
 
# In[41]:
 
 
join_column =  employee["deptid"] == department["id"]
JoinedDF = employee.join(department,join_column,"left_anti")
JoinedDF.show()
 
 
# In[ ]:
 
 
 
 
 
 