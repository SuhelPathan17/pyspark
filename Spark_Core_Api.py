#!/usr/bin/env python
# coding: utf-8
 
# In[10]:
 
 
from pyspark import SparkContext
from pyspark import Row
 
 
# In[4]:
 
 
sc = SparkContext("local[*]","Transformations1")
 
 
# In[3]:
 
 
rddFile = sc.textFile("C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\Demo.txt")
 
 
# In[4]:
 
 
rddFile.collect()
 
 
# In[5]:
 
 
rdd1 = sc.parallelize([2,3,1,2,4,5,3,5,6,7,6,8,9], 3)
 
 
# In[6]:
 
 
sc.defaultParallelism
 
 
# In[7]:
 
 
rdd2 = rdd1.map(lambda x: x*10)
 
 
# In[8]:
 
 
rdd2.collect()
 
 
# In[9]:
 
 
rdd1.count()
 
 
# In[10]:
 
 
# Map Transformation
rdd3 = rdd1.map(lambda x:(x,1))
rdd3.collect()
 
 
# In[11]:
 
 
def add(x):
    output = x[0]+x[1]
    return output
 
rdd4 = rdd3.map(add)
rdd4.collect()
 
 
# In[12]:
 
 
rdd5 = rdd4.map(lambda x:x>5)
rdd5.collect()
 
 
# In[13]:
 
 
rddFile.map(lambda x:x.upper()).collect()
 
 
# In[14]:
 
 
rddFile.collect()
 
 
# In[15]:
 
 
#By default split on space and return list
rddFile.map(lambda x:x.split(" ")).collect()
 
 
# In[16]:
 
 
rddFile.map(lambda x:len(x.split(" "))).collect()
 
 
# In[17]:
 
 
rdd5 = rdd4.map(lambda x:x>5)
rdd5.collect()
 
 
# In[18]:
 
 
# Filter Transformation
rdd4.collect()
 
 
# In[19]:
 
 
rdd5 = rdd4.filter(lambda x:x>5)
rdd5.collect()
 
 
# In[20]:
 
 
rddFile.collect()
 
 
# In[21]:
 
 
rddFile.map(len).collect()
 
 
# In[22]:
 
 
rddFile.filter(lambda x:len(x)<10).collect()
 
 
# In[23]:
 
 
# glom Transformation:
rdd1.collect()
 
 
# In[24]:
 
 
rdd1.getNumPartitions()
 
 
# In[25]:
 
 
rdd_glom = rdd1.glom()
rdd_glom.collect()
 
 
# In[26]:
 
 
rdd1.count()
 
 
# In[27]:
 
 
rdd_glom.count()
 
 
# In[28]:
 
 
rdd_glom.map(lambda x:len(x)).collect()
 
 
# In[29]:
 
 
# flatmap Transformation:
rdd1.collect()
 
 
# In[30]:
 
 
rdd1.map(lambda x:(x,x)).collect()
 
 
# In[31]:
 
 
rdd1.flatMap(lambda x:(x,x)).collect()
 
 
# In[32]:
 
 
rddFile.collect()
 
 
# In[33]:
 
 
rddFile.map(lambda x:x.split()).collect()
 
 
# In[34]:
 
 
#split function returns list as a iterable object and flaMap flattens that list.
rddFile.flatMap(lambda x:x.split()).collect()
 
 
# In[35]:
 
 
rddFile.collect()
 
 
# In[36]:
 
 
rddFile.map(lambda x:x.upper()).collect()
 
 
# In[37]:
 
 
rddFile.flatMap(lambda x:x.upper()).collect()
 
 
# In[38]:
 
 
# mapPartitions Transformation
 
rdd1.collect()
 
 
# In[39]:
 
 
rdd1.glom().collect()
 
 
# In[40]:
 
 
#Invalid : because output of function should be an iterable obj
rdd1.mapPartitions(lambda p:sum(p)).collect()
 
 
# In[41]:
 
 
# Valid :
rdd1.mapPartitions(lambda p:[sum(p)]).collect()
 
 
# In[42]:
 
 
# map function in python
# map(fn,iterable_object)
rdd1.mapPartitions(lambda p: map((lambda x:x+1),p)).glom().collect()
 
 
# In[43]:
 
 
# mapPartitionsWithIndex Transformation
rdd1.glom().collect()
 
 
# In[44]:
 
 
rdd1.mapPartitionsWithIndex(lambda i,p: map((lambda x: (i,x*10)),p)).collect()
 
 
# In[45]:
 
 
rdd1.mapPartitionsWithIndex(lambda i,p: map((lambda x: (i,x*10)),p)).filter(lambda x: x[0]==1).collect()
 
 
# In[46]:
 
 
# distinct Transformation
 
rdd1.collect()
 
 
# In[47]:
 
 
rdd1.distinct().collect()
 
 
# In[48]:
 
 
rddFile.collect()
 
 
# In[49]:
 
 
rddWords = rddFile.map(lambda x:x.split()).flatMap(lambda x:x)
rddWords.collect()
 
 
# In[50]:
 
 
rddWords.flatMap(lambda x:x).distinct().collect()
 
 
# In[55]:
 
 
rdd1.glom().collect()
 
 
# In[56]:
 
 
# Hash Partition
 
rdd1.distinct().glom().collect()
 
# In output we can see.
 
#0th Partition:
#    3%3 = 0
#    6%3 = 0
#    9%3 = 0
#same for other partitions as well.
 
 
# In[65]:
 
 
# Staging
 
rdd1.map(lambda x: x+1).distinct().collect()
 
 
 
 
# In[66]:
 
 
# RDD Persistance
 
 
 
rddFile.collect()
 
 
# In[68]:
 
 
rddWords = rddFile.flatMap(lambda x:x.split())
rddWords.collect()
 
 
# In[70]:
 
 
# checking persistance
 
rddWords.toDebugString()
 
 
# In[71]:
 
 
rddWords.persist()
 
 
# In[72]:
 
 
rddWords.toDebugString()
 
 
# In[74]:
 
 
rddWords.collect()
 
 
# In[76]:
 
 
rddWords.unpersist()
rddWords.toDebugString()
 
 
# In[77]:
 
 
# Storage Levels
 
from pyspark import StorageLevel
 
 
# In[82]:
 
 
rddWords.persist(StorageLevel.MEMORY_AND_DISK)
 
 
# In[83]:
 
 
rddWords.toDebugString()
 
 
# In[84]:
 
 
rddWords.collect()
 
 
# In[85]:
 
 
rddWords.unpersist()
rddWords.toDebugString()
 
 
# In[86]:
 
 
rddWords.persist(StorageLevel.DISK_ONLY)
 
 
# In[87]:
 
 
rddWords.collect()
 
 
# In[88]:
 
 
rddWords.unpersist()
rddWords.toDebugString()
 
 
# In[89]:
 
 
# mapValues Transformation
 
rdd1.collect()
 
 
# In[90]:
 
 
rdd2 = rdd1.map(lambda x:(x,x%4))
rdd2.collect()
 
 
# In[91]:
 
 
rdd3 = rdd2.mapValues(lambda x:(x, x+5))
rdd3.collect()
 
 
# In[92]:
 
 
rdd3.mapValues(lambda x:sum(x)).collect()
 
 
# In[93]:
 
 
rdd3.mapValues(list).collect()
 
 
# In[94]:
 
 
# sortBy Transformation
rdd1.collect()
 
 
# In[96]:
 
 
rdd1.sortBy(lambda x:x%3).collect()
# those who have remainder = 0  will come first
 
 
# In[97]:
 
 
rdd1.sortBy(lambda x:x%3,False).collect()
 
 
# In[99]:
 
 
# groupBy Transformation
 
rdd1.collect()
 
 
# In[101]:
 
 
rdd1.groupBy(lambda x:x%3).collect()
 
 
# In[102]:
 
 
rdd1.groupBy(lambda x:x%3).map(lambda x:(x[0],list(x[1]))).collect()
 
 
# In[104]:
 
 
rdd1.groupBy(lambda x:x%3).mapValues(lambda x:list(x)).collect()
 
 
# In[105]:
 
 
rdd1.groupBy(lambda x:x>4).mapValues(lambda x:list(x)).collect()
 
 
# In[106]:
 
 
rddWords.collect()
 
 
# In[108]:
 
 
rddWords.groupBy(lambda x:len(x)).mapValues(lambda x:list(x)).collect()
 
 
# In[109]:
 
 
rddWords.groupBy(lambda x:x).mapValues(lambda x:list(x)).collect()
 
 
# In[111]:
 
 
rddWords.groupBy(lambda x:x).mapValues(lambda x:len(list(x))).collect()
 
 
# In[113]:
 
 
rddWords.getNumPartitions()
 
 
# In[115]:
 
 
rddWords.saveAsTextFile("C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\output\\")
 
 
# In[117]:
 
 
# partitionBy transformation
rdd2.glom().collect()
 
 
# In[118]:
 
 
rdd2.partitionBy(4).glom().collect()
# By default it will take hash function
# key = 4
# eg.  hash(4) % 4 = 0th partition
# key = 1
#hash(1) % 4 = 1th partition
 
 
# In[119]:
 
 
rdd2.partitionBy(4, lambda x:x+10).glom().collect()
# key = 1
# fun(1) % 4 = 11 % 4 = 3th Partition
 
 
# In[121]:
 
 
# repartition and coalesce Transformation
 
rddWords.glom().collect()
 
 
# In[123]:
 
 
rddWords.repartition(3).glom().collect()
 
 
# In[125]:
 
 
rddWords.coalesce(2).glom().collect()
 
 
# In[127]:
 
 
rdd1.glom().collect()
 
 
# In[128]:
 
 
rdd1.repartition(5).glom().collect()
 
 
# In[129]:
 
 
rdd1.coalesce(2).glom().collect()
 
 
# In[130]:
 
 
rdd1.collect()
 
 
 
# In[133]:
 
 
# ***Key Transformations***
 
# sortByKey Transformation:
 
rdd2 = rdd1.map(lambda x:(x,x+7))
rdd2.collect()
 
 
# In[134]:
 
 
#Ascending By default
rdd2.sortByKey().collect()
 
 
# In[135]:
 
 
#Descending
rdd2.sortByKey(False).collect()
 
 
# In[136]:
 
 
rdd2.getNumPartitions()
 
 
# In[137]:
 
 
rdd2.sortByKey(False).glom().collect()
 
 
# In[138]:
 
 
# we can control partions in o/p rdd
rdd2.sortByKey(False,6).glom().collect()
 
 
# In[139]:
 
 
# groupByKey Transformation
 
rdd2.groupByKey().collect()
 
 
# In[140]:
 
 
rdd2.groupByKey().mapValues(list).collect()
 
 
# In[142]:
 
 
rddWords.collect()
 
 
# In[144]:
 
 
rddPairs = rddWords.map(lambda x:(x,1))
rddPairs.groupByKey().mapValues(sum).collect()
 
 
# In[145]:
 
 
# RDD Action  : Reduce => does iterative thing on partition first and then reduce it to 1 partition
 
rdd1.collect()
 
 
# In[147]:
 
 
rdd1.reduce(lambda x,y:x+y)
 
 
# In[148]:
 
 
rdd1.reduce(lambda x,y:max(x,y))
 
 
# In[149]:
 
 
# RDD Action  : take => return list of required no. of OBJECTS
rdd1.take(4)
 
 
# In[151]:
 
 
# RDD Action  : takeOrdered => return list of req. no. of objects as sorted naturally
rdd1.takeOrdered(4)
 
 
# In[152]:
 
 
# RDD Action  : takeSample => picks sample req. no of objects from rdd
# True = with replacement(same obj can be picked again)
rdd1.takeSample(True,5)
 
 
# In[153]:
 
 
rdd1.takeSample(True,5)
 
 
# In[154]:
 
 
rdd1.takeSample(True,50)
 
 
# In[156]:
 
 
#  Without Replacement sampling
#False = without replacement(same obj cannot be picked again. it's removed). sample size cannot be greater than len of rdd
rdd1.takeSample(False,5)
 
 
# In[159]:
 
 
rdd1.takeSample(False,50)
 
 
# In[158]:
 
 
 
 
 
# In[160]:
 
 
# RDD Action  : countByValue() => return dict object of how many times value is repeated
rdd1.collect()
 
 
# In[161]:
 
 
rdd1.countByValue()
 
 
# In[162]:
 
 
# RDD Action  : countByKey() => works on pair RDD. return dict object of how many times key is repeated
 
rdd3 = sc.parallelize([(1,10),(1,10),(1,11),(1,11),(2,20),(2,21),(2,21)], 2)
rdd3.collect()
 
 
# In[163]:
 
 
rdd3.countByValue()
 
 
# In[164]:
 
 
rdd3.countByKey()
 
 
# In[167]:
 
 
# RDD Action  : foreach() => does not return anything. but runs on each obj of rdd.
rdd3.collect()
 
 
# In[214]:
 
 
a = rdd3.foreach(lambda x:print("Value : {}".format(x[1])))
type(a)
 
 
# In[177]:
 
 
rdd3.foreach(lambda x: sum(x))
 
 
# In[178]:
 
 
# USE Case
carsFile = "C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\data_git\\cars.tsv"
rdd1 = sc.textFile(carsFile)
rdd1.collect()
 
 
# In[180]:
 
 
rdd1.map(lambda x:x.split('\t')).collect()
 
 
# In[182]:
 
 
rdd1.map(lambda x:x.split('\t')).filter(lambda x:x[-1]=='American').collect()
 
 
# In[185]:
 
 
rdd1.map(lambda x:x.split('\t')).filter(lambda x:x[-1]=='American').map(lambda x:(x[0],x[6])).collect()
 
 
# In[189]:
 
 
rdd1.map(lambda x:x.split('\t')).filter(lambda x:x[-1]=='American').map(lambda x:(x[0],x[6])).mapValues(int).collect()
 
 
# In[191]:
 
 
rdd1.map(lambda x:x.split('\t')).filter(lambda x:x[-1]=='American').map(lambda x:(x[0],x[6])).mapValues(lambda x:(int(x),1)).collect()
 
 
# In[193]:
 
 
rdd1.map(lambda x:x.split('\t')).filter(lambda x:x[-1]=='American').map(lambda x:(x[0],x[6])).mapValues(lambda x:(int(x),1)).reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1])).collect()
 
 
# In[197]:
 
 
rdd1.map(lambda x:x.split('\t'))\
.filter(lambda x:x[-1]=='American').map(lambda x:(x[0],x[6]))\
.mapValues(lambda x:(int(x),1))\
.reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))\
.mapValues(lambda p:p[0]/p[1]).collect()
 
 
# In[198]:
 
 
rdd1.map(lambda x:x.split('\t'))\
.filter(lambda x:x[-1]=='American').map(lambda x:(x[0],x[6]))\
.mapValues(lambda x:(int(x),1))\
.reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))\
.mapValues(lambda p:p[0]/p[1])\
.sortBy(lambda x:x[1],False)\
.collect()
 
 
# In[200]:
 
 
rdd1.getNumPartitions()
 
 
# In[207]:
 
 
rdd = rdd1.map(lambda x:x.split('\t'))\
.filter(lambda x:x[-1]=='American').map(lambda x:(x[0],x[6]))\
.mapValues(lambda x:(int(x),1))\
.reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))\
.mapValues(lambda p:p[0]/p[1])\
.sortBy(lambda x:x[1],False)\
.coalesce(1)
 
 
# In[208]:
 
 
rdd.collect()
 
 
# In[209]:
 
 
rdd.getNumPartitions()
 
 
# In[212]:
 
 
rdd.saveAsTextFile("C:\\Users\\SPathan\\OneDrive - Financial Conduct Authority\\Desktop\\Pyspark\\output\\cars")
 
 
# In[240]:
 
 
### SPARK SHARED VARIABLES ###\
rdd3.collect()
 
 
# In[247]:
 
 
rdd_rng = sc.parallelize(range(1,101),3)
 
accum = sc.accumulator(0)
 
def increment_counter(x):
    global accum
    accum += x
 
rdd_rng.foreach(increment_counter)
print(accum.value)