# -*- coding: utf-8 -*-
"""
Created on Sun Sep 25 11:17:32 2016

@author: agoswami
"""
import os
import sys

# Path for spark source folder
#os.environ['SPARK_HOME'] = r"D:\spark-2.0.0-bin-hadoop2.7"

# Append pyspark  to Python Path
sys.path.append("D:\spark-2.0.0-bin-hadoop2.7\python")
sys.path.append("D:\spark-2.0.0-bin-hadoop2.7\python\lib\py4j-0.10.1-src.zip")

try:
    from pyspark import SparkContext
    from pyspark.sql import Row
    from pyspark.sql import SparkSession
    from pyspark.sql.types import IntegerType
    from pyspark.sql.functions import udf, split, explode, col
    import spark_mooc_meta
    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

# Initialize SparkContext
sc = SparkContext('local')

spark = SparkSession.builder\
        .master("local[*]")\
        .appName("trial app 1")\
        .config('spark.sql.warehouse.dir', 'file:///C:/tmp') \
        .getOrCreate()

#-------------------
#select, filter
row = Row(name="Alice", age=11)
row['name'], row['age']

data = [('Alice', 1), ('Bob', 2)]
df = spark.createDataFrame(data, ['name', 'age'])
df.show()

doubled = udf(lambda s : s*2, IntegerType())
df_doubled = df.select(df.name, doubled(df.age).alias('age'))

df_doubled_filter = df_doubled.filter(df_doubled.age > 3)

#--------------------------
#distinct, sort
data = [('Alice', 1), ('Bob', 2), ('Bob', 4)]
df = spark.createDataFrame(data, ['name', 'age'])
df.show()

df_distinct = df.distinct()
df_distinct.show()

df_sorted = df.sort(df.age)
df_sorted.show()

#-------------------
#split, explode
data3 = [Row(a=10, intlist=[1,2,3])]
df4 = spark.createDataFrame(data3)
df5 = df4.select(col("a"), explode(df4.intlist).alias('anInt'))

df = spark.createDataFrame(
    [('cat \n\n elephant rat \n rat cat', )], ['word']
)
df.show()

df_explode_split = df.select(explode(split(df.word, "\s+")).alias("word")).show()

#-------------------
#group by
data = [('Alice',1,6), ('Bob',2,8), ('Alice',3,9), ('Bob',4,7)]
df = spark.createDataFrame(data, ['name', 'age', 'grade'])

gr1 = df.groupBy(df.name)

df1 = gr1.agg({"*": "count"})
df1.show()

df2 = gr1.count()
df2.show()

df.groupBy().avg().show()

#------------
# select expressions
df_star = df.select('*')
df_star.show()

df_name = df.select('name')
df_name.show()

df_2 = df.select(df.name, (df.age + 10).alias('age_10'))
df_2.show()

df_3 = df_2.drop(df_2.age_10)
df_3.show()

#----------------------
#udf
slen = udf(lambda s: len(s), IntegerType())
df_udf = df.select(slen(df.name).alias('slen'))
df_udf2 = df.select(slen(df.name))

df_udf.show()
df_udf2.show()

#-------------------
#creating a dataframe from a file
df_export = spark.read.text('export.csv')
df_export.show()

#---------------------
#filter
#df_export_Premium = df_export.filter('Premium' in df_export.value)
#df_export_Premium.show()

#sc.stop()
















