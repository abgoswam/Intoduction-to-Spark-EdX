# -*- coding: utf-8 -*-
"""
Created on Thu Sep 29 15:53:26 2016

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
        
filename = "abcd.txt"
dataDF = spark.read.text(filename)
dataDF.show()

diamonds = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load(filename)

shakespeareCount = dataDF.count()
print(shakespeareCount)