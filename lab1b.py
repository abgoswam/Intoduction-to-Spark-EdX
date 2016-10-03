# -*- coding: utf-8 -*-
"""
Created on Sun Oct  2 09:41:34 2016

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
    from pyspark.sql.types import StringType
    from pyspark.sql.functions import regexp_replace,lower, trim, length, udf, split, explode, col, concat, lit
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
        
wordsDF = spark.createDataFrame([('  Ca--t',), ('elephant it is',), ('rat',), ('rat',), ('cat', )], ['word'])
wordsDF.show()
print(type(wordsDF))
wordsDF.printSchema()

pluralDF = wordsDF.select(concat(wordsDF.word, lit('s')).alias('word'))
pluralDF.show()

pluralLengthsDF = pluralDF.select(length('word'))
pluralLengthsDF.show()

wordCountsDF = wordsDF.groupBy('word').count()
wordCountsDF.show()

#punctuation = udf(lambda s: 'abcd', StringType())
#wordsDF.select(trim(lower(punctuation(wordsDF.word)))).show()
wordsDF.select(trim(lower(regexp_replace('word', '[^\sa-zA-Z0-9]', '')))).show()

wordsDF.select(explode(split(wordsDF.word, ' '))).show()





