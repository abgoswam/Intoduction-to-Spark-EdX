# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 23:19:53 2016

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
    from pyspark.sql.functions import udf, split, explode, col, concat, lit
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


from faker import Factory
fake = Factory.create()
fake.seed(4321)

def fake_entry():
  name = fake.name().split()
  return (name[1], name[0], fake.ssn(), fake.job(), abs(2016 - fake.date_time().year) + 1)

def repeat(times, func, *args, **kwargs):
    for _ in range(times):
        yield func(*args, **kwargs)
        
# --------- Create dataframe (from fake data) -------------
data = list(repeat(10000, fake_entry))       
dataDF = spark.createDataFrame(data, ('last_name', 'first_name', 'ssn', 'occupation', 'age'))

subDF = dataDF.select('last_name', 'first_name', 'ssn', 'occupation', (dataDF.age - 1).alias('age'))
filteredDF = subDF.filter(subDF.age < 10)
        
from pyspark.sql.types import BooleanType
less_ten = udf(lambda s: s < 10, BooleanType())
lambdaDF = subDF.filter(less_ten(subDF.age))
lambdaDF.show()    
lambdaDF.count()

# Let's collect the even values less than 10
even = udf(lambda s: s % 2 == 0, BooleanType())
evenDF = lambdaDF.filter(even(lambdaDF.age))
evenDF.show()
evenDF.count()

print("first: {0}\n".format(filteredDF.first()))
print("Four of them: {0}\n".format(filteredDF.take(4)))
    
tempDF = spark.createDataFrame([("Joe", 1), ("Joe", 1), ("Anna", 15), ("Anna", 12), ("Ravi", 5)], ('name', 'score'))
tempDF.show()    

dataDF.drop('occupation').drop('age').show()    
dataDF.groupBy('occupation').count().show(truncate=False)

sampledDF = dataDF.sample(withReplacement=False, fraction=0.10)
print(sampledDF.count())
sampledDF.show()

# Cache the DataFrame
filteredDF.cache()
# Trigger an action
print(filteredDF.count())
# Check if it is cached
print(filteredDF.is_cached)





# ------ Create dataframe (from file) -----
dataPath = "export.csv"
diamondsDF = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load(dataPath)
  
diamondsDF.show()
diamondsDF.count()

diamondsDF.cache()

# If we are done with the DataFrame we can unpersist it so that its memory can be reclaimed
diamondsDF.unpersist()
# Check if it is cached
print(filteredDF.is_cached)

#diamondsDF.count()

#-------------------------------------------
(dataDF
 .filter(dataDF.age > 20)
 .select(concat(dataDF.first_name, lit(' '), dataDF.last_name), dataDF.occupation)
 .show(truncate=False)
 )

#---------------------------------------------
def brokenTen(value):
    """Incorrect implementation of the ten function.

    Note:
        The `if` statement checks an undefined variable `val` instead of `value`.

    Args:
        value (int): A number.

    Returns:
        bool: Whether `value` is less than ten.

    Raises:
        NameError: The function references `val`, which is not available in the local or global
            namespace, so a `NameError` is raised.
    """
    if (val < 10):
        return True
    else:
        return False

brokenTen(11)

btUDF = udf(brokenTen)
brokenDF = subDF.filter(btUDF(subDF.age) == True)


