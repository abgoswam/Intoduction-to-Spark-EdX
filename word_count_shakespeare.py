# -*- coding: utf-8 -*-
"""
Created on Mon Oct  3 12:25:32 2016

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

#4a) The wordCount function
#First, define a function for word counting. You should reuse the techniques that have been covered in earlier parts of this lab. This function should take in a DataFrame that is a list of words like wordsDF and return a DataFrame that has all of the words and their associated counts.
def wordCount(wordListDF):
    """Creates a DataFrame with word counts.

    Args:
        wordListDF (DataFrame of str): A DataFrame consisting of one string column called 'word'.

    Returns:
        DataFrame of (str, int): A DataFrame containing 'word' and 'count' columns.
    """
    return wordListDF.groupBy('word').count()


#(4b) Capitalization and punctuation
#Real world files are more complicated than the data we have been using in this lab. Some of the issues we have to address are:
#Words should be counted independent of their capitialization (e.g., Spark and spark should be counted as the same word).
#All punctuation should be removed.
#Any leading or trailing spaces on a line should be removed.
#Define the function removePunctuation that converts all text to lower case, removes any punctuation, and removes leading and trailing spaces. Use the Python regexp_replace module to remove any text that is not a letter, number, or space. If you are unfamiliar with regular expressions, you may want to review this tutorial from Google. Also, this website is a great resource for debugging your regular expression.
#You should also use the trim and lower functions found in pyspark.sql.functions.
#Note that you shouldn't use any RDD operations or need to create custom user defined functions (udfs) to accomplish this task

def removePunctuation(column):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        column (Column): A Column containing a sentence.

    Returns:
        Column: A Column named 'sentence' with clean-up operations applied.
    """
    return trim(lower(regexp_replace(column, '[^\sa-zA-Z0-9]', ''))).alias('sentence')


# Initialize SparkContext
sc = SparkContext('local')

spark = SparkSession.builder\
        .master("local[*]")\
        .appName("word count app")\
        .config('spark.sql.warehouse.dir', 'file:///C:/tmp') \
        .getOrCreate()


#(4c) Load a text file
#For the next part of this lab, we will use the Complete Works of William Shakespeare from Project Gutenberg. To convert a text file into a DataFrame, we use the sqlContext.read.text() method. We also apply the recently defined removePunctuation() function using a select() transformation to strip out the punctuation and change all text to lower case. Since the file is large we use show(15), so that we only print 15 lines.        

fileName = "shakespeare.txt"
#shakespeareDF = spark.read.format("csv")\
#  .option("header","true")\
#  .option("inferSchema", "true")\
#  .load(data)

shakespeareDF = spark.read.text(fileName).select(removePunctuation(col('value')))
shakespeareDF.show(15, truncate=False)


#(4d) Words from lines
#Before we can use the wordcount() function, we have to address two issues with the format of the DataFrame:
#The first issue is that that we need to split each line by its spaces.
#The second issue is we need to filter out empty lines or words.
#Apply a transformation that will split each 'sentence' in the DataFrame by its spaces, and then transform from a DataFrame that contains lists of words into a DataFrame with each word in its own row. To accomplish these two tasks you can use the split and explode functions found in pyspark.sql.functions.
#Once you have a DataFrame with one word per row you can apply the DataFrame operation where to remove the rows that contain ''.
#Note that shakeWordsDF should be a DataFrame with one column named word.

shakeWordsDF = (shakespeareDF
                .select(explode(split(shakespeareDF.sentence, ' ')).alias('word'))
                .where(col('word') != ''))

shakeWordsDF.show()


#(4e) Count the words
#We now have a DataFrame that is only words. Next, let's apply the wordCount() function to produce a list of word counts. We can view the first 20 words by using the show() action; however, we'd like to see the words in descending order of count, so we'll need to apply the orderBy DataFrame method to first sort the DataFrame that is returned from wordCount().
#You'll notice that many of the words are common English words. These are called stopwords. In a later lab, we will see how to eliminate them from the results.

topWordsAndCountsDF = wordCount(shakeWordsDF).orderBy("count", ascending=False)
topWordsAndCountsDF.show()

sc.stop()
  
  
  