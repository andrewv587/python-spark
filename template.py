#!/usr/bin/python
# -*- coding:utf-8 -*- 
#Filename:template.py
#Function:spark-template
#Fulfile wordCount
#Author:Huang Weihang
#Email:huangweihang14@mails.ucas.ac.cn
#Data:2016-11-05

#set defaultCode:
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

## Spark Application - execute with spark-submit
 
## Imports
from pyspark import SparkConf, SparkContext
  
## Module Constants
APP_NAME = "Word Count"
## Closure Functions
def wordSplit(line):
    words=line.split(" ")
    return words
   
## Main functionality
def main(sc):
   data=sc.textFile("shell")
   words_count=data.flatMap(wordSplit).map(lambda x:(x,1)) \
           .reduceByKey(lambda a,b:a+b)
   print data.count()
   print words_count.take(10)


if __name__ == "__main__":
# Configure Spark
    conf=SparkConf().setAppName(APP_NAME)
    sc=SparkContext(conf=conf)
    # Execute Main functionality
    main(sc)
