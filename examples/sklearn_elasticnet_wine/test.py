# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    dbutils = DBUtils(spark)
    print(dbutils.fs.ls("dbfs:/"))
