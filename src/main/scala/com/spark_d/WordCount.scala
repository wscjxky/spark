package com.spark_d

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]) {
    val logFile = "hdfs://192.168.30.126:9000/input/test.txt" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}