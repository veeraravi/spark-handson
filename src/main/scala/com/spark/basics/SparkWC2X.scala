package com.spark.basics

import org.apache.spark.sql.SparkSession

object SparkWC2X {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
                .builder()
                .appName("wordcount 2x")
                .master("local[*]")
                .getOrCreate()
    val sc = spark.sparkContext

    val textRDD = sc.textFile("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\sampledataforwordcount.txt")
    // val baseRdd = sc.textFile(args(0))
    val words = textRDD.flatMap(line => line.split(" "))

    //words.glom().collect().foreach(println)

    //words.collect()

       val wordPair = words.map(ele => (ele,1))
     val res1 = wordPair.reduceByKey(_+_)
    /*
    * (mitran,1)
      (amit,1)
      (mitran,1)
      (veera,1)
      (mitran,1)
      (amit,1)
      ========================
      (amit,Array[1,1])
      (mitran,Array[1,1,1])   ==>
      (veera,Array[1])
    *
    * */
    // val res = wordPair.groupByKey().map(ele => (ele._1,ele._2.sum))

    //res.saveAsTextFile(args(1))
      res1.saveAsTextFile("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\wordCount15\\")
    // res.saveAsTextFile("/sparkout/wordCount1/")
    // res.saveAsTextFile("file:///sparkout/wordCount1/")
    sc.stop()
  }
}
