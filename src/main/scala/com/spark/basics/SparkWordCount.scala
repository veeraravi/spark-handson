package com.spark.basics

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("word count")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    //conf.set
    val baseRdd = sc.textFile("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\sampledataforwordcount.txt")
   // val baseRdd = sc.textFile(args(0))
    val words = baseRdd.map(line => line.split(" "))

  //  words.glom().collect().foreach(println)

   //words.collect()

 //   val wordPair = words.map(ele => (ele,1))
  //  val res = wordPair.reduceByKey(_+_)
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
  //  res.saveAsTextFile("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\wordCount14\\")
  // res.saveAsTextFile("/sparkout/wordCount1/")
  // res.saveAsTextFile("file:///sparkout/wordCount1/")
  }

}
