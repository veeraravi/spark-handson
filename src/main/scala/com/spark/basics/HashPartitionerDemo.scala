package com.spark.basics

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

object HashPartitionerDemo {
  def main(args: Array[String]): Unit = {
    // input outpath dataset sdfsf
    val conf = new SparkConf().setMaster("local[*]").setAppName("word count")
    val sc = new SparkContext(conf)

    //val baseRdd = sc.textFile("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\sampledataforwordcount.txt")
    val baseRdd = sc.textFile(args(0))
    val words = baseRdd.flatMap(line => line.split(" "))
    val wordPair = words.map(ele => (ele,1))
   // val partitionRdd = wordPair.partitionBy(new HashPartitioner(2))
    val partitionRdd = wordPair.partitionBy(new RangePartitioner(2,wordPair))
    val res = partitionRdd.reduceByKey(_+_)
    res.saveAsTextFile(args(1))
  }
}
