package com.spark.basics

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorsDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Travel analysis")
    val sc = new SparkContext(conf)
    val baseRdd = sc.textFile(args(0))
    val columns = baseRdd.map(line => line.split(","))
    val accum = sc.accumulator(0.toInt,"ERROR RECORDS")
    val res = columns.map(ele => {
      if(ele.length < 4) accum +=1

    })
    println(accum)


  }

}
