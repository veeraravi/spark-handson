package com.spark.com.spark.olympicsanalysis

import org.apache.spark.{SparkConf, SparkContext}
/*
Name
age
country
year
closingdate
game
gm
sm
bm
tm:TOTAL MEDALS
* */
object OlympicsAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Olympic analysis")
    val sc = new SparkContext(conf)
    val baseRdd = sc.textFile(args(0))
    val columns = baseRdd.map(line => line.split("\t"))
    //country wise total medals in swimming
    val swimmingRdd = columns.filter( ele => {
      if(ele(5).equalsIgnoreCase("swimming")) true
      else false
    })
   val countryTotalMedalsPair = swimmingRdd.map(ele=>(ele(2),ele(9).toInt))
    val res = countryTotalMedalsPair.reduceByKey(_+_)
    //top 20 countries which own more no of medals
    val swapKeyValue = res.map(key=>key.swap)
    val sortRdd = swapKeyValue.sortByKey(false)
    val takeTop20 = sortRdd.take(20)
    //res.sa
    //res.saveAsTextFile(args(1))
    res.foreach(println)
    println("================")
    takeTop20.foreach(println)
    //year wise total medals

    //country wise total medals
  }

}
