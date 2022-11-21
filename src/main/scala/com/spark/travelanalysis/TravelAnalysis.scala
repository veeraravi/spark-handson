package com.spark.travelanalysis

import org.apache.spark.{SparkConf, SparkContext}

/*
* Travel Sector Dataset Description
Column 1: City pair (Combination of from and to): String
Column 2: From location: String
Column 3: To Location: String
Column 4: Product type: Integer (1=Air, 2=Car, 3 =Air+Car, 4 =Hotel, 5=Air+Hotel, 6=Hotel +Car, 7 =Air+Hotel+Car)
Column 5: Adults traveling: Integer
Column 6: Seniors traveling: Integer
Column 7: Children traveling: Integer
Column 8: Youth traveling: Integer
Column 9: Infant traveling: Integer
Column 10: Date of travel: String
Column 11: Time of travel: String
Column 12: Date of Return: String
Column 13: Time of Return: String
Column 14: Price of booking: Float
Column 15: Hotel name: String
ZIH-ZIH	ZIH	ZIH	4	2	0	0	0	0	2014-10-23 00:00:00.000
2014-10-25 00:00:00.000	0	0	2003.19995117188				Viceroy Zihuatanejo

*/
//
object TravelAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Travel analysis")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val baseRdd = sc.textFile(args(0))
    val columns = baseRdd.map(line => line.split("\t"))
    // find top 20 destinations
    val destinationPair = columns.map(dest => (dest(2),1))
    val destCount = destinationPair.reduceByKey(_+_)
    val swapKeyValue = destCount.map(key=>key.swap)
    //(Goa,100)(OOty,150),(kerala,80) --> (100,Goa),(150,ooty),(80,kerala)

    val sortRdd = swapKeyValue.sortByKey(false)
    val takeTop20 = sortRdd.take(20)
    takeTop20.foreach(println)
    //takeTop20.saveAsTextFile(args(1))

    //Top 20 cities that generate high airline revenues for travel
    val travelFilter = columns.filter(ele => {
      if(ele(3).matches(("1")))true else false
    })
    val airlineCnt = travelFilter.map(ele => (ele(2),1)).reduceByKey(_+_)

    val swapKeyValue1 = airlineCnt.map(ele=>ele.swap)
    val sortRdd1 = swapKeyValue1.sortByKey(false)
    val takeTop20Ailine = sortRdd1.take(20)
    takeTop20Ailine.foreach(println)
  }

}
