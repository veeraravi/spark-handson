package sparkcore

import org.apache.spark.sql.SparkSession

object FilterDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FilterDemo").master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    //filterbyRange
    val fruitsPair = List((1,"banana"),(10,"apple"),(6,"orange"),(15,"Kiwi"),
                 (20,"mango"),(10,"apple"),(15,"Kiwi"),(5,"goa"),(9,"pineApple"))
    val rdd = sc.parallelize(fruitsPair)
    val rangeFruit = rdd.filterByRange(1,5)
    rangeFruit.foreach(println)

    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35)
    val numRdd = sc.parallelize(data)
//    numRdd.me

   // numRdd.filterByRange()
    //ID
    println(" RDD's ID "+numRdd.id)
    println(" RDD's Name "+numRdd.name)
    numRdd.setName("numbers")
    println(" RDD's Name after setName "+numRdd.name)


    val fruits = List("banana","apple","orange","Kiwi","mango","apple","Kiwi","goa","pineApple")

    val fruitRDD = sc.parallelize(fruits)

      val fruitLength = fruitRDD.map(ele =>(ele,ele.length))

        fruitLength.collect.foreach(println)

        println(" "+fruitLength.max)

        fruitLength.map(ele => ele.swap).collect.foreach(println)

        fruitLength.map(ele => (ele._2,ele._1)).collect.foreach(println)

  }
}
