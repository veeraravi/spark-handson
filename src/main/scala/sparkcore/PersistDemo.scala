package sparkcore

import org.apache.spark.sql.SparkSession

object PersistDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(" Relational Trans Demo")
      .master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val fruits = List("banana","apple","orange","Kiwi","mango","apple","Kiwi","goa","pineApple")
    val fRdd = sc.parallelize(fruits,4);
    fRdd.persist()
  }
}
