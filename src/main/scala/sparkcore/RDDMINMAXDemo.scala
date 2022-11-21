package sparkcore

import org.apache.spark.sql.SparkSession

object RDDMINMAXDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                  .appName("RDD MIN MAX tranformations")
                    .master("local[*]")
                    .getOrCreate()
    val sc = spark.sparkContext

    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                     15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
                     27, 28, 29, 30, 31, 32, 33, 34, 35)
    val numRdd = sc.parallelize(data)

    println("Minimum value of RDD "+numRdd.min)
    println("Maximum value of RDD "+numRdd.max)
    println("Mean value of RDD "+numRdd.mean())
    println("sum value of RDD "+numRdd.sum())
    //top(n) return array of  a max values from rdd
    println("top 2 value of RDD "+numRdd.top(2))

    //fold
    //foldBykey
    //foldLeft
    //foldRight



  }
}
