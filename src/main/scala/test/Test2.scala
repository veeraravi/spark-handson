package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object Test2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Test2").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val empData = Seq(
      ("Veera", "Jan", 45000),("Ravi", "Jan", 36000), ("Veera", "Feb", 47000),
      ("Veera", "Mar", 50000), ("Veera", "Apr", 46000), ("Ravi", "Feb", 40000),
      ("Ravi", "Mar", 38000), ("Ravi", "Apr", 41000),   ("Kumar", "Jan", 44000),
      ("Kumar", "Feb", 42000), ("Kumar", "Mar", 50000), ("Kumar", "Apr", 46000),
      ("Raja", "Jan", 40000), ("Raja", "Feb", 42000), ("Raja", "Mar", 36000),
      ("Raja", "Apr", 38000),("Ram", "Jan", 50000), ("Ram", "Feb", 44000),
      ("Ram", "Mar", 40000), ("Ram", "Apr", 39000),("madhu", "Jan", 27000),
      ("madhu", "Feb", 30000), ("madhu", "Mar", 35000), ("madhu", "Apr", 37000),
      ("Vamsi", "Jan", 36000), ("Vamsi", "Feb", 40000), ("Vamsi", "Mar", 38000),
      ("Vamsi", "Apr", 41000),("Haritha", "Jan", 45000), ("Haritha", "Feb", 44000),
      ("Haritha", "Mar", 38000), ("Haritha", "Apr", 47000))
    import spark.implicits._
    val empDf = empData.toDF("name","month","sal")
    empDf.printSchema()

    val res = empDf.withColumn("sal",($"sal")*2)
    val win = Window.partitionBy("sal")
    //val lag =
//val uniqueDf =
    res.show()
  }
}
