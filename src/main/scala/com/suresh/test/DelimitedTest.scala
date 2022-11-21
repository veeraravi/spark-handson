/*
package com.suresh.test
import org.apache.spark.sql.SparkSession
object DelimitedTest {

  def main(args: Array[String]): Unit = {
    case class orig(city: String, product: String, Jan: Int, Feb: Int, Mar: Int)
    case class newOne(city: String, product: String, metric_type: String, metric_value: Int)

    val spark = SparkSession
      .builder()
      .appName("Event Hub Test")
      .config("hive.exec.dynamic.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.parallel", true)
      .config("hive.enforce.bucketing", true)
      // .config("spark.sql.shuffle.partitions",shufflePartition)
      .config("spark.ui.showConsoleProgress", false)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val df = Seq(("c1", "p1", 123, 22, 34), ("c2", "p2", 234, 432, 43)).toDF("city", "product", "Jan", "Feb", "Mar")

    val newDf = df.as[orig].flatMap(v => Seq(newOne(v.city, v.product, "Jan", v.Jan), newOne(v.city, v.product, "Feb", v.Feb), newOne(v.city, v.product, "Mar", v.Mar)))
    newDf.write.option("sep", "|").option("header", "true").csv("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\delimited\\results.txt")
   // df.write.format("csv").option("header","true").option("delimiter","|").save("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\delimited\\results2.txt")

  }
}
*/
