package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object GroupByDemo {
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("GroupBy demo")
        .master("local[*]")
        .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val df1 = spark.createDataFrame(
      Seq((1, "andy", 20, "USA"),
        (2, "jeff", 23, "China"),
        (3, "james", 18, "USA"))).toDF("id", "name", "age", "country")

    // You can call agg function after groupBy directly, such as count/min/max/avg/sum
    val df2 = df1.groupBy("country").count()
    df2.show()

    // Pass a Map if you want to do multiple aggregation
    val df3 = df1.groupBy("country").agg(Map("age"->"avg", "id" -> "count"))
    df3.show()

    // Or you can pass a list of agg function
    val df4 = df1.groupBy("country").agg(avg("age").as("avg_age"), count("id").as("count"))
    df4.show()

    // You can not pass Map if you want to do multiple aggregation on the same column as the key of Map should be unique. So in this case
    // you have to pass a list of agg functions
    val df5 = df1.groupBy("country").agg(avg("age").as("avg_age"), max("age").as("max_age"))
    df5.show()
    val srcdf2 = spark.createDataFrame(
      Seq((1, "andy", 20, "USA"),
        (2, "jeff", 23, "China"),
        (3, "james", 18, "USA"),
        (3, "james", 18, "UK"),
        (3, "james", 18, "IN"),
        (2, "jeff", 23, "USA"),
        (2, "jeff", 23, "JP"))).toDF("id", "name", "age", "country")
  val df6 = srcdf2.groupBy("id").agg(collect_list("country"))

    df6.show
  }
}
