package sparksql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col



object SelectVsSelectExprDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("select vs selectexpr Demo")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val df = spark.createDataFrame(data).toDF("language","users_count")
    df.select("language","users_count as count") //Example 1
    df.select(df("language"),df("users_count").as("count")) //Example 2
    df.select(col("language"),col("users_count")) ////Example 3
  }
}
