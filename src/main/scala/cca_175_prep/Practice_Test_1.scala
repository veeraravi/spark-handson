package cca_175_prep

import org.apache.spark.sql.SparkSession

object Practice_Test_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Practice_Test_1")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")
    /*
    q1:Convert snappy compressed avro data-files stored at hdfs location
     /user/cloudera/practice1/q1  into parquet file.
     Output Requirement:

Result should be saved in /user/cloudera/practice1/q1/output/
Output should consist of only order_id,order_status
Output file should be saved as Parquet file in gzip Compression.

     */
    val df = spark.read.format("avro")
      .load("src/main/resources/practice1/q1")
    df.select("order_id","order_status").write
        .option("compression","gzip")
      .parquet("src/main/resources/practice1/output/q1/output2/")
    df.printSchema()
  }
}
