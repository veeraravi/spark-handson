package cca_175_prep

import org.apache.spark.sql.SparkSession

object Practice_Test_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Practice_Test_2")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")
    /*
    q2:
    instructions:
Join the comma separated file located at below hdfs location to find out  customers who have placed more than 4 orders.
/user/cloudera/practice1/q2/orders
/user/cloudera/practice1/q2/customers
Input Schema

Schema for customer File
customer_id,customer_fname,......................................................

Schema for Order File
order_id,order_date,order_customer_id,order_status


Output Requirement:

Order status should be COMPLETE
Output should have customer_id,customer_fname,orders_count
Save the results in json format.
Result should be order by count of orders in ascending fashion.
Result should be saved in /user/cloudera/practice1/q2/output 
     */
    val df = spark.read.format("csv").option("inferSchema","true")
      .load("src/main/resources/practice1/q2")

    df.select("order_id","order_status").write
        .option("compression","gzip")
      .parquet("src/main/resources/practice1/output/q1/output2/")
    df.printSchema()
  }
}
