package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object CreateDataFrames_2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Creating Dataframes")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    val data = Seq((9861,"spl","5G",499,"5G",900,"09-09-2021" ),
      (9861,"spl","5G",499,"4G",1052,"08-08-2021"),
      (9861,"spl","5G",699,"5G",1024,"01-01-2021"),
      (9862,"spl","4G",499,"5G",900,"09-09-2021" ),
      (9862,"spl","5G",499,"5G",900,"09-09-2021" ),
      (9862,"spl","5G",499,"5G",900,"09-09-2021" ),
      (9863,"spl","5G",499,"5G",900,"09-09-2021" ),
      (9863,"spl","5G",599,"5G",1024,"09-09-2021"),
      (9863,"spl","5G",499,"5G",1024,"07-09-2021"),
      (9863,"spl","5G",799,"5G",1024,"01-09-2021"))

    val cols = Seq("msisdn","plan","basebts","amt","phonetype","datausage","date")
    // ------ toDF ------------
    import spark.implicits._
    val empRDD = sc.parallelize(data);
    val empDF = empRDD.toDF(cols:_*) // empRDD.toDF("empName","month","income")
   /* empDF.printSchema()
    empDF.show();
*/
  //  val res2 = empDF.withColumn("noofdays",datediff(current_date(),col("date")))
    val res = empDF.groupBy(col("msisdn")).agg(sum(col("amt")))
    res.show()


  }

}
