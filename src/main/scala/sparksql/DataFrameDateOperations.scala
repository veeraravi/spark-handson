package sparksql

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object DataFrameDateOperations {
  case class orig(city: String, product: String, Jan: Int, Feb: Int, Mar: Int)
  case class newOne(city: String, product: String, metric_type: String, metric_value: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Basic operations of  Dataframes")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val df1 = Seq(
      ("red", "2016-11-29 07:10:10.234"),
      ("green", "2016-11-29 07:10:10.234"),
      ("orange", "2020-05-26 07:10:10.234"),
      ("pink", "2020-05-26 07:00:10.234"),
      ("black", "2020-05-26 6:30:10.234"),
      ("blue", "2016-11-29 07:10:10.234")).toDF("color", "date")



   var tsDF =  df1.withColumn("ts",unix_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss.S")
      .cast("timestamp"))
println(Timestamp.valueOf(LocalDateTime.now().minusHours(1)))
   // tsDF = tsDF.withColumn("ts1",Timestamp.valueOf(LocalDateTime.now().minusHours(1)))
    tsDF.printSchema()
    tsDF.show(false)

    df1.where(
      unix_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss.S")
        .cast("timestamp")
        .between(
          Timestamp.valueOf(LocalDateTime.now().minusHours(1)),
          Timestamp.valueOf(LocalDateTime.now())
        ))
      .show()




    val df2 = Seq(("c1", "p1", 123, 22, 34),
                  ("c2", "p2", 234, 432, 43))
      .toDF("city", "product", "Jan", "Feb", "Mar")
    df2.printSchema()
    df2.show()

    val newDf = df2.as[orig].flatMap(
      v => Seq(newOne(v.city, v.product, "Jan", v.Jan),
        newOne(v.city, v.product, "Feb", v.Feb),
        newOne(v.city, v.product, "Mar", v.Mar)))
    newDf.printSchema()
    newDf.show()


    val df3 = Seq(
      ("c1", "p1", 123, 22, 34),
      ("c2", "p2", 234, 432, 43)).toDF("city", "product", "col1", "col2", "col3")


    val index = List("city", "product")
    val columns = List( "col1", "col2", "col3")

    val arrayedDF = df3.withColumn("combined", array(columns.head, columns.tail: _*))
    arrayedDF.printSchema()
    arrayedDF.show()

    val resDF = arrayedDF.select("city", "product", "combined")

    resDF.show


    val explodedDF = arrayedDF.selectExpr("city", "product", "posexplode(combined) as (pos, metricValue)")

    val months = List("Jan", "Feb", "Mar")
    val u =  udf((p: Int) => months(p))

    val targetDF = explodedDF.withColumn("metric_type", u($"pos")).drop("pos")

    targetDF.show()

    val df4 = Seq(("c1", "p1", 123, 22, 34), ("c2", "p2", 234, 432, 43)).toDF("city", "product", "Jan", "Feb", "Mar")

    df3.show
//    +----+-------+---+---+---+
//    |city|product|Jan|Feb|Mar|
//    +----+-------+---+---+---+
//    |  c1|     p1|123| 22| 34|
//    |  c2|     p2|234|432| 43|
//    +----+-------+---+---+---+

    // schema of the result data frame
    val schema = StructType(List(StructField("city", StringType, true),
      StructField("product", StringType,true),
      StructField("metric_type", StringType, true),
      StructField("metric_value", IntegerType, true)))



    // use flatMap to convert each row into three rows
    val rdd = df3.rdd.flatMap(
      row => {
        val index_values = index.map(i => row.getAs[String](i))
        months.map(m => Row.fromSeq(index_values ++ List(m, row.getAs[Int](m))))
      }
    )

    spark.createDataFrame(rdd, schema).show

  }
}
