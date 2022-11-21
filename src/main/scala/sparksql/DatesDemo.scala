package sparksql

import org.apache.spark.sql.SparkSession
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object DatesDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Basic Date operations of  Dataframes")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val rdd = sc.parallelize(List(
      Row(1, Date.valueOf("2016-09-30")),
      Row(2, Date.valueOf("2016-12-14"))
    ))
    val personDetails = StructType(List(
      StructField("person_id",IntegerType,true),
      StructField("birth_date",DateType,true)
    ))
    var sourceDF = spark.createDataFrame(rdd, personDetails)
    sourceDF.printSchema()
    sourceDF.show()

    sourceDF=sourceDF.withColumn(
      "birth_date",
      col("birth_date").cast("date")
    )
    sourceDF.printSchema()
    sourceDF.show()

    sourceDF.withColumn(
      "birth_year",
      year(col("birth_date"))
    ).withColumn(
      "birth_month",
      month(col("birth_date"))
    ).withColumn(
      "birth_day",
      dayofmonth(col("birth_date"))
    ).withColumn("birth_quarter",quarter(col("birth_date"))).show()
   // datediff()
   // The datediff() and current_date() functions can be used to calculate the number of days
   // between today and a date in a DateType column.
   // Let's use these functions to calculate someone's age in days.

    sourceDF.withColumn(
      "age_in_days",
      datediff(current_timestamp(), col("birth_date"))
    ).show()
    val someData = Seq(
      Row(1, Date.valueOf("1972-03-31"),Date.valueOf("2015-10-23")),
      Row(2, Date.valueOf("1972-10-31"),Date.valueOf("2015-10-23")),
      Row(3, Date.valueOf("1972-10-31"),Date.valueOf("2015-03-23"))
    )

    val someSchema = List(
      StructField("person_id", IntegerType, true),
      StructField("birth_date", DateType, true),
      StructField("death_date", DateType, true)
    )

    val someDF = spark.createDataFrame(
      spark.sparkContext.parallelize(someData),
      StructType(someSchema)
    )
    someDF.withColumn(
      "age_in_days",
      datediff(col("death_date"), col("birth_date"))
    ).show()

    someDF.withColumn(
      "age_in_years",
      (datediff(col("death_date"), col("birth_date"))/365).cast("int")
    ).show()

    import spark.implicits._

    val df = Seq("20191001").toDF("ACCTG_DT")

    val df1 = df.withColumn("year",
      year(from_unixtime(unix_timestamp(col("ACCTG_DT"),"yyyyMMdd"),"yyyy-MM-dd")))
      .withColumn("month",month(from_unixtime(unix_timestamp(col("ACCTG_DT"),"yyyyMMdd"),"yyyy-MM-dd")))
      .withColumn("day",dayofmonth(from_unixtime(unix_timestamp(col("ACCTG_DT"),"yyyyMMdd"),"yyyy-MM-dd")))
   println("---- date conversion from string to date ------")
    df1.show()
   // date_add()
   // The date_add() function can be used to add days to a date. Let's add 15 days to a date column.
    sourceDF.withColumn(
      "15_days_old",
      date_add(col("birth_date"), 15)
    ).show()

    val rdd2 = sc.parallelize(List(
      Row(1, Timestamp.valueOf("2017-12-02 03:04:00")),
      Row(2, Timestamp.valueOf("1999-01-01 01:45:20"))
    ))
    val pDetails = StructType(List(
      StructField("person_id",IntegerType,true),
      StructField("fun_time",TimestampType,true)
    ))
    val sourceDF2 = spark.createDataFrame(rdd2, pDetails)
    sourceDF2.withColumn(
      "fun_minute",
      minute(col("fun_time"))
    ).withColumn(
      "fun_second",
      second(col("fun_time"))
    ).show()

  }
  }
