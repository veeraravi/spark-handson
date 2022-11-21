package sparksql

import java.time._
import java.time.format._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object CalculateAgeByDate {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Calculate Age By Date")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
import spark.implicits._

    val df = Seq("1957-03-06","1959-03-06").toDF("dob")
    df.show()

    df.withColumn(AppConst.AGE,months_between(current_date,col("dob"))/12).show

var df2 = df.withColumn("age",datediff(col("dob"),current_date))

    println("================================================")


    def pmods = udf[Int,Int,Int]((dividend: Int,divisor:Int) => {
      println("dividend :"+dividend)
      println("divisor :"+divisor)
      var res: Int = 0
      if (divisor > 0) {
        res = (dividend) % divisor
      }
      Math.abs(res)
      // res
    })

    def date_subUDF = udf((date: String,noOfDays:Int) => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val runDay = LocalDate.parse(date.toString, formatter)
      val resDate = runDay.minusDays(noOfDays)
      resDate.toString
    })


  df2= df2.withColumn("pmod", pmods(col("age"),lit(7)))
    df2.withColumn("pmod2",
      date_subUDF(current_date(),col("pmod"))).show()


/*

    df2.withColumn("pmod2", date_sub(current_date(),
      getDays(col("pmod")))).show()
*/





      df.withColumn("sub",date_sub(current_date(),7)).show()
/*
* when(col("sls_d") >= col("report_start_d_minus_14") && col("sls_d") <= col("report_end_d"),
        date_sub(col("sls_d"), pmod(datediff(col("sls_d"), col("report_start_d_minus_14")), lit(7) ) ))
* */
  //spark.sqlContext.udf.register("getModUDF",getMod(_,_))
   // udf[Int, Int](getMod)
// val activeDevicesUDF = udf(Util.activeDevices(_: String, _: String))


  //  spark.udf.register("getModUDF",getMod(_,_))

   /* df2.withColumn("pmod2", date_sub(current_date(),
      (pmod(datediff(col("dob"),current_date),lit(7))))).show()*/

    df2.withColumn("res",date_sub(current_date(),Math.abs(7)))

  def getMod(dividend: Int,divisor:Int):Int={
    println("dividend :"+dividend)
    println("divisor :"+divisor)
    var res: Int = 0
    if (divisor > 0) {
      res = dividend % divisor
    }
  return Math.abs(res)
  }
}

  def getMod(date: String,noOfDays:Int):String={
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val runDay = LocalDate.parse(date.toString, formatter)
    val runDayMinus30 = runDay.minusDays(noOfDays)
    return runDayMinus30.toString
  }

}
