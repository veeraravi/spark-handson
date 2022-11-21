package sparksql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CastToTSDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("cast to timestamp ")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.sqlContext.implicits._

    //String to timestamps
    val df = Seq(("2019-07-01 12:01:19.000"),
      ("2019-06-24 12:01:19.000"),
      ("2019-11-16 16:44:55.406"),
      ("1211")).toDF("input_timestamp")
    df.withColumn("datetype_timestamp",to_timestamp(col("input_timestamp"))).show(false)

    //when dates are not in Spark DateType format 'yyyy-MM-dd  HH:mm:ss.SSS'.
    //Note that when dates are not in Spark DateType format, all Spark functions returns null
    //Hence, first convert the input dates to Spark DateType using to_timestamp function
    val dfDate = Seq(("07-01-2019 12 01 19 406"),
      ("06-24-2019 12 01 19 406"),
      ("11-16-2019 16 44 55 406"),
      ("11-16-2019 16 50 59 406")).toDF("input_timestamp")

    dfDate.withColumn("datetype_timestamp",
      to_timestamp(col("input_timestamp"),"MM-dd-yyyy HH mm ss SSS"))
      .show(false)
    //Convert string to timestamp when input string has just time
    val df1 = Seq(("12:01:19.345"),
      ("12:01:20.567"),
      ("16:02:44.406"),
      ("16:50:59.406"))
      .toDF("input_timestamp")

    df1.withColumn("datetype_timestamp",
      to_timestamp(col("input_timestamp"),"HH:mm:ss.SSS"))
      .show(false)
    Seq(("06-03-2009"),("07-24-2009")).toDF("Date").select(
      col("Date"),
      to_date(col("Date"),"MM-dd-yyyy").as("to_date")
    ).show()
//converting Timestamp to String
    Seq(1).toDF("seq").select(
      current_timestamp().as("current_date"),
      date_format(current_timestamp(),"yyyy MM dd").as("yyyy MM dd"),
      date_format(current_timestamp(),"MM/dd/yyyy hh:mm").as("MM/dd/yyyy"),
      date_format(current_timestamp(),"yyyy MMM dd").as("yyyy MMMM dd"),
      date_format(current_timestamp(),"yyyy MMMM dd E").as("yyyy MMMM dd E")
    ).show(false)


  }
}
