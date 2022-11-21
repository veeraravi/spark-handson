package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ReadCSVFileDemo {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Read CSV File Demo")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val filePath = "src/main/resources/simple-data.txt"

    val df = spark.read.options(Map("inferSchema" -> "true",
      "delimiter" -> "|", "header" -> "true"))
      .csv(filePath)
      .select("Gender", "BirthDate", "TotalCost",
              "TotalChildren", "ProductCategoryName")
    df.show(false)

    val df2 = df
      .filter("Gender is not null")
      .filter("BirthDate is not null")
      .filter("TotalChildren is not null")
      .filter("ProductCategoryName is not null")
    df2.show()

    val currentAge = udf{ (dob: java.sql.Date) =>
      import java.time.{LocalDate, Period}
      Period.between(dob.toLocalDate, LocalDate.now).getYears
    }

    df.withColumn("CurrentAge",
          when(col("BirthDate").isNotNull, currentAge(col("BirthDate")))).

      show()

  }
}
