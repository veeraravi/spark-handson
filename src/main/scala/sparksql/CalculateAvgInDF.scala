package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
object CalculateAvgInDF {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Calculate Avg in DataFrames")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val empFilePath = "src/main/resources/empWithDimensions.csv"
    val empCityFilePath = "src/main/resources/empCity.csv"


    val df1 = spark.read.csv(empFilePath).toDF(
      "name", "age", "title", "salary", "dimensions"
    )

    val df2 = spark.read.csv(empCityFilePath).toDF(
      "name", "location"
    )



    val dfAverage = df1.join(df2, Seq("name")).
      groupBy(df2("location")).
      agg(
        avg(split(df1("dimensions"), ("\\*")).getItem(0).cast(IntegerType)).as("avg_length"),
        avg(split(df1("dimensions"), ("\\*")).getItem(1).cast(IntegerType)).as("avg_width")
      ).
      select(
        col("location"), col("avg_length"), col("avg_width"),
        concat(col("avg_length"), lit("*"),
               col("avg_width")).as("avg_dimensions")
      )

    dfAverage.show
  }
}
