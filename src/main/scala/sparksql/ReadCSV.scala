package sparksql

import org.apache.spark.sql.SparkSession

object ReadCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("READ CSV 2x")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val olympicDF = spark.read.format("csv")
                    .option("header","false")
                    .option("inferSchema","true")
                    .option("delimiter","\t")
                    .load("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\olympix_data.csv")
    olympicDF.show()
    val real_estate_DF = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("delimiter",",")
      .load("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\real_state.csv")
    real_estate_DF.show()

//val custDF = spark.read.
  }
}
