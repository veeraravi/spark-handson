package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
case class Movies(movieName: String, rating: Double, ts: String)

object DataSetDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataSet Demo")
      .master("local[*]") // Standalone, YARN and Mesos
      .getOrCreate()
    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")

    import spark.implicits._
    val dataset_movie=Vector(new Movies("test", 4.5, "11212")).toDS();

    dataset_movie.show();


    var dataframe = spark.read.csv("D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\movies_data.txt")
    dataframe = dataframe.withColumnRenamed("_c0", "movieName")
    dataframe = dataframe.withColumnRenamed("_c1", "rating")
    dataframe = dataframe.withColumnRenamed("_c2", "ts")

    dataframe= dataframe.withColumn("rating", col("rating").cast("double"))
    var dataset=dataframe.as[Movies]
    dataset.show()
    var doubledataframe=dataframe.select(col("rating")).as[Double]
    doubledataframe.show()

    // RDD to DataSet

    var rdd = spark.sparkContext.textFile("D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\movies_data.txt")
    val ds = rdd.map(ele => ele.split(","))
                .map(ele => Movies(ele(0),ele(1).toDouble,ele(2))).toDS()



  }
}
