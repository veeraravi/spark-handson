package sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}

object SparkStreamingFromSocket {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("kafka-streaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    // install netcat in linux
    // sudo apt install netcat
    //nc -l -p 9090
    val df = spark.readStream
      .format("socket")
      .option("host","192.168.1.100")
      .option("port","7890")
      .load()

    df.printSchema()

    val wordsDF = df.select(explode(split(df("value")," ")).alias("word"))

    val count = wordsDF.groupBy("word").count()

    val query = count.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }
}
