package sparkstreaming.batch

import org.apache.spark.sql.SparkSession
//https://spark.apache.org/docs/2.3.0/structured-streaming-kafka-integration.html
object SparkBatchConsumeFromKafka {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("kafka-streaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.1.100:9092")
      .option("subscribe", "text_topic6")
//      .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
//      .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")

//      .option("subscribePattern", "topic.*")
//      .option("startingOffsets", "earliest")
//      .option("endingOffsets", "latest")
      .load()

    df.printSchema()

    // Displays Data in Binary
    df.show()

    //// Displays Data in String
    val df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)","topic")
    df2.show(false)
  }
}
