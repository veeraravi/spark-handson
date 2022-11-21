package sparkstreaming.batch

import org.apache.spark.sql.SparkSession
//https://spark.apache.org/docs/2.3.0/structured-streaming-kafka-integration.html
object SparkBatchProduceToKafka {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("kafka-streaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val data = Seq (("iphone", "2007"),("iphone 3G","2008"),
      ("iphone 3GS","2009"),
      ("iphone 4","2010"),
      ("iphone 4S","2011"),
      ("iphone 5","2012"),
      ("iphone 8","2014"),
      ("iphone 10","2017"))

    val df = spark.createDataFrame(data).toDF("key","value")

    /*
      since we are using dataframe which is already in text,
      selectExpr is optional.
      If the bytes of the Kafka records represent UTF8 strings,
      we can simply use a cast to convert the binary data
      into the correct type.

      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    */
    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.1.100:9092")
      .option("topic","text_topic6")
      .save()


    val data2 = Seq((1,"James ","","Smith",2018,1,"M",3000),
      (2,"Michael ","Rose","",2010,3,"M",4000),
      (3,"Robert ","","Williams",2010,3,"M",4000),
      (4,"Maria ","Anne","Jones",2005,5,"F",4000),
      (5,"Jen","Mary","Brown",2010,7,"",-1)
    )

    val columns = Seq("id","firstname","middlename","lastname","dob_year",
      "dob_month","gender","salary")
    import spark.sqlContext.implicits._
    val df2 = data2.toDF(columns:_*)

    /*
      Writing Json as a Value to Kafka topic
     */
    df2.toJSON.write
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.1.100:9092")
      .option("topic","text_topic6")
      .save()

    /*
      Another way of Writing Json
      By sending key and value to Kafka
      using to_json()
      */
    df2.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.1.100:9092")
      .option("topic","text_topic6")
      .save()

  }
}
