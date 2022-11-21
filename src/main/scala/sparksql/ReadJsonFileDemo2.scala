package sparksql

import org.apache.spark.sql.SparkSession


object ReadJsonFileDemo2 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Read Json File Demo")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val multiLineJson = "src/main/resources/dell.json"

    val mulDf = spark.read.option("multiline","true").json(multiLineJson)
   // println("====== multiline json =======")
 //mulDf.show(false)
   // mulDf.coalesce(1).write.saveAsTe("src/main/resources/dell.out")



   // spark.sqlContext.sql("CREATE TEMPORARY VIEW zipcode USING json OPTIONS" +
    //  " (path 'src/main/resources/zipcodes.json')")
   // spark.sqlContext.sql("select * from zipcode").show(false)

    import spark.implicits._

    // create an RDD of tuples with some data
    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, " "),
      (1, "Widget Co", 0.0, 10.00, " "),
      (1, "Widget Co", 0.0, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA"),
      (6, "Widget Co", 12000.00, 10.00, "AZ")
    )
    val customerRows = spark.sparkContext.parallelize(custs, 4)

    // convert RDD of tuples to DataFrame by supplying column names

    val customerDF = customerRows.toDF("id", "name", "sales", "discount", "state")

    println("*** Here's the whole DataFrame with duplicates")

    customerDF.printSchema()

    //customerDF.groupBy("id").agg(collect_list("mappingcol"))

  }
}
