package com.spark_xml

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
object SparkXMLParser {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("XML parse")
    val sc = new SparkContext(conf)

   // val sc=SparkContext.getOrCreate() // Creating spark context object
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "us-bibliographic-data-grant")
      .load("file:\\C:\\Users\\KAMAKSHITHAI\\Downloads\\ipg101026\\ipg101026.xml")

   // val flattened = df.withColumn("LineItem", explode(df("RetailTransaction.LineItem")))

   /* val selectedData = flattened.select("RetailStoreID","WorkstationID",
                               "OperatorID._OperatorName",
                                 "OperatorID._VALUE","CurrencyCode",
                                   "RetailTransaction.ReceiptDateTime",
                                   "RetailTransaction.TransactionCount",
                                  "LineItem.SequenceNumber","LineItem.Tax.TaxableAmount")
*/
    val selectedDf = df.select(df("application-reference.document-id.doc-number").as("Application_Number"),
      df("application-reference.document-id.country").alias("country"),
      df("category").alias("category")
    )
val selectedDf2 = selectedDf.withColumn("Application_Type",lit("General"))
    selectedDf2.createOrReplaceTempView("tempTable")
      val finalDf = sqlContext.sql("select row_number() over (order by application_Number) as SerialNumber, * from tempTable")
    finalDf.write.mode(SaveMode.Overwrite).saveAsTable("test.ipg101026")

    finalDf.show(10,false)
   /* val spark = SparkSession
      .builder()
      .appName("Integrate SensorDataUpload Process")
      .config("hive.exec.dynamic.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.parallel", true)
      .config("hive.enforce.bucketing", true)
      .config("spark.ui.showConsoleProgress",false)
      .getOrCreate()*/



   // val spark = SparkSession.builder().appName("testings").master("local").config("configuration key", "configuration value").getOrCreate

  }
}
