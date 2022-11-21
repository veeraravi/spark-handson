package com.spark_xml

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number, udf}

//import com.xyz.util.LoggerUtils
//import com.xyz.util.PropertyUtils
import org.apache.spark.sql.types.DataTypes
object SparkXMLParse {
  val log: Logger = Logger.getLogger(SparkXMLParse.getClass)
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    val startTimestamp: String = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    println("started at : " + startTimestamp)

    //UDF to get the application status by calling API
    def getAppStatus(application_number:String):String={
      //scala rest client call
      "status"
    }

    val getAppStatusUDF = udf(getAppStatus(_:String))

    val spark = SparkSession.builder.appName("SparkXMLParse").master("local[*]").getOrCreate()
    var xmlDF = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "references-cited")
      .option("rowTag", "citation")
      .load("file:\\C:\\Users\\KAMAKSHITHAI\\Downloads\\ipg101026\\ipg101026.xml")
    var resultDF = xmlDF.select(
      col("category"),
      col("patcit.document-id.country").as("country"),
      col("patcit.document-id.doc-number").as("Application_Number"),
      col("Application_Type")
    ).where(col("Application_Number").isNotNull)

    resultDF = resultDF.withColumn("Application_Type", lit("General").cast(DataTypes.StringType))
      .withColumn("SerialNumber", row_number().over(Window.orderBy("Application_Number")))
      .withColumn("appStatus", getAppStatusUDF(col("Application_Number")))

    resultDF.printSchema()

    resultDF.show()

    resultDF.write.mode(SaveMode.Overwrite).saveAsTable("test.ipg101026")

    spark.stop();

    val totalTimeConsumedInSecs = (System.currentTimeMillis() - startTime) / 1000
    println(s"Total Time consumed :  $totalTimeConsumedInSecs seconds")
    print("Ended at : " + LocalDateTime.now())
  }
}
