package com.spark.regex

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object pivot_note {

  def main(args: Array[String]) = {
    System.out.println("application starting-----------------------------");

    val spark = SparkSession
      .builder()
      .appName("pivotingfield")
      .master("local[*]")
     // .enableHiveSupport()
      .getOrCreate();

    // val inputPath = spark.read.table("/user/hive/warehouse/cdms/installation_note_version_vw/");
    System.out.println("reading data completed");
    // var outputPath = "/user/hive/warehouse/cdms/installation_note_version_reporting/";

   // val insDF = spark.sql("select * from cdms.installation_note_version_vw")

    val insDF = spark.read.format("csv").option("header", "true")
      .option("inferSchema", "true")
      .load("D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\sample-regex-data.csv")

    //  insDF.createOrReplaceTempView("insTable");

    //   val sqlDF = spark.sql("select * from staging");

    def noteSplitUDF = udf((s: String) => {
      var res: Array[String] = Array()
      if (s != null && !s.isEmpty()) {
        res = s.split("(?=[N][0-9]{1,9}?(\\s)?[-])(\\s)?")
      }
      res
    })

    // res1.select("inst_note_ver_id","inst_no","note_text","note_text_split").printSchema

    val res1 = insDF.withColumn("note_text_split", explode(noteSplitUDF(col("note_text"))))

    res1.coalesce(1).write.format("csv").option("header", "true")
      .option("inferSchema", "true")
      .save("D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\regex1")// val res2 = res1.withColumn("note_text_split", trim(col("note_text"))).withColumn("inst_no_id", concat_ws("-", col("inst_no"), substring(col("note_text"), 0, 3)))

   // res2.write.mode(SaveMode.Overwrite).saveAsTable("cdms.installation_note_version_reporting");

  }
}