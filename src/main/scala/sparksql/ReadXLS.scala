package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadXLS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("READ XLS 2x")
      .master("local[*]")
      .getOrCreate()

   /* val spark = SparkSession.builder().appName(sessionName)
      .config(conf=conf).enableHiveSupport().getOrCreate()*/

    val sc = spark.sparkContext
    val  conf = new SparkConf()
    conf.set("spark.default.parallelism","200")
    conf.set("spark.sql.shuffle.partitions", "8")
    conf.set("hive.exec.dynamic.partition","true")
    conf.set(" hive.exec.dynamic.partition.mode","nonstrict")
    conf.set(" mapred.job.queue.name"," root.haddeveloper")


   /* val olympicDF = spark.read.format("csv")
                    .option("header","false")
                    .option("inferSchema","true")
                    .option("delimiter","\t")
                    .load("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\data\\categories\\categories.xls")
    olympicDF.show()
*/
/*

    val real_estate_DF = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("delimiter",",")
      .load("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\real_state.csv")
    real_estate_DF.show()
*/

    val sessionName = "HiveContextSpark"


    val df = spark.read.format("com.crealytics.spark.excel")
      //.option("dataAddress", "A1") // Optional, default: "A1"
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("inferSchema", "false")
      .option("addColorColumns", "false")
      .load("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\data\\categories\\categories.xls")

    val rdd = df.rdd.mapPartitionsWithIndex{
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

    df.printSchema()
    df.show()
/*

    df.coalesce(1).
      write.
      format("com.databricks.spark.csv").
      mode("overwrite").
      option("header", "true").
      save("/user/hive/warehouse/vehicle_analytics.db/xpo_outofservice/CWY_OUTOFSERVICE_EXPORT.csv")
*/

//val custDF = spark.read.
  }
}
