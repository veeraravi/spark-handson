package spark_hbase

import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog

object HBaseSparkRead {

  def main(args: Array[String]): Unit = {

    def catalog =
      s"""{
         |"table":{"namespace":"default", "name":"employee"},
         |"rowkey":"key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"key", "type":"string"},
         |"fName":{"cf":"person", "col":"firstName", "type":"string"},
         |"lName":{"cf":"person", "col":"lastName", "type":"string"},
         |"mName":{"cf":"person", "col":"middleName", "type":"string"},
         |"addressLine":{"cf":"address", "col":"addressLine", "type":"string"},
         |"city":{"cf":"address", "col":"city", "type":"string"},
         |"state":{"cf":"address", "col":"state", "type":"string"},
         |"zipCode":{"cf":"address", "col":"zipCode", "type":"string"}
         |}
         |}""".stripMargin

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("hbase read")
      .getOrCreate()

  /*  val hbaseDF = sparkSession.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    hbaseDF.printSchema()

    hbaseDF.show(false)

    hbaseDF.filter($"key" === "1" && $"state" === "FL")
      .select("key", "fName", "lName")
      .show()

    //Create Temporary Table on DataFrame
    hbaseDF.createOrReplaceTempView("employeeTable")
*/
    //Run SQL
    sparkSession.sql("select * from employeeTable where fName = 'Amaya' ").show

  }
}