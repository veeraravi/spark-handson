package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}

import scala.io.Source

object CreateDFFromJsonSchemaFileArray {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Read Json File Demo")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val jsonRdd = sc.textFile("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\student-json.txt")
    println("No of partitions textFile Api: "+jsonRdd.getNumPartitions)

    val url = ClassLoader.getSystemResource("student-schema.json")
    val schemaSource = Source.fromFile(url.getFile).getLines.mkString
    //val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]
    //.option("mode", "DROPMALFORMED")
    val df2 = spark.read.schema(schemaFromJson )
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .option("primitivesAsString", true)
     // .json("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\sample-json.txt")
      .json(jsonRdd)
    df2.printSchema()
    df2.show(false)
  }
}
