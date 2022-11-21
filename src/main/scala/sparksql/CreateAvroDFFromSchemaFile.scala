package sparksql

import java.io.File
import java.nio.file.Paths

import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession

object CreateAvroDFFromSchemaFile {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Read Avro File Demo")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val filePath = "src/main/resources/zipcodes.avro"

    val df = spark.read.format("avro").load(filePath)

    //println(df.schema.prettyJson)

    //val path = "file:\\D:\\veeraravi\\veeraravi\\spark\\veera-code\\spark-handson\\src\\main\\resources\\zipcodes.avro"
    //df.show(false)

    val avroRdd = sc.textFile(filePath)
   // avroRdd.foreach(ele => println("**"+ele))
   val schemaAvro = new Schema.Parser()
     .parse(new File("src/main/resources/zipcodes.avsc"))
    val jmap = new java.util.HashMap[String, String]()
    jmap.put("mode", "PERMISSIVE")
   val schema = new String(java.nio.file.Files.readAllBytes(Paths.get("src/main/resources/zipcodes.avsc")))
    import spark.implicits._
    val adf = avroRdd.toDF().selectExpr("CAST(value AS STRING) as value")
 // .select(from_avro($"value",schema,jmap).as("value")).select("value.*")
   adf.show(false)
/*
    val url = ClassLoader.getSystemResource("schema.json")
    val schemaSource = Source.fromFile(url.getFile).getLines.mkString
    //val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]
    //.option("mode", "DROPMALFORMED")
    val df2 = spark.read.schema(schemaFromJson )
      .option("badRecordsPath", "file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\badrecords")
     // .option("mode", "PERMISSIVE")
     // .option("columnNameOfCorruptRecord", "_corrupt_record")
     // .json("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\sample-json.txt")
      .json(jsonRdd)
    df2.printSchema()
    df2.show(false)*/
  }
}
