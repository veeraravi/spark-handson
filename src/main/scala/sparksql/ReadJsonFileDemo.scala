package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object ReadJsonFileDemo {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Read Json File Demo")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val filePath = "src/main/resources/cars.json"

    val df = spark.read.json(filePath)
    // df.show

    //multilineJson
    val multiLineJson = "src/main/resources/cars-formatted.json"

    val mulDf = spark.read.option("multiline","true").json(multiLineJson)
   // println("====== multiline json =======")
   // mulDf.show


    //Multiple Single line Json sets
    val singleLineJson = "src/main/resources/cars-single-line.json"
    val singDf = spark.read.option("multiline","true").json(singleLineJson)
   // singDf.show

    //Reading from multiple paths
   val multiPathDf = spark.read.json(
      "src/main/resources/zipcodes_streaming/zipcode1.json",
      "src/main/resources/zipcodes_streaming/zipcode2.json")
    //  multiPathDf.show(false)
    //read all files from a folder
    val allFiles = spark.read.json("src/main/resources/zipcodes_streaming")
   // allFiles.show(false)

    // read json by passing schema
    //Define custom schema
    val schema = new StructType()
      .add("RecordNumber",IntegerType,true)
      .add("Zipcode",IntegerType,true)
      .add("ZipCodeType",StringType,true)
      .add("City",StringType,true)
      .add("State",StringType,true)
      .add("LocationType",StringType,true)
      .add("Lat",DoubleType,true)
      .add("Long",DoubleType,true)
      .add("Xaxis",IntegerType,true)
      .add("Yaxis",DoubleType,true)
      .add("Zaxis",DoubleType,true)
      .add("WorldRegion",StringType,true)
      .add("Country",StringType,true)
      .add("LocationText",StringType,true)
      .add("Location",StringType,true)
      .add("Decommisioned",BooleanType,true)
      .add("TaxReturnsFiled",StringType,true)
      .add("EstimatedPopulation",IntegerType,true)
      .add("TotalWages",IntegerType,true)
      .add("Notes",StringType,true)
    val df_with_schema = spark.read.schema(schema)
      .json("src/main/resources/zipcodes.json")
    //df_with_schema.printSchema()
    //df_with_schema.show(false)

    spark.sqlContext.sql("CREATE TEMPORARY VIEW zipcode USING json OPTIONS" +
      " (path 'src/main/resources/zipcodes.json')")
    spark.sqlContext.sql("select * from zipcode").show(false)


  }
}
