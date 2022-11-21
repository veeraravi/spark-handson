package sparksql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object NestedSchemaStructure {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Nested Schema Structure in Dataframes")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val data = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
      Row(Row("Michael ","Rose",""),"40288","M",4000),
      Row(Row("Robert ","","Williams"),"42114","M",4000),
      Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
      Row(Row("Jen","Mary","Brown"),"","F",-1)
    )

    val schema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)
    val rowRDD = sc.parallelize(data)
    val df = spark.createDataFrame(rowRDD,schema)
    df.printSchema()
    df.show
    df.select(col("name.firstname").as("fName"),col("name.middlename").as("mName"),
              col("name.lastname").as("lName"),col("dob").as("dateOfBirth"),
              col("gender").as("gender"),
              col("salary").as("sal")).show
  }
}
