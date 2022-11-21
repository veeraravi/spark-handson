package sparksql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._
object WhenTransfermation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Basic operations of  Dataframes")
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

    val df2 = df.select(col("name.firstname").as("fname"),
      col("name.middlename").as("mname"),
      col("name.lastname").as("lname"),
      col("dob"),col("gender"),col("salary"))
    df2.printSchema()
    df2.show()
    val df3 = df2.withColumn("new_gender",
      when(col("gender") === "M","Male")
      .when(col("gender") === "F","Female")
      .otherwise("Unknown"))
    df3.printSchema()
    df3.show()
    val df4 = df2.select(col("*"), when(col("gender") === "M","Male")
      .when(col("gender") === "F","Female")
      .otherwise("Unknown").alias("new_gender"))
    //using expr()
    df4.printSchema()
    df4.show()
    val df5 = df2.withColumn("new_gender",
      expr("case when gender = 'M' then 'Male' " +
        "when gender = 'F' then 'Female' " +
        "else 'Unknown' end"))
    df5.printSchema()
    df5.show()
    val df6 = df2.select(col("*"),
      expr("case when gender = 'M' then 'Male' " +
        "when gender = 'F' then 'Female' " +
        "else 'Unknown' end").alias("new_gender"))
    df6.printSchema()
    df6.show()
    import spark.implicits._

    val dataDF = Seq(
      (66, "a", "4"), (67, "a", "0"), (70, "b", "4"), (71, "d", "4"
      )).toDF("id", "code", "amt")
    dataDF.printSchema()
    dataDF.show()
    dataDF.withColumn("new_column",
      when(col("code") === "a" || col("code") === "d", "A")
        .when(col("code") === "b" && col("amt") === "4", "B")
        .otherwise("A1"))
      .show()
    dataDF.distinct()
  }

}
