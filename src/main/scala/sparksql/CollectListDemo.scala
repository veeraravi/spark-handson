package sparksql

import org.apache.spark.sql.functions.{array_contains, col, _}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CollectListDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Collect list/set/map demo")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    val arrayStructData = Seq(
      Row("James", "Java"), Row("James", "C#"),Row("James", "Python"),
      Row("Michael", "Java"),Row("Michael", "PHP"),Row("Michael", "PHP"),
      Row("Robert", "Java"),Row("Robert", "Java"),Row("Robert", "Java"),
      Row("Washington", null)
    )
    val arrayStructSchema = new StructType().add("name", StringType)
      .add("booksInterested", StringType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructData),arrayStructSchema)
    df.printSchema()
    df.show(false)
    /*
    * def collect_list(e : org.apache.spark.sql.Column) : org.apache.spark.sql.Column
      def collect_list(columnName : scala.Predef.String) : org.apache.spark.sql.Column
      def collect_set(e : org.apache.spark.sql.Column) : org.apache.spark.sql.Column
      def collect_set(columnName : scala.Predef.String) : org.apache.spark.sql.Column
    */
    val df2 = df.groupBy("name").agg(collect_list("booksIntersted")
      .as("booksInterested"))
    df2.printSchema()
    df2.show(false)
    df.groupBy("name").agg(collect_set("booksInterested")
      .as("booksInterestd"))
      .show(false)

    // collect as map
    val dept = List(("Finance",10),("Marketing",20),
      ("Sales",30), ("IT",40))
    val rdd=spark.sparkContext.parallelize(dept)
    val dataColl=rdd.collect()
    dataColl.foreach(println)
    dataColl.foreach(f=>println(f._1 +","+f._2))
    val dataCollLis=rdd.collectAsMap()
    dataCollLis.foreach(f=>println(f._1 +","+f._2))

    //array_contains()

    val data = Seq(
      Row("James,,Smith",List("Java","Scala","C++"),"CA"),
      Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
      Row("Robert,,Williams",null,"NV")
    )

    val schema2 = new StructType()
      .add("name",StringType)
      .add("languagesAtSchool", ArrayType(StringType))
      .add("currentState", StringType)

    val df3 = spark.createDataFrame(
      spark.sparkContext.parallelize(data),schema2)
    df.printSchema()
    df.show(false)

    val df21=df3.withColumn("Java Present",
      array_contains(col("languagesAtSchool"),"Java"))
    df21.show(false)
    val df32=df3.where(array_contains(col("languagesAtSchool"),"Java"))
    df32.show(false)
  }
}
