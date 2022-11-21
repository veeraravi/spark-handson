package sparksql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object ConvertRDDToDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Convert RDD To DF")
      .master("local[*]") // Standalone, YARN and Mesos
      .getOrCreate()
    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")

    val empRDD = sc.textFile("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\spark-programs\\emp-data.txt")
    val deptRDD = sc.textFile("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\spark-programs\\dept-data.txt")

    val empWithOutHeader = empRDD.mapPartitionsWithIndex
    {
      (idx,itr) => if(idx == 0) itr.drop(1) else itr
    }
    // case class
    val mapRdd = empWithOutHeader.map(line => line.split(","))
       .map(ele => Employee(ele(0).toInt,ele(1),ele(2),ele(3),
         ele(4),ele(5).toDouble,ele(6),ele(7).toInt))
    import spark.implicits._
    val df = mapRdd.toDF()
    df.printSchema()
    df.show()
println("-----------------------------------------------")
    // create schema and pass to createDataframe method

    // The schema is encoded in a string
    val schemaString = "empno,ename,job,mgr,hiredate,sal,comm,deptno"

    // Generate the schema based on the string of schema
    val fields = List(
      StructField("empno",IntegerType,true),
      StructField("ename",StringType,true),
      StructField("job",StringType,true),
      StructField("mgr",StringType,true),
      StructField("hiredate",StringType,true),
      StructField("sal",DoubleType,true),
      StructField("comm",StringType,true),
      StructField("deptno",IntegerType,true)
    )
    val schema = StructType(fields)

    val rowRDD = empWithOutHeader.map(line => line.split(","))
      .map(ele => Row(ele(0).toInt,ele(1),ele(2),ele(3),
        ele(4),ele(5).toDouble,ele(6),ele(7).toInt))

    val empDF = spark.createDataFrame(rowRDD,schema)
    empDF.printSchema()
    empDF.show()

/*

    val empDF = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("delimiter",",")
      .load("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\spark-programs\\emp-data.txt")
*/

  }
  case class Employee(empno:Int,ename:String,job:String,mgr:String,
                      hiredate:String,sal:Double,comm:String,deptno:Int)

}
