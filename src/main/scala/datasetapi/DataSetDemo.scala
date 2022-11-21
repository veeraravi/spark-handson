package datasetapi

import org.apache.spark.sql.SparkSession

object DataSetDemo {
  case class Person(empno: Int, ename: String,
                    job: String, mgr: Int,
                    hiredate: String, sal: Double,
                    comm: Double, deptno: Double)
  def main(args: Array[String]): Unit = {

    val spark2 = SparkSession.builder().appName("DataSetDemo")
      .master("local[*]").getOrCreate()
    val sc = spark2.sparkContext
    sc.setLogLevel("ERROR")
    val personDf = spark2.read.format("csv")
                              .option("header","true")
                              .option("inferSchema","true")
      .option("delimiter",",")
      .load("D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\PersonData.csv")
    //personDf.printSchema()
   // print("***** clas of datafrmae ****"+personDf.getClass)
   // personDf.show()

   // personDf.select("empno","ename","salary").show
import spark2.implicits._

    val ds=  spark2.read.csv("D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\PersonData.csv")
      .as[Person]
    ds.show

  val data=  Seq(
    Person (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
    Person(7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
    Person(7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
    Person (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
    Person(7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
    Person (7655, "MART", "SALESMAN", 7698, "28-Sep-81", 1200, 1400, 30),
    Person(7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
    Person(7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
    Person(7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
    Person(7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
    Person(7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
    Person(7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )
    val rdd = sc.makeRDD(data)
import spark2.implicits._
    val personDs= spark2.read
      .option("header","true")
      .option("inferSchema","true")
      .option("delimiter",",")
      .csv("D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\PersonData.csv").as[Person]
   // print("***** clas of dataset ****"+personDs.getClass)

     personDs.printSchema()
   // personDs.show()
  }
}
