package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._



object CreateDataFrames {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Creating Dataframes")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val empData = Seq(
      ("Veera", "Jan", 45000),("Ravi", "Jan", 36000), ("Veera", "Feb", 47000),
      ("Veera", "Mar", 50000), ("Veera", "Apr", 46000), ("Ravi", "Feb", 40000),
      ("Ravi", "Mar", 38000), ("Ravi", "Apr", 41000),   ("Kumar", "Jan", 44000),
      ("Kumar", "Feb", 42000), ("Kumar", "Mar", 50000), ("Kumar", "Apr", 46000),
      ("Raja", "Jan", 40000), ("Raja", "Feb", 42000), ("Raja", "Mar", 36000),
      ("Raja", "Apr", 38000),("Ram", "Jan", 50000), ("Ram", "Feb", 44000),
      ("Ram", "Mar", 40000), ("Ram", "Apr", 39000),("madhu", "Jan", 27000),
      ("madhu", "Feb", 30000), ("madhu", "Mar", 35000), ("madhu", "Apr", 37000),
      ("Vamsi", "Jan", 36000), ("Vamsi", "Feb", 40000), ("Vamsi", "Mar", 38000),
      ("Vamsi", "Apr", 41000),("Haritha", "Jan", 45000), ("Haritha", "Feb", 44000),
      ("Haritha", "Mar", 38000), ("Haritha", "Apr", 47000))

    val cols = Seq("EmpName","month","income")

    // ------ toDF ------------
    import spark.implicits._
    val empRDD = sc.parallelize(empData);
    val empDF = empRDD.toDF(cols:_*) // empRDD.toDF("empName","month","income")
    empDF.printSchema()
    empDF.show();

    // ----- createDataFrame function------
    val cDF = spark.createDataFrame(empData).toDF("EmpName","month","income")
    //cDF.printSchema()
    //cDF.show()

    // ---- another way of toDF -----

    val dataDF = empData.toDF("EmpName","month","income")

    //dataDF.printSchema()
   // dataDF.show()

    val seqEmpRows = Seq(  Row("Veera", "Jan", 45000),Row("Ravi", "Jan", 36000), Row("Veera", "Feb", 47000),
  Row("Veera", "Mar", 50000), Row("Veera", "Apr", 46000), Row("Ravi", "Feb", 40000),
  Row("Ravi", "Mar", 38000), Row("Ravi", "Apr", 41000),   Row("Kumar", "Jan", 44000),
  Row("Kumar", "Feb", 42000), Row("Kumar", "Mar", 50000), Row("Kumar", "Apr", 46000),
  Row("Raja", "Jan", 40000), Row("Raja", "Feb", 42000), Row("Raja", "Mar", 36000),
  Row("Raja", "Apr", 38000),Row("Ram", "Jan", 50000), Row("Ram", "Feb", 44000),
  Row("Ram", "Mar", 40000), Row("Ram", "Apr", 39000),Row("madhu", "Jan", 27000),
  Row("madhu", "Feb", 30000), Row("madhu", "Mar", 35000), Row("madhu", "Apr", 37000),
  Row("Vamsi", "Jan", 36000), Row("Vamsi", "Feb", 40000), Row("Vamsi", "Mar", 38000),
  Row("Vamsi", "Apr", 41000),Row("Haritha", "Jan", 45000), Row("Haritha", "Feb", 44000),
  Row("Haritha", "Mar", 38000), Row("Haritha", "Apr", 47000))

    val rowRdd = sc.parallelize(seqEmpRows)

    val empFields = List(
      StructField("empName",StringType,true),
      StructField("month",StringType,true),
      StructField("income",IntegerType,true)
    )
    val empSchema = StructType(empFields)

    val empDf1 = spark.createDataFrame(rowRdd,empSchema);

    //empDf1.printSchema()
   // empDf1.show()

    // ----read csv file ------

   // val agentCustDf = spark.read.csv("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\agent-cust.csv")
   /* val agentCustDf = spark.read.format("csv").option("inferSchema","true").option("header","true").csv("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\agent-cust.csv")
*/

   /* val agentCustDf = spark.read.options(
      Map("sep"->",","header"->"true","inferSchema"->"true")).csv("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\agent-cust.csv")
*/
    val agentCustFields = List(
      StructField("mgr_name",StringType,true),
      StructField("clnt_name",StringType,true),
      StructField("clt_gndr",StringType,true),
      StructField("clnt_age",IntegerType,true),
      StructField("resp _time",DoubleType,true),
      StructField("stat_lvl",DoubleType,true)
    )
    val agentCustSchema = StructType(agentCustFields)

   /* val agentCustDf = spark.read.options(
      Map("sep"->",","header"->"true","inferSchema"->"true")).schema(agentCustSchema).csv("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\agent-cust.csv")
    */

    val agentCustDf = spark.read.format("csv")
    //  .option("inferSchema","true")
      .option("header","true")
        .schema(agentCustSchema).load("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\agent-cust.csv")
   // agentCustDf.printSchema()
   // agentCustDf.show()

    // ----read JSON file ------
     val peopleDf = spark.read.json("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\people.json")
     val claimDF = spark.read.json("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\claim.json")

    //claimDF.printSchema()
    //claimDF.show()

    val jdbcDf = spark.read.format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/retail_db")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","categories").load()

    jdbcDf.printSchema()
    jdbcDf.show()

 /*   val df = spark.createDF(
      List(
        (Array("a", "b"), Array(1, 2)),
        (Array("x", "y"), Array(33, 44))
      ), List(
        ("letters", ArrayType(StringType, true), true),
        ("numbers", ArrayType(IntegerType, true), true)
      )
    )*/
  }

}
