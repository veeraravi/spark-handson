package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object DataFrameOperations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Basic operations of  Dataframes")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val empData = Seq(("Veera", "Jan", 45000),("Ravi", "Jan", 36000), ("Veera", "Feb", 47000),
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

    // ----- createDataFrame function------
    val cDF = spark.createDataFrame(empData).toDF("EmpName","month","income")
    val selected = cDF.select("EmpName","income")
   // selected.show()
    val fileted = cDF.filter(cDF("income") > 40000)
    //fileted.show

    // sql way

    cDF.createOrReplaceTempView("emp")
    val sqlFilter = spark.sql("select * from emp where income > 40000")
   // sqlFilter.show()

    // add column and rename

    val empGross = cDF.withColumn("tax",col("income")*0.2)
                     .withColumnRenamed("income","sal")
    val empDf2 = empGross.withColumn("comp_name",lit("BigDataSolutions"))
    //empDf2.printSchema()
    // drop column
    val dropCol = empGross.drop("comp_name")

    // udf
    val netSalUDF = udf((sal:Int)=> {
      val netIncome = sal - (sal*0.2)
      netIncome
    })

    val netSalDf = empDf2.withColumn("netSal",netSalUDF(col("sal")))
    netSalDf.printSchema()
   // netSalDf.show()

    // groupby

    val groupByMonth = netSalDf.groupBy("empName").agg(avg("sal"))
    groupByMonth.show

  }
}
