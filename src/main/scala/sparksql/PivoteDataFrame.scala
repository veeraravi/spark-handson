package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PivoteDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Nested Schema Structure in Dataframes")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

    import spark.sqlContext.implicits._
    val df = data.toDF("Product","Amount","Country")
    df.show()

    //To get the total amount exported to each country of each product,
    // will do group by Product, pivot by Country, and the sum of Amount.

    val pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
    pivotDF.show()

    val countries = Seq("USA","China","Canada","Mexico")
    val pivotDF2 = df.groupBy("Product").pivot("Country", countries).sum("Amount")
    pivotDF2.show()


    val pivotDF3 = df.groupBy("Product","Country")
      .sum("Amount")
      .groupBy("Product")
      .pivot("Country")
      .sum("sum(Amount)")
    pivotDF3.show()

    //unpivot
    val unPivotDF = pivotDF3.select($"Product",
      expr("stack(4, 'Canada', Canada, 'China', China, 'Mexico', Mexico,'USA', USA) as (Country,Total)"))
      .where("Total is not null")
    unPivotDF.show()
  }
}
