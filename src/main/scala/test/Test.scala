package test

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
object Test  {
def main(args:Array[String]):Unit ={
  val spark = SparkSession.builder().appName("Test")
    .master("local[*]").getOrCreate();
  val sc = spark.sparkContext;

  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )

  val schema =  new StructType(Array(
    StructField("name",StringType,false),
    StructField("dept",StringType,false),
    StructField("sal",IntegerType,false)))

val rdd = sc.parallelize(simpleData);
  val rdd2 = rdd.map(ele => Row(ele._1,ele._2,ele._3))

  val df1 = spark.createDataFrame(rdd2,schema);
  val df2 = spark.createDataFrame(rdd2,schema);
  df1.show()

 // val partWindow = Window.partitionBy(col("dept")).orderBy(col("sal").desc)
 // sal    rnk  dnsRnk
  //10000  1      1
  //9000   2      2
  //9000   2      2
  //8000   4      3

  //val dnsRNk

  val joinedDF = df1.join(df2,df1("deptId")=== df2("dept"),"inner")

}
}
