package sparksql
import org.apache.spark.sql.SparkSession


object CompareDFs {
  def main(args: Array[String]): Unit = {
   // import spark.implicits._
   val spark = SparkSession.builder()
     .appName("compare Dataframes")
     .master("local[*]")
     .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val d1 = Seq(
      (3, "Chennai", "rahman", "9848022330", 45000, "SanRamon"),
      (1, "Hyderabad", "ram", "9848022338", 50000, "SF"),
      (2, "Hyderabad", "robin", "9848022339", 40000, "LA"),
      (4, "sanjose", "romin", "9848022331", 45123, "SanRamon"))
    val d2 = Seq(
      (3, "Chennai", "rahman", "9848022330", 45000, "SanRamon"),
      (1, "Hyderabad", "ram", "9848022338", 50000, "SF"),
      (2, "Hyderabad", "robin", "9848022339", 40000, "LA"),
      (4, "sanjose", "romin", "9848022331", 45123, "SanRamon"),
      (4, "sanjose", "romino", "9848022331", 45123, "SanRamon"),
      (5, "LA", "Test", "1234567890", 12345, "Testuser"))

    import spark.implicits._

    val df1 = d1.toDF("emp_id" ,"emp_city" ,"emp_name" ,"emp_phone" ,"emp_sal" ,"emp_site")
    val df2 = d2.toDF("emp_id" ,"emp_city" ,"emp_name" ,"emp_phone" ,"emp_sal" ,"emp_site")

    spark.sql("((select * from df1) union (select * from df2)) " +
      "minus ((select * from df1) intersect (select * from df2))").show //spark is SparkSession

    //----------------------------

    val dfA = spark.createDataset(Seq((1,"a",11),(2,"b",2),(3,"c",33),(5,"e",5))).toDF("n", "s", "i")
    val dfB = spark.createDataset(Seq((1,"a",11),(2,"bb",2),(3,"cc",34),(4,"d",4))).toDF("n", "s", "i")

   // def diff(key: String, df1: DataFrame, df2: DataFrame): DataFrame =
    /* above definition */

      //diff("n", df1, df2).show(false)
      case class Person(personid: Int, personname: String, cityid: Int)
 /*   val df11 = Seq(
      Person(0, "AgataZ", 0),
      Person(1, "Iweta", 0),
      Person(2, "Patryk", 2),
      Person(9999, "Maria", 2),
      Person(5, "John", 2),
      Person(6, "Patsy", 2),
      Person(7, "Gloria", 222),
      Person(3333, "Maksym", 0)).toDF

    val df21 = Seq(
      Person(0, "Agata", 0),
      Person(1, "Iweta", 0),
      Person(2, "Patryk", 2),
      Person(5, "John", 2),
      Person(6, "Patsy", 333),
      Person(7, "Gloria", 2),
      Person(4444, "Hans", 3)).toDF

    val joined = df11.join(df21, df11("personid") === df21("personid"), "outer")
    val newNames = Seq("personId1", "personName1", "personCity1", "personId2", "personName2", "personCity2")
    val df_Renamed = joined.toDF(newNames: _*)

    // Some deliberate variation shown in approach for learning
    val df_temp = df_Renamed.filter($"personCity1" =!= $"personCity2" || $"personName1" =!= $"personName2" || $"personName1".isNull || $"personName2".isNull || $"personCity1".isNull || $"personCity2".isNull).select($"personId1", $"personName1".alias("Name"), $"personCity1", $"personId2", $"personName2".alias("Name2"), $"personCity2").  withColumn("PersonID", when($"personId1".isNotNull, $"personId1").otherwise($"personId2"))

    val df_final = df_temp.withColumn("nameChange ?", when($"Name".isNull or $"Name2".isNull or $"Name" =!= $"Name2", "Yes").otherwise("No")).withColumn("cityChange ?", when($"personCity1".isNull or $"personCity2".isNull or $"personCity1" =!= $"personCity2", "Yes").otherwise("No")).drop("PersonId1").drop("PersonId2")

    df_final.show()
  }
  def diff(key: String, df1: DataFrame, df2: DataFrame): DataFrame = {
    val fields = df1.schema.fields.map(_.name)
    val diffColumnName = "Diff"

    df1
      .join(df2, df1(key) === df2(key), "full_outer")
      .withColumn(
        diffColumnName,
        when(df1(key).isNull, "New row in DataFrame 2")
          .otherwise(
            when(df2(key).isNull, "New row in DataFrame 1")
              .otherwise(
                concat_ws("",
                  fields.map(f => when(df1(f) =!= df2(f), s"$f ").otherwise("")):_*
                )
              )
          )
      )
      .filter(col(diffColumnName) =!= "")
      .select(
        fields.map(f =>
          when(df1(key).isNotNull, df1(f)).otherwise(df2(f)).alias(f)
        ) :+ col(diffColumnName):_*
      )
      */
  }

  
}
