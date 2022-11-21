package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
object Joins {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Joins in Dataframes")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._
    val left = Seq((1, "A1"), (2, "A2"), (3, "A3"), (4, "A4")).toDF("id", "value")
    val right = Seq((3, "A3"), (4, "A4"), (4, "A4_1"), (5, "A5"), (6, "A6")).toDF("id", "value")

    println("LEFT TABLE SORT")
    left.orderBy("id").show()

    println("RIGHT TABLE SORT")
    right.orderBy("id").show()

    val joinTypes = Seq("inner", "outer", "full", "full_outer", "left",
                   "left_outer", "right", "right_outer", "left_semi", "left_anti")

    joinTypes foreach { joinType =>
      println(s"${joinType.toUpperCase()} JOIN")
      left.join(right, Seq("id"), joinType).orderBy("id").show()
     // left.join(right, left("id")===right("id"), joinType).orderBy("id").show()
    }
/*
left.join(right, Seq("id"), "inner").orderBy("id").show()
left.join(right, left("id")===right("id"), "inner").orderBy("id").show()
 */

    val matches = spark.sparkContext.parallelize(Seq(
      Row(1, "John Wayne", "John Doe"),
      Row(2, "Ive Fish", "San Simon")))

    val players = spark.sparkContext.parallelize(Seq(
      Row("John Wayne", 1986),
      Row("Ive Fish", 1990),
      Row("San Simon", 1974),
      Row("John Doe", 1995)
    ))

    val matchesDf = spark.createDataFrame(matches, StructType(Seq(
      StructField("matchId", IntegerType, nullable = false),
      StructField("player1", StringType, nullable = false),
      StructField("player2", StringType, nullable = false)))
    ).as('matches)

    val playersDf = spark.createDataFrame(players, StructType(Seq(
      StructField("player", StringType, nullable = false),
      StructField("birthYear", IntegerType, nullable = false)
    ))).as('players)

  val joinedDF=  matchesDf
      .join(playersDf, col("matches.player1") === col("players.player"))

  val selectDF = joinedDF.select(col("matches.matchId").as( "matchId"), col("matches.player1").as( "player1"),
        col("matches.player2").as("player2"), col("players.birthYear").as("player1BirthYear"))

  val joined2Df =  selectDF .join(playersDf, col("player2") === col("players.player"))
      .select(col("matchId").as("MatchID"), col("player1").as("Player1"),
        col("player2").as("Player2"), col("player1BirthYear").as("BYear_P1"),
        col("players.birthYear").as("BYear_P2"))
      .withColumn("Diff", abs(col("BYear_P2").minus(col("BYear_P1"))))
      .show()


  }
}
