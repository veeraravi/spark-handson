package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object DataFrameBasicTransfermations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Basic operations of  Dataframes")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val filePath = "src/main/resources/cars.json"
    val data = List(1,2,3,4,5)
    val carDataFrame = spark.read.json(filePath)
    //spark.read.text

    //show()
    //If you want to see top 20 rows of DataFrame in a tabular form then use the following command.
    carDataFrame.show

    //show(n)
    //If you want to see n rows of DataFrame in a tabular form then use the following command.
    //carDataFrame.show(2)
    //carDataFrame.show(false)
    //carDataFrame.show(2,false)

    //take()
    //take(n) Returns the first n rows in the DataFrame.
    //carDataFrame.take(2).foreach(println)

    //count()
    //Returns the number of rows.
    //carDataFrame.groupBy("speed").count().show()

    //head()
    //head () is used to returns first row.
    val resultHead = carDataFrame.head()

    //println(resultHead.mkString(","))

    //head(n)
    //head(n) returns first n rows.
    //val resultHeadNo = carDataFrame.head(3)

    //println(resultHeadNo.mkString(","))

    //first()
    //Returns the first row.
    val resultFirst = carDataFrame.first()

    println("fist:" + resultFirst.mkString(","))

    //collect()
    //Returns an array that contains all of Rows in this DataFrame.
    //val resultCollect = carDataFrame.collect()

    //println(resultCollect.mkString(","))

    //Basic DataFrame functions:
    //printSchema()
    //If you want to see the Structure (Schema) of the DataFrame, then use the following command.
    //carDataFrame.printSchema()

    //toDF()
    //toDF() Returns a new DataFrame with columns renamed. It can be quite convenient in conversion from a RDD of tuples into a DataFrame with meaningful names.
    //val car = sc.textFile("src/main/resources/fruits.txt")
    //.map(_.split(","))
    //.map(f => Fruit(f(0).trim.toInt, f(1), f(2).trim.toInt))
    //.toDF().show()

    //dtypes()
    //Returns all column names and their data types as an array.
    carDataFrame.dtypes.foreach(println)
    carDataFrame.printSchema()

    //columns ()
    //Returns all column names as an array.
    carDataFrame.columns.foreach(println)

    //cache()
    //cache() explicitly to store the data into memory.
    // Or data stored in a distributed way in the memory by default.
    val resultCache = carDataFrame.filter(carDataFrame("speed") > 300)

    resultCache.cache().show()

    //Data Frame operations:
    //sort()
    //Returns a new DataFrame sorted by the given expressions.
    //carDataFrame.sort($"itemNo".desc).show()
    //carDataFrame.sort("itemNo".desc).show()
    carDataFrame.sort(col("itemNo").desc).show()
    carDataFrame.sort(carDataFrame("itemNo").desc).show()

    //orderBy()
    //Returns a new DataFrame sorted by the specified column(s).
    //carDataFrame.orderBy(desc("speed")).show()

    //groupBy()
    //counting the number of cars who are of the same speed .
    //carDataFrame.groupBy("speed").count().show()

    //na()
    //Returns a DataFrameNaFunctions for working with missing data.
    //carDataFrame.na.drop().show()

    //as()
    //Returns a new DataFrame with an alias set.
    //carDataFrame.select(avg($"speed").as("avg_speed")).show()

    //alias()
    //Returns a new DataFrame with an alias set. Same as as.
    //carDataFrame.select(avg($"weight").alias("avg_weight")).show()

    //select()
    //To fetch speed-column among all columns from the DataFrame.
    //carDataFrame.select("speed").show()

    //filter()
    //filter the cars whose speed is greater than 300 (speed > 300).
    //carDataFrame.filter(carDataFrame("speed") > 300).show()

    //where()
    //Filters age using the given SQL expression.
    //carDataFrame.where($"speed" > 300).show()

    //agg()
    //Aggregates on the entire DataFrame without groups.
    //carDataFrame.agg(max($"speed")).show()

    //limit()
    //Returns a new DataFrame by taking the first n rows.
    // The difference between this function and head is that head
    // returns an array while limit returns a new DataFrame.
    //carDataFrame1.limit(3).show()

    //unionAll()
    //Returns a new DataFrame containing union of rows in this frame
    // and another frame.
    //carDataFrame.unionAll(empDataFrame2).show()


    //intersect()
    //Returns a new DataFrame containing rows only in both this frame
    // and another frame.
    //carDataFrame1.intersect(carDataFrame).show()

    //except()
    //Returns a new DataFrame containing rows in this frame
    // but not in another frame.
    //carDataFrame.except(carDataFrame1).show()

    //withColumn()
    //Returns a new DataFrame by adding a column
    // or replacing the existing column that has the same name.

    val resDF = carDataFrame.withColumn("no_of_gears",lit(6))

    val coder: (Int => String) = (arg: Int) => {
    if (arg < 300) "slow" else "high speed"
    }
    val sqlfunc = udf(coder)

    carDataFrame.withColumn("First", sqlfunc(col("speed"))).show()


    //withColumnRenamed()
    //Returns a new DataFrame with a column renamed.
    //empDataFrame2.withColumnRenamed("id", "employeeId").show()

    //drop()
    //Returns a new DataFrame with a column dropped.
    //carDataFrame.drop("speed").show()

    //dropDuplicates()
    //Returns a new DataFrame that contains only the unique rows from this DataFrame.
    // This is an alias for distinct.
    //carDataFrame.dropDuplicates().show()

    //describe()
    //describe returns a DataFrame containing information such as
    // number of non-null entries (count),mean, standard deviation,
    // and minimum and maximum value for each numerical column.
    //carDataFrame.describe("speed").show()

    //carDataFrame.
  }
}
