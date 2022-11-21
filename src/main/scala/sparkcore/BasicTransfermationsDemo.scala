package sparkcore

import org.apache.spark.sql.SparkSession

object BasicTransfermationsDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Basic transfermations")
      .master("local[*]").getOrCreate()
val sc = spark.sparkContext
    // different ways to create RDD
    //1. parallelize
    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11,12,13,14,15,16,17,18,19,20,
      21,22,23,24,25,26,27,28,29,30,
      31,32,33,34,35)
    val rdd = sc.parallelize(data)

    println(":: number of partition :: "+rdd.getNumPartitions)

    val rdd1 = sc.parallelize(data,4)

    println(":: number of partition :: "+rdd1.getNumPartitions)

    // 2. load data from files
    // the textFile api returns content of the file
    val textRDD = sc.textFile("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\sampledataforwordcount.txt")

    println(":: number of partition :: "+textRDD.getNumPartitions)

    // the wholetextfile api retuns (filepath,content)
    val wholeRdd = sc.wholeTextFiles("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\sampledataforwordcount.txt")

    println(":: number of partition :: "+textRDD.getNumPartitions)

    val rdd2 = sc.textFile("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\spark-programs\\")

   // operation on RDD 1. transfermations  2. action
    //1. transfermation :- Transfermations are lazy operations, immutable and it retuns another RDD
    //2. Action are operations to compute and retunr value

    //map  --> 1:1
    // print even numbers from RDD
    val evenNums = rdd.map(ele => ele*10)
    // flatMap
    val words = textRDD.flatMap(line => line.split(" "))

     val data1 = List(1,2,3,4,5,6,7,8,9)

    val mapTest = data1.map(ele => List(ele,ele*10))

     data1.flatMap(ele => List(ele,ele*10))
//3. filter
         val rdd12 = sc.parallelize(data)

        rdd12.filter(ele => ele % 2 == 0).collect

         val namedata = List("SPARK","VEERA","RAVIKUMAR")

         val chars = namedata.map(ele => ele.split(""))

         val fruits = List("banana","apple","orange","Kiwi","mango","apple","Kiwi","goa","pineApple")

         fruits.filter(ele => ele.startsWith("apple"))
  }
}
