package spark.core.rdd.spark.core.rdd.youtube

import org.apache.spark.sql.SparkSession

object DefaultNumPatitionsDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
                  .appName("Partitions Demo")
                    .master("local[5]")
                    .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val data = Array(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
    val parallelizeRdd = sc.parallelize(data)
    println("No of partitions : "+parallelizeRdd.getNumPartitions)
    val fileRdd1 = sc.textFile("file:\\D:\\veeraravi\\veeraravi\\spark\\batch-7-10\\spark-programs\\dataforwordcount.txt")
    println("No of partitions textFile Api: "+fileRdd1.getNumPartitions)

    val biggerFileRdd=sc.textFile("file:\\D:\\veeraravi\\veeraravi\\drivedata\\biggerfile.txt")
    println("No of partitions textFile Api biggerFileRdd: "+biggerFileRdd.getNumPartitions)


    val emptyRdd1 = sc.emptyRDD
    println("No of partitions emptyRdd Api : "+emptyRdd1.getNumPartitions)

    val emptyIntRDD = sc.emptyRDD[Int]
    println("No of partitions emptyRdd Api emptyIntRDD : "+emptyIntRDD.getNumPartitions)

    val emptyParallRdd = sc.parallelize(Seq.empty[Int])
    println("No of partitions emptyParallRdd Api  : "+emptyParallRdd.getNumPartitions)

    type idName = (Int,String)

    val pairRdd = sc.emptyRDD[idName]
    println("No of partitions pairRdd emptyRDD Api  : "+pairRdd.getNumPartitions)
  }

}
