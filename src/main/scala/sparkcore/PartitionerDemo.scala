package sparkcore

import org.apache.spark.sql.SparkSession

object PartitionerDemo {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("WordCount Example")
      .getOrCreate()
    val sc = spark.sparkContext
    // if you are reading from localfilesystem it will consider a
    // blocksize as partiton i.e 32mb is one block
    //
    val textRDD = sc.textFile("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\sampledataforwordcount.txt",3)

    println(":: number of partition :: "+textRDD.getNumPartitions)

    val rdd2 = textRDD.flatMap(line => line.split(" "))

    rdd2.foreach(ele => println(ele))

    val rdd3 = rdd2.map(ele => (ele,1))
    val myPartiRdd = rdd3.partitionBy(new MyPartitioner(26))
    rdd3.foreach(ele => println(ele))
    /*
    (Amazon, [1,1,1,1,1,1])
        [1,1,1,1,1,1]
          reduceByKey((a,b) =>a+b)
             it1 -> 1,1 ===> 2
             it2 -> 2,1 ===> 3
             it3 -> 3,1 ===> 4
             it4 -> 4,1 ===> 5
             it5 -> 5,1 ===> 6
     */
    val countWords = rdd3.reduceByKey((a,b) => a+b)

    countWords.foreach(ele => println(ele))
    countWords.saveAsTextFile("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\wordCount19\\")
    println(" DAG PLAN "+countWords.toDebugString)
  }
}
