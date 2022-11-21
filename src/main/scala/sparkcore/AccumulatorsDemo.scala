package sparkcore

import org.apache.spark.sql.SparkSession

object AccumulatorsDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(" Relational Trans Demo")
      .master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val textRDD = sc.textFile("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\sales.csv")

    println(":: number of partition :: "+textRDD.getNumPartitions)

    val rdd2 = textRDD.map(line => line.split(","))

    //rdd2.foreach(ele => println(ele))
    val rdd3 = rdd2.repartition(4)
    var counter = 0
    val accum = sc.accumulator(0.toInt,"ERROR RECORDS")
    val res = rdd3.map(
      ele => {
      if(ele.length < 4){
        accum +=1
        counter +=1;
        ele
      }
    })
    println(accum)
    println("local varibale counter "+counter)

    val rdd4 = rdd3.map(ele => (ele,1))

   // rdd4.foreach(ele => println(ele))

    val countWords = rdd4.reduceByKey((a,b) => a+b).persist()
    countWords.foreach(ele => println(ele))
    countWords.saveAsTextFile("file:\\D:\\veeraravi\\veeraravi\\drivedata\\DataSets\\wordCount21\\")
    println(" DAG PLAN "+countWords.toDebugString)

    //sc.stop()



  }

}
