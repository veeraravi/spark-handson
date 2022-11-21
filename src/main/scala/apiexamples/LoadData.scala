package apiexamples

import org.apache.spark.SparkContext

/**
 * Created by Veeraravi on 20/1/15.
 */
object LoadData {

  def main(args: Array[String]) {
    //create spark context
    val sc = new SparkContext(args(0),"apiexamples")

    //it creates RDD[String] of type MappedRDD

    val dataRDD = sc.textFile(args(1))

    //print the content . Converting to List just for nice formatting
    println(dataRDD.collect().toList)
  }


}
