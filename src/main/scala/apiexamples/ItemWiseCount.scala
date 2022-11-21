package apiexamples

import org.apache.spark.SparkContext

/**
 * Created by Veeraravi on 20/1/15.
 */
object ItemWiseCount {
  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))
    val itemPair = dataRDD.map(row => {
      val columns = row.split(",")
      (columns(2), 1)
    })
    /*
    itemPair is MappedRDD which is a pair. We can import the following to get more methods

     */
    import org.apache.spark.SparkContext._

    val result = itemPair.reduceByKey(_+_)

    println(result.collect().toList)

  }
}
