package apiexamples

import org.apache.spark.SparkContext

/**
 * Created by Veeraravi on 20/1/15.
 */
object ItemFilter {
  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))
    val itemIDToSearch = args(2)

    val itemRows =  dataRDD.filter(row =>{
      val columns = row.split(",")
      val itemId = columns(2)
      if(itemId.equals(itemIDToSearch)) true
      else false
    })

    println(itemRows.collect().toList)
  }
}
