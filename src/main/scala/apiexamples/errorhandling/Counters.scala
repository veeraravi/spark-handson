package apiexamples.errorhandling

import apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by Veeraravi on 20/1/15.
 */
object Counters {
  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))
    val malformedRecords = sc.accumulator(0)

    // foreach is a action
    dataRDD.foreach(row => {
      val parseResult = SalesRecordParser.parse(row)
      if(parseResult.isLeft){
        malformedRecords+=1
      }
    })

    //print the counter

    println("No of malformed records is =  " + malformedRecords.value)


  }

}
