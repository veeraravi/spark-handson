package extend

import apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext

/**
 * Discount RDD example
 */
object DiscountRDDExample {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "discount RDD example")
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    import extend.CustomOperators._
    val discountRDD = salesRecordRDD.discount(0.1)
    println(discountRDD.collect().toList)
  }


}
