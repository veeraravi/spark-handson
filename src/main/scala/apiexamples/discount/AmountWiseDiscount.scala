package apiexamples.discount

import apiexamples.serilization.{SalesRecord, SalesRecordParser}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by Veeraravi on 20/1/15.
 */
object AmountWiseDiscount {

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    val totalAmountByCustomer = salesRecordRDD.map(row => (row.customerId, row.itemValue)).reduceByKey(_ + _)

    val discountAmountByCustomer = totalAmountByCustomer.map {
      case (customerId, totalAmount) => {
        if (totalAmount > 500) {
          val afterDiscount = totalAmount - (totalAmount * 10) / 100.0
          (customerId, afterDiscount)
        }
        else (customerId, totalAmount)
      }
    }

    println(discountAmountByCustomer.collect().toList)


  }

}
