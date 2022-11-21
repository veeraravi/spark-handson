package apiexamples.discount

import apiexamples.serilization.{SalesRecord, SalesRecordParser}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by Veeraravi on 20/1/15.
 */
object ItemWiseDiscount {

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })


    //apply discount for each book from a customer

    val itemDiscountRDD = salesRecordRDD.map(salesRecord => {
      val itemValue = salesRecord.itemValue
      val newItemValue = itemValue - (itemValue * 5) / 100.0
      new SalesRecord(salesRecord.customerId,salesRecord.customerId,salesRecord.itemId,newItemValue)
    })

    val totalAmountByCustomer = itemDiscountRDD.map(row => (row.customerId,row.itemValue)).reduceByKey(_+_)
    println(totalAmountByCustomer.collect().toList)





  }

}
