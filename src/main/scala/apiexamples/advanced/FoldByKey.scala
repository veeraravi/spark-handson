package apiexamples.advanced

import apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by Veeraravi on 28/1/15.
 */
object FoldByKey {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })
    val byCustomer = salesRecordRDD.map(salesRecord => (salesRecord.customerId,salesRecord.itemValue))
    val maxByCustomer = byCustomer.foldByKey(Double.MinValue)((acc,itemValue) => {
      if(itemValue > acc ) itemValue else acc
    })
    println(maxByCustomer.collect().toList)

  }

}
