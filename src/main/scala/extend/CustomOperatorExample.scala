package extend

import apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext

/**
 * Created by Veeraravi on 27/2/15.
 */
object CustomOperatorExample {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })
    import extend.CustomOperators._
    salesRecordRDD.totalAmount
  }

}
