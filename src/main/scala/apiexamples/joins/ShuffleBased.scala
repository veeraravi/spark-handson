package apiexamples.joins

import apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Created by Veeraravi on 20/1/15.
 */
object ShuffleBased {

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "apiexamples")
    val salesRDD = sc.textFile(args(1))
    val customerRDD = sc.textFile(args(2))

    /*val joinedDf = salesDF.join(custDF,salesDf("custId") === custDf("custId") &&
    salesDF("name") === custDF("name"),"left_outer").select("".as() ....)

    select A.custId,A.custName from custumer A left outer join sales s on
    a.custId = s.custId and   a.name = s.name;
*/



    val salesPair = salesRDD.map(row => {
      val salesRecord = SalesRecordParser.parse(row).right.get
      (salesRecord.customerId,salesRecord)
    })

    val customerPair = customerRDD.map(row => {
      val columnValues = row.split(",")
      (columnValues(0),columnValues(1))
    })


    val joinRDD = customerPair.join(salesPair).map{
      case (customerId,(customerName,salesRecord)) => {
        (customerName,salesRecord)
      }
    }

    println(joinRDD.collect().toList)

  }


}
