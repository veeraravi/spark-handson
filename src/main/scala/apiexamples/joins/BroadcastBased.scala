package apiexamples.joins

import java.io.{File, FileReader, BufferedReader}

import apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.collection.mutable

/**
 * Created by Veeraravi on 20/1/15.
 */
object BroadcastBased {


  def parseCustomerData(customerDataPath: String): mutable.Map[String, String] = {

    val customerMap = mutable.Map[String, String]()

    val bufferedReader = new BufferedReader(new FileReader(new File(customerDataPath)))
    var line: String = null
    while ((({
      line = bufferedReader.readLine;
      line
    })) != null) {
      val values = line.split(",")
      customerMap.put(values(0), values(1))
    }
    bufferedReader.close()
    customerMap
  }


  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "apiexamples")
    val salesRDD = sc.textFile(args(1))
    val customerDataPath = args(2)
    val customerMap = parseCustomerData(customerDataPath)

    //broadcast data

    val customerBroadCast = sc.broadcast(customerMap)

    val joinRDD = salesRDD.map(row => {
      val salesRecord = SalesRecordParser.parse(row).right.get
      val customerName = customerBroadCast.value(salesRecord.customerId)
      (customerName,salesRecord)
    })


    println(joinRDD.collect().toList)

  }


}
