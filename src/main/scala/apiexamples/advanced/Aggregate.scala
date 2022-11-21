package apiexamples.advanced

import apiexamples.serilization.{SalesRecord, SalesRecordParser}
import org.apache.spark.SparkContext

/**
 * Created by Veeraravi on 28/1/15.
 */
object Aggregate {
  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "apiexamples")
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    val zeroValue = (Double.MaxValue,0.0)

    val seqOp = ( minMax:(Double,Double), record: SalesRecord) => {
        val currentMin = minMax._1
        val currentMax = minMax._2
        val min = if(currentMin > record.itemValue) record.itemValue else currentMin
        val max = if(currentMax < record.itemValue) record.itemValue else currentMax
        (min,max)
      }

    val combineOp = (firstMinMax:(Double,Double), secondMinMax:(Double,Double)) => {
      ((firstMinMax._1 min secondMinMax._1), (firstMinMax._2 max secondMinMax._2))
     }

    val minmax = salesRecordRDD.aggregate(zeroValue)(seqOp,combineOp)

    println("mi = "+ minmax._1 +" and max = "+ minmax._2)


  }





}
