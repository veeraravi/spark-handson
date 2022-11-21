package extend

import apiexamples.serilization.SalesRecord
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Created by Veeraravi on 27/2/15.
 */
class CustomOperators(rdd:RDD[SalesRecord]) {

  def totalAmount = rdd.map(_.itemValue).sum
  
  def discount(discountPercentage:Double) = new DiscountRDD(rdd,discountPercentage)

}

object CustomOperators {

  implicit def toUtils(rdd: RDD[SalesRecord]) = new CustomOperators(rdd)
}
