package anatomy.partition

import org.apache.hadoop.mapred.lib.HashPartitioner
import org.apache.spark.SparkContext

/**
 * Created by Veeraravi on 11/3/15.
 */
object HashPartitioning {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0),"hash partitioning example")

    val salesData = sc.textFile(args(1))

    val salesByCustomer = salesData.map(value => {
      val colValues = value.split(",")
      (colValues(1),colValues(3).toDouble)
    })

    val groupedData = salesByCustomer.groupByKey()

    println("default partitions are "+groupedData.partitions.length)

    //increase partitions
    val groupedDataWithMultiplePartition = salesByCustomer.groupByKey(2)
    println("increased partitions are " + groupedDataWithMultiplePartition.partitions.length)

  }

}
