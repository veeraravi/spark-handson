package anatomy.partition

import org.apache.spark.SparkContext

/**
 * Created by Veeraravi on 11/3/15.
 */
object LookUp {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "look up example")

    val salesData = sc.textFile(args(1))

    val salesByCustomer = salesData.map(value => {
      val colValues = value.split(",")
      (colValues(1), colValues(3).toDouble)
    })


    val groupedData = salesByCustomer.groupByKey(new CustomerPartitioner)

    val groupedDataWithPartitionData = groupedData.mapPartitionsWithIndex((partitionNo, iterator) => {
      println(" accessed partition " + partitionNo)
      iterator
    }, true)


    println("for accessing customer id 1")
    groupedDataWithPartitionData.lookup("1")


  }


}
