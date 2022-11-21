package anatomy.partition

import anatomy.partition.CustomerPartitioner
import org.apache.spark.SparkContext

/**
 * Created by Veeraravi on 11/3/15.
 */
object CustomPartition {


  
  def main(args: Array[String]) {
    
    val sc = new SparkContext(args(0),"custom partitioning example")

    val salesData = sc.textFile(args(1))

    val salesByCustomer = salesData.map(value => {
      val colValues = value.split(",")
      (colValues(1),colValues(3).toDouble)
    })

    val groupedData = salesByCustomer.groupByKey(new CustomerPartitioner)

    //printing partition specific data

    val groupedDataWithPartitionData = groupedData.mapPartitionsWithIndex{
      case(partitionNo,iterator) => {
       List((partitionNo,iterator.toList.length)).iterator
      }
    }

    println(groupedDataWithPartitionData.collect().toList)
    
    
    
  }
  

}
