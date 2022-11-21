package hadoopintegration.read

import apiexamples.serilization.SalesRecord
import hadoopintegration.SalesRecordWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext

/**
 * Created by Veeraravi on 20/1/15.
 */
object ObjectFileRead {
  def main(args: Array[String]) {

    val sc = new SparkContext(args(0),"apiexamples")
    val dataRDD = sc.objectFile[SalesRecord](args(1))
    println(dataRDD.collect().toList)

  }

}
