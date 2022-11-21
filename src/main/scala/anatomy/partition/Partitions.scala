package anatomy.partition

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkContext

/**
 * Created by Veeraravi on 11/3/15.
 */
object Partitions {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0),"partition example")

    //actual textFile api converts to the following code
    val dataRDD = sc.hadoopFile(args(1), classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      sc.defaultMinPartitions).map(pair => pair._2.toString)



    println(dataRDD.partitions.length)

  }


}
