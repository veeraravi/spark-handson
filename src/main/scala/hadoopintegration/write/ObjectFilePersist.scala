package hadoopintegration.write

import apiexamples.serilization.SalesRecordParser
import org.apache.spark.SparkContext

/**
 * Created by Veeraravi on 20/1/15.
 */
object ObjectFilePersist {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "hadoopintegration")
    val dataRDD = sc.textFile(args(1))
    val outputPath = args(2)
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    salesRecordRDD.saveAsObjectFile(outputPath)

  }

}
