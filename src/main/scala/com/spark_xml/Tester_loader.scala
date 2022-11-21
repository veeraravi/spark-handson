/*
package com.spark_xml

//---------------spark_import
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

//----------------xml_loader_import
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ LongWritable, Text }
import com./**/cloudera.datascience.common.XmlInputFormat

object Tester_loader {
  case class User(account: String, name: String, number: String)
  def main(args: Array[String]): Unit = {

    val sparkHome = "/usr/big_data_tools/spark-1.5.0-bin-hadoop2.6/"
    val sparkMasterUrl = "spark://SYSTEMX:7077"

    var jars = new Array[String](3)

    jars(0) = "/home/hduser/Offload_Data_Warehouse_Spark.jar"
    jars(1) = "/usr/big_data_tools/JARS/Spark_jar/avro/spark-avro_2.10-2.0.1.jar"

    val conf = new SparkConf().setAppName("XML Reading")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .setSparkHome(sparkHome)
      .set("spark.executor.memory", "512m")
      .set("spark.default.deployCores", "12")
      .set("spark.cores.max", "12")
      .setJars(jars)

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //---------------------------------loading user from XML

    val pages = readFile("src/input_data", "<user>", "<\\user>", sc) //calling function 1.1

    val xmlUserDF = pages.map { tuple =>
    {
      val account = extractField(tuple, "account")
      val name = extractField(tuple, "name")
      val number = extractField(tuple, "number")

      User(account, name, number)
    }
    }.toDF()
    println(xmlUserDF.count())
    xmlUserDF.show()
  }

  //------------------------------------Functions

  def readFile(path: String, start_tag: String, end_tag: String, sc: SparkContext) = {
    val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, start_tag)
    conf.set(XmlInputFormat.END_TAG_KEY, end_tag)
    val rawXmls = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable],
      classOf[Text], conf)

    rawXmls.map(p => p._2.toString)
  }

  def extractField(tuple: String, tag: String) = {
    var value = tuple.replaceAll("\n", " ").replace("<\\", "</")

    if (value.contains("<" + tag + ">") && value.contains("</" + tag + ">")) {

      value = value.split("<" + tag + ">")(1).split("</" + tag + ">")(0)

    }
    value
  }
}*/
