package com.spark.basics
import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.spark._
object PropertiesTest {

  //spark-submit --master local[4] --driver-memory 4g --executor-memory  4g
  // --class com.primetherapeutics.spark.streaming.WriteStreamToHbase
  // /opt/iig/canonical_ingest/hbase-spark-test-0.0.1-SNAPSHOT-jar-with-dependencies.jar
  // local[4] 20 /opt/iig/canonical_ingest/wso2.properties


  def main(args: Array[String]): Unit = {
    //val spark = SparkSession.builder.appName("GcdfCanadaBatchA").enableHiveSupport.getOrCreate
    val conf = new SparkConf().setMaster("local[*]").setAppName("word count")
    val sc = new SparkContext(conf)
    val progprops: Properties = new Properties
    //val file = new File(args(0));
   // val envConf = new FileInputStream(getClass().getResource("src/main/resources/spark-conf/spark.properties").toURI)

    val envConf = new FileInputStream(args(0))
    progprops.load(envConf)

    val exec_mem = progprops.getProperty("spark.executor.memory")
    val exec_cores = progprops.getProperty("spark.executor.cores")
    val driv_mem = progprops.getProperty("spark.executor.memory")
    val driv_cores = progprops.getProperty("spark.driver.cores")
    val serializer = progprops.getProperty("spark.serializer")
    val exec_inst = progprops.getProperty("spark.executor.instances")
    val dflt_parallel = progprops.getProperty("spark.default.parallelism")
    val cbo_enbl = progprops.getProperty("spark.sql.cbo.enabled")
    val cbo_joinr = progprops.getProperty("spark.sql.cbo.joinReorder.enabled")
    val shuf_prt = progprops.getProperty("spark.sql.shuffle.partitions")
    val debug_maxstr = progprops.getProperty("spark.debug.maxToStringFields")
    val schdlr_mode = progprops.getProperty("spark.scheduler.mode")
println(exec_mem)
println(exec_cores)
println(driv_mem)


    /*spark.conf.set("spark.executor.memory", exec_mem)
    spark.conf.set("spark.executor.cores", exec_cores)
    spark.conf.set("spark.driver.memory", driv_mem)
    spark.conf.set("spark.driver.cores", driv_cores)
    spark.conf.set("spark.sql.cbo.enabled", cbo_enbl)
    spark.conf.set("spark.sql.cbo.joinReorder.enabled", cbo_joinr)
    spark.conf.set("spark.sql.shuffle.partitions", shuf_prt)
    spark.conf.set("spark.debug.maxToStringFields", debug_maxstr)
    spark.conf.set("spark.scheduler.mode", schdlr_mode)
    spark.conf.set("spark.executor.instances", exec_inst)
    spark.conf.set("spark.default.parallelism", dflt_parallel)
    spark.conf.set("spark.serializer", serializer)*/
  }
}
