/*
package com.venat.assignment

import java.util.Properties

import org.apache.spark.sql.functions.get_json_object
import org.apache.spark.sql.functions.udf
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver
import javax.jms.Connection
import javax.jms.MessageConsumer
import javax.jms.Queue
import javax.jms.QueueConnection
import javax.jms.QueueConnectionFactory
import javax.jms.QueueSession
import javax.jms.TextMessage
import javax.naming.Context
import javax.naming.InitialContext

import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.util.Try
import java.util.Date
import java.io.FileInputStream
import java.sql.Connection
import java.util.Calendar

import com.google.gson.Gson
import org.json.JSONArray
import org.apache.commons.lang3.math.NumberUtils
import org.json.JSONObject
object edw_mc_casing_drg_grouper {
  def main(args: Array[String]) {

    if (args.length < 4){
      System.err.println(
        "Job CanadaGcdf requires <Source DB > < Target DB > <Batch Run Date> <config file path>  ")
      System.exit(1)
    }

    /*
    * var properties = new Properties()
      try {
        properties.load(FileSystem.get(new Configuration()).open(new Path("/filtrete/WatchDogStream.props")))
      } catch {
        case ex: IOException => {
          System.err.println("Unable to fetch Configuration details...")
          logger.error("Unable to fetch Configuration details ...")
          //System.exit(1)
        }
      }*/
    val Seq( src_db, tgt_db,run_date, env ) = args.toSeq
    val progprops: Properties = new Properties
    val envConf= new FileInputStream(env);
    progprops.load(envConf)

    val Seq(master, batchInterval, wso2propsfile) = args.toSeq

    val exec_mem = progprops.getProperty("spark.executor.memory")
    val exec_cores = progprops.getProperty("spark.executor.cores")
    val driv_mem = progprops.getProperty("spark.executor.memory")
    val driv_cores = progprops.getProperty("spark.driver.cores")
    val serializer = progprops.getProperty("spark.serializer")
    val exec_inst =  progprops.getProperty("spark.executor.instances")
    val dflt_parallel =  progprops.getProperty("spark.default.parallelism")
    val cbo_enbl =  progprops.getProperty("spark.sql.cbo.enabled")
    val cbo_joinr =  progprops.getProperty("spark.sql.cbo.joinReorder.enabled")
    val shuf_prt =  progprops.getProperty("spark.sql.shuffle.partitions")
    val debug_maxstr =  progprops.getProperty("spark.debug.maxToStringFields")
    val schdlr_mode =  progprops.getProperty("spark.scheduler.mode")
    val spark = SparkSession.builder().appName("GcdfCanadaBatchA").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.executor.memory",exec_mem)
    spark.conf.set("spark.executor.cores",exec_cores)
    spark.conf.set("spark.driver.memory",driv_mem)
    spark.conf.set("spark.driver.cores",driv_cores)
    spark.conf.set("spark.sql.cbo.enabled",cbo_enbl)
    spark.conf.set("spark.sql.cbo.joinReorder.enabled",cbo_joinr)
    spark.conf.set("spark.sql.shuffle.partitions",shuf_prt)
    spark.conf.set("spark.debug.maxToStringFields",debug_maxstr)
    spark.conf.set("spark.scheduler.mode",schdlr_mode)
    spark.conf.set("spark.executor.instances",exec_inst)
    spark.conf.set("spark.default.parallelism",dflt_parallel)
    spark.conf.set("spark.serializer",serializer)

    val sqlDbConnStr = progprops.get("dbServer") + ";" +
      "database=" + progprops.get("database") + ";" +
      "user=" + progprops.get("dbUser") + ";" +
      "password=" + progprops.get("dbPassword")
    val sqlConn: Connection = getSqlJdbcConnection(sqlDbConnStr)

    val IndoorJobControlOptions = new java.util.HashMap[String, String]()
    IndoorJobControlOptions.put("url", sqlDbConnStr)
    IndoorJobControlOptions.put("dbtable", "IndoorJobControl")

    val CASE_ISTL_CLM_LN = spark.read.format("jdbc")
      .options(IndoorJobControlOptions).load()
      .select("status","processstartUTS","processstartdt","processenddt","processendUTS").filter("status='Processed'")
    CASE_ISTL_CLM_LN.createOrReplaceTempView("IndoorJobControl")
    // val hbaseJobConf:JobConf = getHbaseConfig(); /
    //sqlContext.udf.register("getPharmacyClaimVersionJson", getJSONString _)
    val getRowRankNumberUDF = udf( getRowRankNumber _)
    val getClientCodeUDF = udf( getClientCode _)
    val getMemberDetailsUDF = udf(getMemberDetails _)
    val getPharmacyBenefitPlanDetailsUDF = udf( getPharmacyBenefitPlanDetails _ )

    def getPharmacyId(provCtxCode:String,pharmacyId:String)= if( "NCPDP" == provCtxCode ) pharmacyId  else "0"

    val getPharmacyIdUDF = udf( getPharmacyId _ )
    def getNDC(pharmaProdCode:String,NDC:String) = if( "NDC".equalsIgnoreCase(pharmaProdCode)) NDC else "0"
    val getNDCUDF = udf( getNDC _ )
    val getRelatedClaimDetailsUDF = udf( getRelatedClaimDetails _)
    val calendar = Calendar.getInstance()

    import sqlContext.implicits._


  }

  def getSqlJdbcConnection(sqlDatabaseConnectionString : String): Connection ={
    var con:Connection = null
    try{
      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      con = DriverManager.getConnection(sqlDatabaseConnectionString)
    }catch{
      case e:Exception => { print("Exception while Creating Connection " + e.getMessage)}
    }
    con
  }

  def getRowRankNumber(statusCode:String,rxClaimSequenceNumber:String)={
    if( Array("P","C").indexOf(statusCode) != -1 ){
      ((1000 - NumberUtils.toDouble(rxClaimSequenceNumber)) * 10 ) + 1
    }else if(Array("X","Z").indexOf(statusCode) != -1 ){
      ((1000 - NumberUtils.toDouble(rxClaimSequenceNumber)) * 10 ) + 2
    }else{
      0
    }
  }

  private def getString(jsonObj:JSONObject,key:String):String={
    if(jsonObj.has(key))
      return jsonObj.get(key).toString;
    else
      return null
  }

  def getClientCode(client:String)={
    var clientCode:String = null;
    if( clientCode != null && clientCode.trim().length() == 0){
      val jsonObjArr = new JSONArray(client);
      for( i <- 0 until jsonObjArr.length() ){
        val client = jsonObjArr.getJSONObject(i);
        val contextCode = getString(client,"contextCode")
        if( contextCode == "CAMMCLNT"){
          clientCode = getString(client,"componentiD1")
        }
      }
    }

    clientCode
  }

  def getRelatedClaimDetails(relatedClaim:String)={
    var relatedClaimId:String = null
    var relatedClaimReasonCode:String = null;
    if( relatedClaim != null && relatedClaim.length() > 0 ){
      val jsonObjArr = new JSONArray(relatedClaim);
      val codes = Array("SYNCH","HIM","SVC")
      for( i <- 0 until jsonObjArr.length() ){
        val obj = jsonObjArr.getJSONObject(i)
        val relatedClaimReasonCodeTmp = getString(obj,"relatedClaimReasonCode")
        if( codes.indexOf(relatedClaimReasonCodeTmp) != -1){
          relatedClaimId = getString(obj,"identifierComponentId1")
          relatedClaimReasonCode = relatedClaimReasonCodeTmp
        }
      }
    }

    (relatedClaimId,relatedClaimReasonCode)
  }

  def getMemberDetails(member:String)={
    // val jsonObjArr = new JSONArray(member);
    var carrierId = "0"
    var accountId = "0"
    var groupId   = "0"
    var memberId = "0"
    var enterprisePersonId:String = null
    var enterpriseMemberId:String = null
    if( member != null && member.length() > 0 ){
      val jsonObjArr = new JSONArray(member);
      for( i <- 0 until jsonObjArr.length() ){
        val obj = jsonObjArr.getJSONObject(i)
        val contextCode = getString(obj,"contextCode")
        if(contextCode.equalsIgnoreCase("CAGM")){
          carrierId = getString(obj,"component1")
          accountId = getString(obj,"component2")
          groupId   = getString(obj,"component3")
          memberId  = getString(obj,"component4")
        }else if( contextCode.equalsIgnoreCase("ENT_PRSN")){
          enterprisePersonId = getString(obj,"component1")
        }else if( contextCode.equalsIgnoreCase("ENT_MBR")){
          enterpriseMemberId = getString(obj,"component1")
        }
      }
    }

    (carrierId,accountId,groupId,memberId,enterpriseMemberId,enterprisePersonId)
  }

  def getPharmacyBenefitPlanDetails(pharmacyBenefitPlan:String)={

    var memberPlanCode:String = null;
    var groupPlanCode:String = null;
    var finalPlanCode:String = null;
    if( pharmacyBenefitPlan != null  && pharmacyBenefitPlan.length() > 0){
      val jsonObjArr = new JSONArray(pharmacyBenefitPlan);
      for( i <- 0 until jsonObjArr.length() ){
        val obj = jsonObjArr.getJSONObject(i)
        val planCategoryCode = getString(obj,"planCategoryCode")
        if( planCategoryCode == "MBR"){
          memberPlanCode = getString(obj,"pharmacyBenefitPlanCode")
        }else if( planCategoryCode == "GRP"){
          groupPlanCode = getString(obj,"pharmacyBenefitPlanCode")
        }else if( planCategoryCode == "FINAL"){
          finalPlanCode = getString(obj,"pharmacyBenefitPlanCode")
        }
      }
    }

    (memberPlanCode,groupPlanCode,finalPlanCode)
  }


}
*/
