/*

package com.filter.streaming.driver

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, FloatType }
import java.sql.DriverManager
import java.sql.Connection
import java.util.Properties
import java.io.IOException
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import java.io.File
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Dataset
import java.time.Instant
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.functions._
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import com.microsoft.azure.eventhubs.EventData
import org.apache.spark.eventhubs.ConnectionStringBuilder
import org.apache.spark.eventhubs.{ EventHubsConf, EventPosition, EventHubsUtils }
import org.apache.spark.eventhubs.rdd.HasOffsetRanges
import org.apache.spark.eventhubs.rdd.OffsetRange
import org.apache.spark.TaskContext
import org.apache.spark.eventhubs.NameAndPartition
import java.time.Instant

import com.filter.streaming.dbutils._
import org.apache.spark.SparkException


case class FilterSensorProcessException(private val message: String = "",
                                        private val cause: Throwable = None.orNull)
  extends Exception(message, cause)


object FilterSensorStreamingDriver {

  def main(args: Array[String]) {
    var logger = Logger.getRootLogger
    logger.setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    Logger.getLogger("com").setLevel(Level.OFF);
    logger.info("**** Starting Filter - Integrate Sensor Data Process *****")

    var prop = new Properties()
    try {
      prop.load(FileSystem.get(new Configuration()).open(new Path("/filtrete/FilterConfig.props")))
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        throw new FilterSensorProcessException("Error Reading Configuration File FilterConfig.props --------")
      }
    }

    try {
      /*
       * Reading properties from properties file filter-config.properties and intializing
       */
      val policyName = prop.getProperty("policyName_filter")
      val policykey = prop.getProperty("policyKey_filter")
      val namespace = prop.getProperty("nameSpace_filter")
      val hubName = prop.getProperty("hubName_filter")
      val sparkCheckpointDir = prop.getProperty("sparkCheckpointDir_filter")
      val partitioncnt = prop.getProperty("partitioncnt_filter").toInt
      val consumergroup = prop.getProperty("consumergroup_filter")
      val adlsPath = prop.getProperty("adlsPath")
      val adlsFileName = prop.getProperty("adlsFileName")
      val invalidJsonAdlsFileName = prop.getProperty("invalidJsonAdlsFileName")
      //val eventHubRetentionDuration = prop.getProperty("eventHubRetentionDuration").toLong
      val batchDuration = args(0).toInt
      val maxRate = args(1).toInt
      val shufflePartition = args(2).toInt
    //  val policyName = "RAVI";
      /**
        * DB connection url intiated
        */

      val sqlDbConnStr = prop.get("dbServer") + ";" +
        "database=" + prop.get("database") + ";" +
        "user=" + prop.get("dbUser") + ";" +
        "password=" + prop.get("dbPassword")

      val sparkSession = SparkSession
        .builder()
        .appName("Integrate SensorDataUpload Process")
        .config("hive.exec.dynamic.partition", true)
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("hive.exec.parallel", true)
        .config("hive.enforce.bucketing", true)
        .config("spark.sql.shuffle.partitions",shufflePartition)
        .config("spark.ui.showConsoleProgress",false)
        .enableHiveSupport()
        .getOrCreate()


      val eventHubConnectionString = ConnectionStringBuilder()
        .setNamespaceName(namespace)
        .setEventHubName(hubName)
        .setSasKeyName(policyName)
        .setSasKey(policykey)
        .build

      //Read EventHub Offsets from FilterEventHubOffsetLog Table
      val ehOffsetOptions = new java.util.HashMap[String, String]()
      ehOffsetOptions.put("url", sqlDbConnStr)
      ehOffsetOptions.put("dbtable", "FilterEventHubOffsetLog")
      val EHOffset_DF1 = sparkSession.read.format("jdbc").options(ehOffsetOptions).load().select("ehName", "partitionId","fromSeqNo","untilSeqNo","lastProcessedTS")
      EHOffset_DF1.show()

      val currentTime = Instant.now.getEpochSecond
      var positions = Map[NameAndPartition,EventPosition]()


      if(EHOffset_DF1.rdd.isEmpty()){
        println("---------FilterEventHubOffsetLog is empty. Read from Start of Stream")
        positions = Map(
          new NameAndPartition(hubName, 0) -> EventPosition.fromStartOfStream,
          new NameAndPartition(hubName, 1) -> EventPosition.fromStartOfStream
        )
      }else{
        val ehPartition0 = EHOffset_DF1.select("partitionId").collectAsList().get(0).getInt(0)
        val ehSeqNo0 = EHOffset_DF1.select("untilSeqNo").collectAsList().get(0).getLong(0)
        val ehPartition1 = EHOffset_DF1.select("partitionId").collectAsList().get(1).getInt(0)
        val ehSeqNo1 = EHOffset_DF1.select("untilSeqNo").collectAsList().get(1).getLong(0)
        val lastProcessedTS = EHOffset_DF1.select("lastProcessedTS").collectAsList().get(0).getLong(0)

        println("--------- Extracting ----------")
        println(s" $ehPartition0 : $ehSeqNo0 : $ehPartition1 : $ehSeqNo1 : $lastProcessedTS")

        positions = Map(
          new NameAndPartition(hubName, ehPartition0) -> EventPosition.fromSequenceNumber(ehSeqNo0),
          new NameAndPartition(hubName, ehPartition1) -> EventPosition.fromSequenceNumber(ehSeqNo1)
        )
      }

      val eventHubConf = EventHubsConf(eventHubConnectionString)
        .setConsumerGroup(consumergroup)
        .setMaxRatePerPartition(maxRate)
        .setStartingPositions(positions)

      val ssc = StreamingContext.getOrCreate(sparkCheckpointDir, () =>
        createNewStreamingContext(eventHubConf,sparkSession,sparkCheckpointDir, batchDuration, adlsPath, adlsFileName, invalidJsonAdlsFileName, sqlDbConnStr))

      ssc.start()
      ssc.awaitTermination()

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        throw new FilterSensorProcessException("Error Creating Streaming Context --------")
        //System.err.println("Unable to run spark streaming. Speckdata streaming stopped restart required ...")
      }
    }
  }

  def createNewStreamingContext(
                                 eventHubConf : EventHubsConf,
                                 sparkSession: SparkSession,
                                 sparkCheckpointDir: String,
                                 batchDuration: Int,
                                 adlsPath: String,
                                 adlsFileName: String,
                                 invalidJsonAdlsFileName: String,
                                 sqlDbConnStr: String): StreamingContext = {

    println("-------- Streaming Context Created")

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(batchDuration))
    //ssc.checkpoint(sparkCheckpointDir)

    val inputDirectStream = EventHubsUtils.createDirectStream(ssc, eventHubConf)
    inputDirectStream.foreachRDD{
      rdd =>
        val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        processSensorData(rdd,  sparkSession, adlsPath, adlsFileName, invalidJsonAdlsFileName, sqlDbConnStr, ranges)
    }
    ssc
  }

  /**
    * Save RDD to ADLS and update filter table with latest battery information
    */
  def processSensorData(rdd: RDD[EventData], spark: SparkSession, adlsPath: String, adlsFileName: String, invalidJsonAdlsFileName: String, sqlDbConnStr: String, ranges:Array[OffsetRange]) {

    // println(s"---------------- READING FROM FILTER EVENT HUB START ---------------------------")

    if (!rdd.isEmpty()) {
      try{
        val count = rdd.count()
        //println("------------ No of messages: "+count)
        val rddString = rdd.map(eventData => new String(eventData.getBytes))
        /*
        val rddString = rdd.map{
            eventdata =>
           new String("{\"MessageBody\":") + new String(eventdata.getBytes) + new String(",\"SeqNo\":")+ eventdata.getSystemProperties.getSequenceNumber + new String(",\"EnqueTime\":")+ eventdata.getSystemProperties.getEnqueuedTime.getEpochSecond + new String("}")
         }
        */
        val dataSet = spark.read.json(rddString);
        //dataSet.show()


        import spark.sqlContext.implicits._
        val inputSchema = dataSet.schema.toString()
        val invalidWarehousePath = getDynamicDateFolder(adlsPath) + invalidJsonAdlsFileName
        var unCurrupted_DF : DataFrame = null
        var messageStatus = "_valid_"
        if(inputSchema.contains("_corrupt_record")){
          if(inputSchema.contains("version") && inputSchema.contains("filterID") && inputSchema.contains("batteryLife") && inputSchema.contains("batteryLifeTS") && inputSchema.contains("sensorData")){
            val currupted_DF = dataSet.select($"_corrupt_record").filter($"_corrupt_record".isNotNull)
            unCurrupted_DF = dataSet.select($"version",$"filterID",$"batteryLife",$"batteryLifeTS",$"sensorData").filter($"_corrupt_record".isNull)
            //write Corrupted data to ADLS invalid data path
            if(!currupted_DF.rdd.isEmpty()){
              currupted_DF.repartition(1).write.mode("append").json(invalidWarehousePath)
            }
          }else{
            messageStatus = "_corrupt_invalid_"
            dataSet.repartition(1).write.mode("append").json(invalidWarehousePath)
          }
        }else{
          if(inputSchema.contains("version") && inputSchema.contains("filterID") && inputSchema.contains("batteryLife") && inputSchema.contains("batteryLifeTS") && inputSchema.contains("sensorData")){
            unCurrupted_DF = dataSet.select($"version",$"filterID",$"batteryLife",$"batteryLifeTS",$"sensorData")
          }else if(!inputSchema.contains("version") || !inputSchema.contains("filterID") || !inputSchema.contains("batteryLife") || !inputSchema.contains("batteryLifeTS") || !inputSchema.contains("sensorData")){
            messageStatus = "_invalid_"
            dataSet.repartition(1).write.mode("append").json(invalidWarehousePath)
          }
        }
        if(messageStatus.contains("_valid_") && !unCurrupted_DF.rdd.isEmpty()){
          val invalidData_DF = unCurrupted_DF.filter($"version".isNull || $"filterID".isNull || $"batteryLife".isNull || $"batteryLifeTS".isNull || $"sensorData".isNull)
          val validData_DF = unCurrupted_DF.filter($"version".isNotNull && $"filterID".isNotNull && $"batteryLife".isNotNull && $"batteryLifeTS".isNotNull && $"sensorData".isNotNull)

          if(!invalidData_DF.rdd.isEmpty()){
            //write Invalid data to ADLS invalid data path
            val invalidWarehousePath = getDynamicDateFolder(adlsPath) + invalidJsonAdlsFileName
            invalidData_DF.repartition(1).write.mode("append").json(invalidWarehousePath)
          }
          if(!validData_DF.rdd.isEmpty()){
            //write Valid data to ADLS valid data path
            val warehousePath = getDynamicDateFolder(adlsPath) + adlsFileName
            validData_DF.repartition(1).write.mode("append").json(warehousePath)

            //Explode Sensor Data for each FilterId
            val sensorDataDF = validData_DF.select($"filterID",$"sensorData.data".as("data_values"))
            val sensorDataExplodeDF = sensorDataDF.select($"filterID",explode($"data_values").as("data"))
            val sensorUniqueValuesDF = sensorDataExplodeDF.dropDuplicates()
            val sensorDataFinalDF = sensorUniqueValuesDF.withColumn("UnixTS",$"data".getItem(0))
              .withColumn("Temperature",$"data".getItem(1))
              .withColumn("Pressure",$"data".getItem(2))
              .select("filterID","UnixTS","Temperature","Pressure")
            val finalDataExpr = sensorDataFinalDF.selectExpr("filterID","cast(UnixTS as BIGINT) UnixTS","cast(Temperature as Double) Temperature","cast(Pressure as Double) Pressure","from_unixtime(cast(UnixTS as BIGINT)) sensorDataTS","TO_DATE(from_unixtime(cast(UnixTS as BIGINT))) sensordatadt","FROM_UNIXTIME(UNIX_TIMESTAMP()) createdTS", "FROM_UNIXTIME(UNIX_TIMESTAMP()) modifiedTS")
            finalDataExpr.createOrReplaceTempView("SensorDataView")

            //Write Exploded sensor data to ADLS
            spark.sql("INSERT INTO TABLE filtersensordata PARTITION(sensordatadt) SELECT filterID, sensorDataTS, Temperature, Pressure, createdTS, modifiedTS, sensordatadt from SensorDataView")
            spark.sql("INSERT INTO TABLE filtersensordatanew PARTITION(createddt) SELECT filterID, sensorDataTS, Temperature, Pressure, createdTS, modifiedTS, TO_DATE(createdTS) createddt from SensorDataView")

            //Update batterylife and batterylifeTS in Filter table
            updateFilterTable(spark, validData_DF, sqlDbConnStr)
          }
        }

        println(s"FILTER MESSAGES $count Processed At: "+ Instant.now.getEpochSecond)
        //Update Offset values to FilterEventHubOffsetLog in SQL
        updateFilterOffsetLog(spark,sqlDbConnStr,ranges)

      }catch{
        case ex: Exception => {
          ex.printStackTrace()
          throw new FilterSensorProcessException("Error Processing Filter Sensor Data -------")
        }
      }
    }
  }

  /*
   * Updating the filter values
   *
   */
  def updateFilterTable(spark: SparkSession, dataFrame: DataFrame, sqlDbConnStr: String): String = {
    var status = "success"
    // if (filter != null) {
    dataFrame.createOrReplaceTempView("3M_FILTER_TABLE")
    /*var sparkSession: SparkSession = spark1;
    sparkSession = SparkSession.builder().getOrCreate();*/
    val filterDF = spark.sql(FilterConstants.SEL_FILTER_INFO)
    val filterSQLRecords = filterDF.collect()
    val filter_schema = filterDF.schema
    if (filterDF.count() > 0) {
      val updateSqlTable = new TableData(filter_schema, filterSQLRecords)
      executeProc(sqlDbConnStr, "usp_update_FilterInfoProc", updateSqlTable, "FilterInfoUpdateTypeTable")
    }
    status
  }

  def executeProc(sqlDbConnStr: String,procName: String, inputData: TableData, tableType: String) = {
    var sqlConn: Connection = getSqlConnection(sqlDbConnStr)
    val pStmt: SQLServerPreparedStatement = sqlConn.prepareStatement(s"EXECUTE $procName ?").asInstanceOf[SQLServerPreparedStatement]
    pStmt.setStructured(1, tableType, inputData)
    pStmt.execute()
    sqlConn.close()
  }

  def getSqlConnection(sqlDatabaseConnectionString: String): Connection = {
    var conn: Connection = null
    try {
      Class.forName(FilterConstants.DB_SERVER_NAME) //"com.microsoft.sqlserver.jdbc.SQLServerDriver"
      conn = DriverManager.getConnection(sqlDatabaseConnectionString)
    } catch {
      case e: Exception => {
        print("Exception while creating Connection " + e.getMessage)
        //logger.info("Exception while Creating Connection " + e.getMessage)
      }
    }
    conn
  }
  def getDynamicDateFolder(adlsPath: String):String={
    var YYYY = ""
    var folder = ""
    var  dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    try {
      var now:LocalDateTime = LocalDateTime.now()
      var datetime = dtf.format(now)
      YYYY = Integer.toString(now.getYear())
      folder = datetime.replaceAll("-", "")
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    return adlsPath+ YYYY + "/" + folder + "/";
  }
  def updateSqlDB(spark: SparkSession, sqlConnStr: String, query: String, dbProcName: String, tableType : String) = {
    val localDF = spark.sql(query)
    val records = localDF.collect()
    val schema = localDF.schema
    val length = records.length
    val sqlConn = getSqlConnection(sqlConnStr)
    if(length > 0){
      val tableData = new TableData(localDF.schema,records)
      val pStmt: SQLServerPreparedStatement = sqlConn.prepareStatement(s"EXECUTE $dbProcName ?").asInstanceOf[SQLServerPreparedStatement]
      pStmt.setStructured(1,tableType , tableData)
      pStmt.execute()
    }
    sqlConn.close()
  }
  def getFinalJSON(inputJSON: String):String={
    var finalJSONStr: String = null
    if(inputJSON != null){
      val start: Int = inputJSON.lastIndexOf("[")
      val end: Int = inputJSON.lastIndexOf("]")-1
      var output: String = inputJSON.substring(start, end)
      finalJSONStr = output + "]"
    }
    return finalJSONStr
  }
  def updateFilterOffsetLog(spark: SparkSession, sqlDbConnStr: String, ranges:Array[OffsetRange]){
    var offsets :String =  "["
    ranges.foreach { range =>
      val offsetDetails = "{\"ehName\":\""+range.name+"\",\"partitionId\":"+range.partitionId+", \"fromSeqNo\":"+range.fromSeqNo+",\"untilSeqNo\":"+range.untilSeqNo+"}"
      offsets += offsetDetails + ","
    }
    offsets += "]"
    val finalOffsets = getFinalJSON(offsets)
    val offsetData = spark.sparkContext.parallelize(finalOffsets::Nil)
    val offsetDataDF = spark.read.format("json").json(offsetData)
    offsetDataDF.createOrReplaceTempView("FilterEventHubOffsets")
    val query = "select ehName,partitionId,fromSeqNo,untilSeqNo, UNIX_TIMESTAMP() as lastProcessedTS from FilterEventHubOffsets"
    updateSqlDB(spark, sqlDbConnStr,query , "usp_FilterEventHubOffsetLog_Upsert", "FilterEventHubOffsetTbl")
    //println("FilterEventHubOffsetLog table updated----------")
  }
}
*/
