/*
package com.specdata.streaming.driver

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
import org.apache.spark.streaming.Duration
import scala.concurrent.duration._
import org.apache.spark.util.Utils


import com.speckdata.streaming.dbutils._
import java.io.File
import com.speckdata.streaming.utils._
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
import org.apache.spark.eventhubs._
import org.apache.spark.eventhubs.ConnectionStringBuilder
import org.apache.spark.eventhubs.{ EventHubsConf, EventPosition, EventHubsUtils }
import org.apache.spark.eventhubs.rdd.HasOffsetRanges
import org.apache.spark.eventhubs.rdd.OffsetRange
import org.apache.spark.TaskContext
import org.apache.spark.eventhubs.NameAndPartition

import org.apache.spark.eventhubs.client.EventHubsClient
import com.microsoft.azure.eventhubs.{ EventHubClient, PartitionReceiver }
import org.apache.spark.eventhubs.SequenceNumber
import org.apache.spark.eventhubs.rdd.{ EventHubsRDD, OffsetRange }
import com.microsoft.azure.eventhubs.impl.EventHubClientImpl
import org.apache.spark.streaming.eventhubs.EventHubsDirectDStream
import org.apache.spark.sql.eventhubs.EventHubsSourceProvider

case class IndoorSpeckDataProcessException(private val message: String = "",
                                           private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

object IndoorSpeckStreamingDriver {

  def main(args: Array[String]) {
    var logger = Logger.getRootLogger
    logger.setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    Logger.getLogger("com").setLevel(Level.OFF);
    logger.info("**** Starting Indoor Integrate Speck Data Process *****")

    val spark = SparkSession
      .builder
      .getOrCreate

    import org.apache.spark._

    /** print(s"---printing Json Data---")



    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val lines = sc.textFile("adl://actairdlakecentralus.azuredatalakestore.net/Telemetry/wdtelemetry_0_17860ef03fba48888225a976bf9f6208.json")
    val readDf = spark.read.json(lines)
    readDf.printSchema()
    readDf.show(10)
      *
      */

    var properties = new Properties()
    try {
      properties.load(FileSystem.get(new Configuration()).open(new Path("/IotHub/IndoorSpeckConfig.props")))
    } catch {
      case ex: IOException => {
        System.err.println("Unable to fetch Configuration details...")
        logger.error("Unable to fetch Configuration details ...")
      }
    }

    try {
      /*
       * Reading properties from properties file filter-config.properties and intializing
       */
      //val progressDir = properties.getProperty("progressDir_indoor")
      val policyName = properties.getProperty("policyName_outdoor")
      val policykey = properties.getProperty("policyKey_outdoor")
      val hostName = properties.getProperty("Hostname_outdoor")
      val namespace = properties.getProperty("nameSpace_outdoor")
      val hubName = properties.getProperty("hubName_outdoor")
      val sparkCheckpointDir = properties.getProperty("sparkCheckpointDir_outdoor")
      val partitioncnt = properties.getProperty("partitioncnt_outdoor")
      val consumergroup = properties.getProperty("consumergroup_outdoor")
      val adlsPath = properties.getProperty("adlsPath_outdoor")
      val validSpeckFileName = properties.getProperty("validDataFileName_outdoor")
      val invalidSpeckFileName = properties.getProperty("invalidDataFileName_outdoor")
      val batchDuration = args(0).toInt
      val maxRate = args(1).toInt
      val shufflePartition = args(2).toInt
      println("------ Completed reading properties file")


      println(policyName)
      println(policykey)
      println(hostName)
      println(namespace)
      println(hubName)
      println(partitioncnt)
      println(consumergroup)



      /**
        * DB connection url intiated
        */

      val sqlDbConnStr = properties.get("dbServer") + ";" +
        "database=" + properties.get("database") + ";" +
        "user=" + properties.get("dbUser") + ";" +
        "password=" + properties.get("dbPassword")

      val spark = SparkSession
        .builder()
        .appName("Indoor Speck Data Process")
        .config("hive.exec.dynamic.partition", true)
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("hive.exec.parallel", true)
        .config("hive.enforce.bucketing", true)
        .config("spark.sql.shuffle.partitions",shufflePartition)
        .config("spark.ui.showConsoleProgress",false)
        .enableHiveSupport()
        .getOrCreate()
      println("------ Spark Session Creation")



      //Read DSTCalendar table from SQL Database
      val DSTOptions = new java.util.HashMap[String, String]()
      DSTOptions.put("url", sqlDbConnStr)
      DSTOptions.put("dbtable", "DSTCalendar")
      val DST_DF = spark.read.format("jdbc").options(DSTOptions).load().select("year", "dst_start_unixTS","dst_end_unixTS").persist()
      DST_DF.createOrReplaceTempView("DSTCalendar")

      val eventHubConnectionString = ConnectionStringBuilder()
        .setNamespaceName(namespace)
        .setEventHubName(hubName)
        .setSasKeyName(policyName)
        .setSasKey(policykey)
        .build

      //Read EventHub Offsets from IndoorEventHubOffsetLog Table
      val ehOffsetOptions = new java.util.HashMap[String, String]()
      ehOffsetOptions.put("url", sqlDbConnStr)
      ehOffsetOptions.put("dbtable", "WatchdogIotHubOffsetLog")
      val EHOffset_DF1 = spark.read.format("jdbc").options(ehOffsetOptions).load().select("ehName", "partitionId","fromSeqNo","untilSeqNo")
      EHOffset_DF1.show()

      val ehPartition0 = EHOffset_DF1.select("partitionId").collectAsList().get(0).getInt(0)
      val ehSeqNo0 = EHOffset_DF1.select("untilSeqNo").collectAsList().get(0).getLong(0)
      val ehPartition1 = EHOffset_DF1.select("partitionId").collectAsList().get(1).getInt(0)
      val ehSeqNo1 = EHOffset_DF1.select("untilSeqNo").collectAsList().get(1).getLong(0)

      println("--------- Extracting ----------")
      println(s" $ehPartition0 : $ehSeqNo0 : $ehPartition1 : $ehSeqNo1")

      val positions = Map(
        new NameAndPartition(hubName, ehPartition0) -> EventPosition.fromSequenceNumber(ehSeqNo0),
        new NameAndPartition(hubName, ehPartition1) -> EventPosition.fromSequenceNumber(ehSeqNo1)
      )
      val eventHubConf = EventHubsConf(eventHubConnectionString)
        .setConsumerGroup(consumergroup)
        .setMaxRatePerPartition(maxRate)
        .setStartingPositions(positions)
        .setStartingPosition(EventPosition.fromStartOfStream)

      println("-------eventhub conf created")

      println("----------s-printing streaming df")
      val df = spark
        .readStream
        .format("org.apache.spark.sql.eventhubs.EventHubsSourceProvider")
        .options(eventHubConf.toMap)
        .load()
      val query1 = df.writeStream
        .outputMode("append")
        .format("console")
        .start()

      df.printSchema()

      df.show(10)
      query1.awaitTermination()
      spark.stop()
      /*
               val ssc = StreamingContext.getOrCreate(sparkCheckpointDir, () =>
                  createNewStreamingContext(eventHubConf,spark, sparkCheckpointDir,  batchDuration, adlsPath, validSpeckFileName, invalidSpeckFileName, sqlDbConnStr))

                  println("-----------about to start streaming")
                ssc.start()
                ssc.awaitTermination()
            */
    } catch {
      case ex: IOException => {
        System.err.println("Unable to run spark streaming. Speckdata streaming stopped restart if you want ...")
      }
    }
  }

  def createNewStreamingContext(
                                 eventHubConf : EventHubsConf,
                                 sparkSession : SparkSession,
                                 sparkCheckpointDir: String,
                                 batchDuration: Int,
                                 adlsPath: String,
                                 adlsFileName: String,
                                 invalidJsonAdlsFileName: String,
                                 sqlDbConnStr: String): StreamingContext = {

    println("-------- Streaming Context Creation")

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(batchDuration))
    //ssc.checkpoint(sparkCheckpointDir)
    println("-------- Streaming Context Created")

    val inputDirectStream = EventHubsUtils.createDirectStream(ssc, eventHubConf)
    println("Dstream created")
    println("--------printing RDD-------------")


    println(inputDirectStream.print())

    println("----------done-------------")


    inputDirectStream.foreachRDD { rdd =>
      rdd.flatMap(eventData => new String(eventData.getBytes).split(" ")).collect().toList.
        foreach(println)


    }
    ssc
  }


  /**
    * Save RDD to ADLS and update filter table with latest battery information
    */

  def processSensorData(rdd: RDD[EventData], spark: SparkSession, adlsPath: String, adlsFileName: String, invalidJsonAdlsFileName: String, sqlDbConnStr: String, ranges:Array[OffsetRange]) {

    println(s"---------------- READING FROM IOT HUB START ---------------------------")

    if (!rdd.isEmpty()) {

      try{

        //Read IndoorAirQualityDevice table from SQL Database
        val indoorDeviceOptions = new java.util.HashMap[String, String]()
        indoorDeviceOptions.put("url", sqlDbConnStr)
        indoorDeviceOptions.put("dbtable", "IndoorAirQualityDevice")
        val IAQD_DF = spark.read.format("jdbc").options(indoorDeviceOptions).load()
        val IAQD_ACTIVE_DF = IAQD_DF.select("indoorDeviceID","accountID","indoorDeviceToken","gmtOffset","dstOffset","activeFlag")//.filter(SpeckDataConstants.IAQD_ACTIVETBLE_COND)
        IAQD_ACTIVE_DF.createOrReplaceTempView("IndoorAirQualityDevice")
        print(s"===> printing IndoorAirQualityDevice table data <===")
        val IAQD_ACTIVE_DF12 = spark.sql("select * from IndoorAirQualityDevice limit 10")
        IAQD_ACTIVE_DF12.show()

        println(s"==============1")
        //Read input messages from EventData rdd
        val rddString = rdd.map(eventData => new String(eventData.getBytes))
        val count = rdd.count()
        val collect1 = rdd.take(10)
        collect1.foreach(println)
        //println(s"NO OF MESSAGES Received: $count At: "+ Instant.now.getEpochSecond)

        val InputDF = spark.read.json(rddString);
        val parseValidator = InputDF.schema.toString()
        InputDF.createOrReplaceTempView("TelemetryDataTable")
        val TelemetryTable = spark.sql("select * from TelemetryDataTable limit 10")
        TelemetryTable.show()
        println(s"==============2")

        if (!parseValidator.contains("_corrupt_record")){

          //Writing Input Messages to ADLS
          val validPath = getDynamicDateFolder(adlsPath) + adlsFileName
          InputDF.repartition(1).write.mode("append").json(validPath)
          //Flatten Input messages
          val inputDevicesFlattenDF = getFlattenDF(InputDF,spark)
          inputDevicesFlattenDF.createOrReplaceTempView("IndoorInputDevices")
          println("==============>Printing IndoorInputDevices table data<==================")
          val IndoorDevice = spark.sql("select * from IndoorInputDevices limit 10")
          IndoorDevice.show()



          val indoorDevicesDF = spark.sql(SpeckDataConstants.INDOOR_DF_QUERY)
          indoorDevicesDF.createOrReplaceTempView("IndoorDevices")

          // Convert to Local Time and update SQL table
          val localDevices_DF = spark.sql(SpeckDataConstants.LOCAL_DEVICES_QUERY)
          localDevices_DF.createOrReplaceTempView("IndoorDevicesLocalTS")
          val indoorUpdateDevicesDF = spark.sql(SpeckDataConstants.UPDATE_DEVICES_QUERY)
          indoorUpdateDevicesDF.createOrReplaceTempView("IndoorDevicesUpdateTable")

          //Update latest values to IndoorAirQualityDevice table
          updateIndoorSqlTable(spark,indoorUpdateDevicesDF,sqlDbConnStr)

          //Insert Input Indoor flatten data into HIVE Temp table
          spark.sql("INSERT INTO TABLE IndoorSpeckDeviceDataTmp PARTITION(CreatedUTS) SELECT indoorDeviceId, unixTS, speckDataHr, particleConcentration, temperature, humidity, CreatedUTS from IndoorDevices where activeFlag = 1")
          spark.sql("INSERT INTO TABLE IndoorInvalidSpeckDeviceData PARTITION(speckDataDt) SELECT indoorDeviceToken, unixTS, particleConcentration, temperature, humidity, createdTS, modifiedTS, utcDate from IndoorDevices where activeFlag = 0")


        }else{

          //Write invalid messages to Invalid folder in ADLS
          val invalidPath =  getDynamicDateFolder(adlsPath) + invalidJsonAdlsFileName
          InputDF.repartition(1).write.mode("append").json(invalidPath)
          println("-------- Received Invalid Indoor Input message At: "+ Instant.now.getEpochSecond)
        }

        println(s"NO OF MESSAGES Processed: $count At: "+ Instant.now.getEpochSecond)

      } catch {
        case ex: Exception => {
          ex.printStackTrace()
          throw new IndoorSpeckDataProcessException("Error Processing Indoor Data")
        }
      }
      //Update Offset values to IndoorEventHubOffsetLog in SQL
      updateIndoorOffsetLog(spark,sqlDbConnStr,ranges)
    }
  }

  def getSqlConnection(sqlDatabaseConnectionString: String): Connection = {
    var conn: Connection = null
    try {
      Class.forName(SpeckDataConstants.DB_SERVER_NAME)
      conn = DriverManager.getConnection(sqlDatabaseConnectionString)
    } catch {
      case e: Exception => {
        print("Exception while Creating Connection " + e.getMessage)
      }
    }
    conn
  }
  def updateIndoorSqlTable(spark: SparkSession, dataFrame: DataFrame, sqlDbConnStr: String) = {
    val indoorDF = spark.sql("SELECT indoorDeviceID, accountID, indoorDeviceToken, particleConcentration, temperature, humidity, localTS, createdTS, modifiedTS FROM IndoorDevicesUpdateTable")
    val noRecords = indoorDF.collect()
    val indoor_schema = indoorDF.schema
    val updateSqlTable = new TableData(indoor_schema, noRecords)
    executeProc(sqlDbConnStr, "usp_Indoor_Device_LastUploadTS", updateSqlTable, "IndoorLastUploadTSUpdate")
  }
  def executeProc(sqlDbConnStr: String,procName: String, inputData: TableData, tableType: String) = {
    var sqlConn: Connection = getSqlConnection(sqlDbConnStr)
    val pStmt: SQLServerPreparedStatement = sqlConn.prepareStatement(s"EXECUTE $procName ?").asInstanceOf[SQLServerPreparedStatement]
    pStmt.setStructured(1, tableType, inputData)
    pStmt.execute()
    sqlConn.close()
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

  def updateDFSqlDB(spark: SparkSession, sqlConnStr: String, localDF: DataFrame, dbProcName: String, tableType : String) = {
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
        System.err.println("Please check dynamic folder creation code for resolution.");
      }
    }
    return adlsPath + YYYY + "/" + folder + "/" ;
  }
  def getFlattenDF(inputDF: DataFrame,sparkSession: SparkSession): DataFrame = {
    import sparkSession.sqlContext.implicits._

    val deviceData = inputDF.select($"devices")
    val devicesExplode = deviceData.withColumn("devices", explode(deviceData("devices")))
    val deviceIdData = devicesExplode.select($"devices.deviceId".as("indoorDeviceToken"),$"devices.data".as("data_values"))
    val deviceDataValues = deviceIdData.select($"indoorDeviceToken",explode($"data_values").as("data"))
    val finalData = deviceDataValues.withColumn("unixTS",$"data".getItem(0))
      .withColumn("particleConcentration",$"data".getItem(1))
      .withColumn("temperature",$"data".getItem(2))
      .withColumn("humidity",$"data".getItem(3))
      .select("indoorDeviceToken","unixTS","particleConcentration","temperature","humidity")
    val finalUniqueData = finalData.dropDuplicates()
    val flattenDF = finalUniqueData.selectExpr("indoorDeviceToken","cast(unixTS as BIGINT) unixTS","cast(particleConcentration as Double) particleConcentration","cast(temperature as Double) temperature","cast(humidity as Double) humidity")

    return flattenDF
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
  def updateIndoorOffsetLog(spark: SparkSession, sqlDbConnStr: String, ranges:Array[OffsetRange]){
    var offsets :String =  "["
    ranges.foreach { range =>
      val offsetDetails = "{\"ehName\":\""+range.name+"\",\"partitionId\":"+range.partitionId+", \"fromSeqNo\":"+range.fromSeqNo+",\"untilSeqNo\":"+range.untilSeqNo+"}"
      offsets += offsetDetails + ","
    }
    offsets += "]"
    val finalOffsets = getFinalJSON(offsets)
    val offsetData = spark.sparkContext.parallelize(finalOffsets::Nil)
    val offsetDataDF = spark.read.format("json").json(offsetData)
    offsetDataDF.createOrReplaceTempView("IndoorEventHubOffsets")
    val query = "select ehName,partitionId,fromSeqNo,untilSeqNo from IndoorEventHubOffsets"
    updateSqlDB(spark, sqlDbConnStr,query , "usp_IndoorEventHubOffsetLog_Upsert", "IndoorEventHubOffsetTbl")
    //println("IndoorEventHubOffsetLog table updated----------")
  }
}
*/
