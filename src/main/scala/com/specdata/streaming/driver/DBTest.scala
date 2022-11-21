/*
package com.specdata.streaming.driver

import java.util.Properties
import java.io.IOException
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import com.mmm.speckdata.streaming.dbutils.TableData
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement
import java.sql.Connection
import java.sql.DriverManager

object DBTest {
  def main(args: Array[String]) {
    var logger = Logger.getRootLogger
    logger.setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    Logger.getLogger("com").setLevel(Level.OFF);
    var properties = new Properties()
    try {
      properties.load(FileSystem.get(new Configuration()).open(new Path("/filtrete/IndoorSpeckConfig.props")))
    } catch {
      case ex: IOException => {
        System.err.println("Unable to fetch Configuration details...")
        logger.error("Unable to fetch Configuration details ...")
        //System.exit(1)
      }
    }
    val sqlDbConnStr = properties.get("dbServer") + ";" +
      "database=" + properties.get("database") + ";" +
      "user=" + properties.get("dbUser") + ";" +
      "password=" + properties.get("dbPassword")

    val jdbcUrl = ""+properties.get("dbServer")
    val jdbcUsername = ""+properties.get("dbUser")
    val jdbcPassword = ""+properties.get("dbPassword")
    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    Class.forName(driverClass)
    val connectionProperties = new Properties()
    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")
    connectionProperties.setProperty("Driver", driverClass)

    val spark = SparkSession
      .builder()
      .appName("DB Test")
      .getOrCreate()

    val ehOffsetOptions = new java.util.HashMap[String, String]()
    ehOffsetOptions.put("url", sqlDbConnStr)
    ehOffsetOptions.put("dbtable", "IndoorEventHubOffsetLog")
    val EHOffset_DF1 = spark.read.format("jdbc").options(ehOffsetOptions).load().select("ehName", "partitionId","fromSeqNo","untilSeqNo")
    EHOffset_DF1.show()
    println("---------------- Updating SQL DB---------")
    EHOffset_DF1.createOrReplaceTempView("IndoorEventHubOffsets")
    val query = "select ehName,partitionId,fromSeqNo,untilSeqNo from IndoorEventHubOffsets"
    updateSqlDB(spark, sqlDbConnStr,query , "usp_IndoorEventHubOffsetLog_Update", "IndoorEventHubOffsetTbl")
    println("------------SQL Table Updated")
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
  def getSqlConnection(sqlDatabaseConnectionString: String): Connection = {
    var conn: Connection = null
    try {
      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      conn = DriverManager.getConnection(sqlDatabaseConnectionString)
    } catch {
      case e: Exception => {
        print("Exception while Creating Connection " + e.getMessage)
        //logger.info("Exception while Creating Connection " + e.getMessage)
      }
    }
    conn
  }
}*/
