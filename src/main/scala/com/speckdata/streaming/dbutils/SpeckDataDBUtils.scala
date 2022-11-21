/*
package com.speckdata.streaming.dbutils

import java.sql.DriverManager
import java.sql.Connection
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import com.microsoft.sqlserver.jdbc._
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, FloatType }
import java.io.Serializable
import com.microsoft.sqlserver.jdbc._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import com.mmm.speckdata.streaming.dbutils._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.util.Properties
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException
import org.apache.log4j.Level
import org.apache.log4j.Logger

object SpeckDataDBUtils {

  def getSqlConnection(sqlDatabaseConnectionString: String, logger:Logger): Connection = {
    var conn: Connection = null
    try {
      Class.forName(SpeckDataConstants.DB_SERVER_NAME)//"com.microsoft.sqlserver.jdbc.SQLServerDriver"
      conn = DriverManager.getConnection(sqlDatabaseConnectionString)
    } catch {
      case e: Exception => {
        print("Exception while Creating Connection " + e.getMessage)
        logger.info("Exception while Creating Connection " + e.getMessage)
      }
    }
    conn
  }

  def getDynamicDateFolder():String={
    var properties = new Properties()
    Logger.getRootLogger.setLevel(Level.ERROR)
    var logger = Logger.getLogger(this.getClass())
    try {
      properties.load(FileSystem.get(new Configuration()).open(new Path("IndoorConfig.props")))
    } catch {
      case ex: IOException => {
        System.err.println("Unable to fetch Configuration details...  Please check confoguration file location and settings")
        logger.error("Unable to fetch Configuration details ... Please check confoguration file location and settings ")
        //System.exit(1)
      }
    }
    val adlsPath = properties.getProperty("adlsPath_indoor")
    val validSpeckFileName = properties.getProperty("validDataFileName_indoor")
    val invalidSpeckFileName = properties.getProperty("invalidDataFileName_indoor")
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
    return adlsPath + YYYY + "/" + folder + "/" + validSpeckFileName;
  }

}*/
