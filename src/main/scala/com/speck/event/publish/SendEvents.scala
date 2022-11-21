/*
package com.speck.event.publish

//import org.apache.spark.eventhubs._
import java.text._
import com.microsoft.azure.servicebus.ConnectionStringBuilder
//import org.apache.spark.eventhubs.ConnectionStringBuilder
import com.microsoft.azure.eventhubs.EventData
import com.microsoft.azure.eventhubs.EventHubClient
import java.util.Properties
import java.io.FileSystem
import java.io.IOException
import java.time.Instant
import util.control.Breaks._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import java.time.Instant

object SendEvents {
  def main(args: Array[String]): Unit = {
    var logger = Logger.getRootLogger
    logger.setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    Logger.getLogger("com").setLevel(Level.OFF);

    val namespaceName = "Actair-WDEventHub4-Dev"
    val eventHubName = "watchdog_eventhub4_dev"
    val sasKeyName = "RootManageSharedAccessKey"
    val sasKey = "dHJb/P/2aNm6VlVephioNzRsjk8TSPQC51knH+YgcBo="
    //val time_val = Instant.now().toString()

    val connStr = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey)
    println(connStr.toString())

    while(true){

      for(a <- 1 to 6){

        val UXT1,UXT3,UXT5 = System.currentTimeMillis
        val UXT2,UXT4,UXT6 = System.currentTimeMillis + 20
        var str1 = "{\"channels\":[\"Ultrafine\", \"PM2.5\", \"AQI\",\"temperature\",\"humidity\"],\"devices\":{\"deviceId\" : \"WD_10004\",\"data\" :[["+UXT1+",10.5,38.8, 43.6, 23.3,34],["+UXT2+",12.8,40.8, 45.0, 24.1,35]]},{\"deviceId\" : \"WD_10005\",\"data\" : [["+UXT3+",8.7,28.2,35.1,22.7,null],["+UXT4+",6.5,25.9,32.5,22.8,null]]},{\"deviceId\" : \"WD_10006\",\"data\" : [["+UXT5+",9.7,29.2,36.1,28.7,null],["+UXT6+",6.5,25.9,null,22.8,null]]}}"
        var str2 = "{\"channels\":[\"Ultrafine\", \"PM2.5\", \"AQI\",\"temperature\",\"humidity\"],\"devices\":{\"deviceId\" : \"WD_10004\",\"data\" :[["+UXT1+",10.5,38.8, 43.6, 23.3,34],["+UXT2+",12.8,40.8, 45.0, 24.1,35]]},{\"deviceId\" : \"WD_10005\",\"data\" : [["+UXT3+",8.7,28.2,35.1,22.7,null],["+UXT4+",6.5,25.9,32.5,22.8,null]]},{\"deviceId\" : \"WD_10006\",\"data\" : [["+UXT5+",9.7,29.2,36.1,28.7,null],["+UXT6+",6.5,25.9,null,22.8,null]]}}"
        var str3 = "{\"channels\":[\"Ultrafine\", \"PM2.5\", \"AQI\",\"temperature\",\"humidity\"],\"devices\":{\"deviceId\" : \"WD_10004\",\"data\" :[["+UXT1+",10.5,38.8, 43.6, 23.3,34],["+UXT2+",12.8,40.8, 45.0, 24.1,35]]},{\"deviceId\" : \"WD_10005\",\"data\" : [["+UXT3+",8.7,28.2,35.1,22.7,null],["+UXT4+",6.5,25.9,32.5,22.8,null]]},{\"deviceId\" : \"WD_10006\",\"data\" : [["+UXT5+",9.7,29.2,36.1,28.7,null],["+UXT6+",6.5,25.9,null,22.8,null]]}}"
        var str4 = "{\"channels\":[\"Ultrafine\", \"PM2.5\", \"AQI\",\"temperature\",\"humidity\"],\"devices\":{\"deviceId\" : \"WD_10004\",\"data\" :[["+UXT1+",10.5,38.8, 43.6, 23.3,34],["+UXT2+",12.8,40.8, 45.0, 24.1,35]]},{\"deviceId\" : \"WD_10005\",\"data\" : [["+UXT3+",8.7,28.2,35.1,22.7,null],["+UXT4+",6.5,25.9,32.5,22.8,null]]},{\"deviceId\" : \"WD_10006\",\"data\" : [["+UXT5+",9.7,29.2,36.1,28.7,null],["+UXT6+",6.5,25.9,null,22.8,null]]}}"
        var str5 = "{\"channels\":[\"Ultrafine\", \"PM2.5\", \"AQI\",\"temperature\",\"humidity\"],\"devices\":{\"deviceId\" : \"WD_10004\",\"data\" :[["+UXT1+",10.5,38.8, 43.6, 23.3,34],["+UXT2+",12.8,40.8, 45.0, 24.1,35]]},{\"deviceId\" : \"WD_10005\",\"data\" : [["+UXT3+",8.7,28.2,35.1,22.7,null],["+UXT4+",6.5,25.9,32.5,22.8,null]]},{\"deviceId\" : \"WD_10006\",\"data\" : [["+UXT5+",9.7,29.2,36.1,28.7,null],["+UXT6+",6.5,25.9,null,22.8,null]]}}"
        var str6 = "{\"channels\":[\"Ultrafine\", \"PM2.5\", \"AQI\",\"temperature\",\"humidity\"],\"devices\":{\"deviceId\" : \"WD_10004\",\"data\" :[["+UXT1+",10.5,38.8, 43.6, 23.3,34],["+UXT2+",12.8,40.8, 45.0, 24.1,35]]},{\"deviceId\" : \"WD_10005\",\"data\" : [["+UXT3+",8.7,28.2,35.1,22.7,null],["+UXT4+",6.5,25.9,32.5,22.8,null]]},{\"deviceId\" : \"WD_10006\",\"data\" : [["+UXT5+",9.7,29.2,36.1,28.7,null],["+UXT6+",6.5,25.9,null,22.8,null]]}}"

        val mapmessage = Map("message1" -> str1,"message2" -> str2,"message3" -> str3,"message4" -> str4,"message5" -> str5,"message6" -> str6 )
        var finalMessage = mapmessage("message"+a)
        println("message"+a)
        println(finalMessage)
        val message = finalMessage.getBytes("UTF-8") //.replace("yyyy", counter2.toString()).replace("time_val_xx", time_val).replace("1.0", counter.toString()).replace("100", (100 - counter).toString()).replace("unix_ts1",unixTS1.toString()).replace("unix_ts2",unixTS.toString()).getBytes("UTF-8")
        val sendEvent = new EventData(message)
        val ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString());
        ehClient.sendSync(sendEvent);
        println("------ Message Sent -----------")
        Thread.sleep(500)
      }
      Thread.sleep(1000)
    }
  }
}*/
