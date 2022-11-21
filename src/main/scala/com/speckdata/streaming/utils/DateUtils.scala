package com.speckdata.streaming.utils

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Date
import java.lang.Exception
import java.time.Instant
import java.time.ZoneId
import java.text.SimpleDateFormat
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
object DateUtils {


  def getLocalTS(timestamp:Long):String= {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val unixTime = timestamp//1456358553
    val localTS = Instant.ofEpochSecond(unixTime)
      .atZone(ZoneId.of("GMT-4"))
      .format(formatter);
    return localTS
  }

  def getLocalDate(timestamp:Long):String= {
    val localTS = getLocalTS(timestamp)
    var localDate = ""
    if(localTS!=null && localTS!=""){
      val tsArr = localTS.split(" ")
      localDate = tsArr(0).trim
    }
    return localDate
  }

  def getLocalHour(timestamp:Long):String= {
    val localTS = getLocalTS(timestamp)
    var localHour = ""
    if(localTS!=null && localTS!=""){
      val tsArr = localTS.split(" ")
      val localHr = tsArr(1).trim
      val indexCol = localHr.indexOf(":")
      localHour = localHr.substring(0, indexCol)
    }
    return localHour
  }

}
