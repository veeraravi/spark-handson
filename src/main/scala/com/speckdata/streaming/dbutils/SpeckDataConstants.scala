package com.speckdata.streaming.dbutils

object SpeckDataConstants {

  val DB_SERVER_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

  val HBASE_FORMAT_PARAM = "org.apache.spark.sql.execution.datasources.hbase"

  val IAQD_QUERY = "SELECT indoorDeviceID,accountID,indoorDeviceToken,gmtOffset,dstOffset FROM IndoorAirQualityDevice WHERE activeFlag='True'"

  val IAQD_ACTIVETBLE_COND = "activeFlag=1"

  val SPECK_TABLE_NAME = "IndoorAirQualityDevice"

  val HBASE_DF_QUERY = "select cast(CONCAT(indoorDeviceToken,'_',cast(unixTS as string)) as string) as indoorDeviceKey, cast(indoorDeviceToken as string) as indoorDeviceToken, cast(unixTS as string) as unixTS, cast(NVL(particleConcentration,0) as string) as particleConcentration, cast(NVL(temperature,0) as string) as temperature, cast(NVL(humidity,0) as string) as humidity, cast(DATE_FORMAT(TO_DATE(FROM_UNIXTIME(unixTS)),'yyyy') as string) as utcYear, FROM_UNIXTIME(UNIX_TIMESTAMP()) as createdTS, FROM_UNIXTIME(UNIX_TIMESTAMP()) as modifiedTS from IndoorInputDevices"

  val INDOOR_DF_QUERY = "select cast(A.indoorDeviceToken as string) as indoorDeviceToken, B.indoorDeviceID as indoorDeviceId, B.activeFlag as activeFlag, cast(A.unixTS as string) as unixTS, "+
    "cast(A.particleConcentration as string) as particleConcentration, cast(A.temperature as string) as temperature, cast(A.humidity as string) as humidity, cast(FROM_UNIXTIME(A.unixTS,'yyyyMMddHH') as string) as speckDataHr, cast(FROM_UNIXTIME(A.unixTS,'yyyyMMdd') as string) as utcDate, cast(FROM_UNIXTIME(A.unixTS,'yyyy') as string) as utcYear, "+
    "cast(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyyMMdd') as string) as CreatedDt, cast(UNIX_TIMESTAMP() as string) as CreatedUTS, "+
    "FROM_UNIXTIME(UNIX_TIMESTAMP()) as createdTS, FROM_UNIXTIME(UNIX_TIMESTAMP()) as modifiedTS from IndoorInputDevices A JOIN IndoorAirQualityDevice B ON A.indoorDeviceToken = B.indoorDeviceToken"

  val HBASE_SPECK_RAWDATA_CATALOG = s"""{
                                       |"table":{"namespace":"default", "name":"Speck_RawData", "tableCoder":"PrimitiveType"},
                                       |"rowkey":"key",
                                       |"columns":{
                                       |"indoorDeviceKey":{"cf":"rowkey", "col":"key", "type":"string"},
                                       |"indoorDeviceToken":{"cf":"DeviceData", "col":"indoorDeviceToken", "type":"string"},
                                       |"unixTS":{"cf":"DeviceData", "col":"unixTS", "type":"string"},
                                       |"particleConcentration":{"cf":"DeviceData", "col":"particleConcentration", "type":"string"},
                                       |"temperature":{"cf":"DeviceData", "col":"temperature", "type":"string"},
                                       |"humidity":{"cf":"DeviceData", "col":"humidity", "type":"string"},
                                       |"utcYear":{"cf":"DeviceData", "col":"utcYear", "type":"string"},
                                       |"createdTS":{"cf":"DeviceData", "col":"createdTS", "type":"string"},
                                       |"modifiedTS":{"cf":"DeviceData", "col":"modifiedTS", "type":"string"}
                                       |}
                                       |}""".stripMargin

  val LOCAL_DEVICES_QUERY = "SELECT A.indoorDeviceID, A.accountID, A.gmtOffset, A.dstOffset, B.particleConcentration, B.temperature, B.humidity, B.indoorDeviceToken, B.utcYear, "+
    "B.unixTS, "+
    "FROM_UNIXTIME(B.unixTS) as utcTS, "+
    "case when B.unixTS between C.dst_start_unixTS and C.dst_end_unixTS then (cast(B.unixTS as bigint) + cast(A.dstOffset as bigint)*3600) else (cast(B.unixTS as bigint) + cast(A.gmtOffset as bigint)*3600) end as localUnixTS, "+
    "case when B.unixTS between C.dst_start_unixTS and C.dst_end_unixTS then FROM_UNIXTIME(cast(B.unixTS as bigint) + cast(A.dstOffset as bigint)*3600) else FROM_UNIXTIME(cast(B.unixTS as bigint) + cast(A.gmtOffset as bigint)*3600) end as localTS, "+
    "case when B.unixTS between C.dst_start_unixTS and C.dst_end_unixTS then DATE_FORMAT(FROM_UNIXTIME(cast(B.unixTS as bigint) + cast(A.dstOffset as bigint)*3600), 'yyyyMMdd') else DATE_FORMAT(FROM_UNIXTIME(cast(B.unixTS as bigint) + cast(A.gmtOffset as bigint)*3600), 'yyyyMMdd') end as localDate, "+
    "case when B.unixTS between C.dst_start_unixTS and C.dst_end_unixTS then DATE_FORMAT(FROM_UNIXTIME(cast(B.unixTS as bigint) + cast(A.dstOffset as bigint)*3600), 'HH') else DATE_FORMAT(FROM_UNIXTIME(cast(B.unixTS as bigint) + cast(A.gmtOffset as bigint)*3600), 'HH') end as localHour "+
    "FROM IndoorAirQualityDevice A JOIN IndoorDevices B on A.indoorDeviceToken = B.indoorDeviceToken JOIN DSTCalendar C on B.utcYear = C.year and A.activeFlag = 1"

  val UPDATE_DEVICES_QUERY = "SELECT A.indoorDeviceID, A.accountID, A.indoorDeviceToken, A.particleConcentration, A.temperature, A.humidity, A.localTS, FROM_UNIXTIME(UNIX_TIMESTAMP()) as createdTS, FROM_UNIXTIME(UNIX_TIMESTAMP()) as modifiedTS FROM "+
    "IndoorDevicesLocalTS A join (select indoorDeviceID, accountID, max(unixTS) as lastUploadTS from IndoorDevicesLocalTS group by indoorDeviceID, accountID) B "+
    " ON A.indoorDeviceID = B.indoorDeviceID and A.accountID = B.accountID and A.unixTS = B.lastUploadTS"

  val INDOOR_DEVICE_HOURLY_TOTAL_QUERY = "SELECT indoorDeviceId, accountId, localDate, localHour,  sum(particleConcentration) as totalParticleConcentration, count(*) as noOfRecords from IndoorDevicesLocalTS group by indoorDeviceId, accountId, localDate, localHour"

  val HBASE_INDOOR_DEVICE_HOURLYDATA_CATALOG = s"""{
                                                  |"table":{"namespace":"default", "name":"IndoorAirQuality_Hourly", "tableCoder":"PrimitiveType"},
                                                  |"rowkey":"key",
                                                  |"columns":{
                                                  |"indoorDeviceKey":{"cf":"rowkey", "col":"key", "type":"string"},
                                                  |"indoorDeviceId":{"cf":"HourlyData", "col":"indoorDeviceId", "type":"string"},
                                                  |"date":{"cf":"HourlyData", "col":"date", "type":"string"},
                                                  |"hour":{"cf":"HourlyData", "col":"hour", "type":"string"},
                                                  |"avgParticleConcentration":{"cf":"HourlyData", "col":"avgParticleConcentration", "type":"string"},
                                                  |"noOfRecords":{"cf":"HourlyData", "col":"noOfRecords", "type":"string"},
                                                  |"createdTS":{"cf":"HourlyData", "col":"createdTS", "type":"string"},
                                                  |"modifiedTS":{"cf":"HourlyData", "col":"modifiedTS", "type":"string"}
                                                  |}
                                                  |}""".stripMargin

  val HBASE_INDOOR_FINAL_HOURLY_AVG_QUERY = "select cast(CONCAT(A.indoorDeviceId,'_',A.localDate,'_',A.localHour) as string) as indoorDeviceKey, cast(A.indoorDeviceId as string) as indoorDeviceId, cast(A.localDate as string) as date, "+
    "cast(A.localHour as string) as hour, cast((NVL(B.avgParticleConcentration,0) * NVL(B.noOfRecords, 0) + A.totalParticleConcentration) / (NVL(B.noOfRecords, 0) + A.noOfRecords) as string) as avgParticleConcentration, "+
    "cast((A.noOfRecords + NVL(B.noOfRecords, 0)) as string) as noOfRecords, cast(FROM_UNIXTIME(UNIX_TIMESTAMP()) as string) as createdTS, "+
    "cast(FROM_UNIXTIME(UNIX_TIMESTAMP()) as string) as modifiedTS "+
    "from IndoorInputHoulyTemp A left outer join IndoorHourlyTemp B on A.indoorDeviceId = B.indoorDeviceId and A.localDate = B.date and A.localHour = B.hour"

  val INDOOR_HOURLY_SQL_QUERY = "SELECT indoorDeviceId, date, hour, avgParticleConcentration, createdTS, modifiedTS from IndoorFinalHourlyAverage"

  val INDOOR_24_HOURLY_QUERY = "SELECT A.indoorDeviceId, A.date, A.hour, substr(A.date,0,6) as month, A.avgParticleConcentration, (A.avgParticleConcentration * A.noOfRecords) as totalParticleConcentration ,A.noOfRecords  "+
    "from IndoorDeviceHourlyAverage A join IndoorFinalHourlyAverage B ON A.date = B.date and A.indoorDeviceId = B.indoorDeviceId"
  val HBASE_INDOOR_DEVICE_DAILYDATA_CATALOG = s"""{
                                                 |"table":{"namespace":"default", "name":"IndoorAirQuality_Daily", "tableCoder":"PrimitiveType"},
                                                 |"rowkey":"key",
                                                 |"columns":{
                                                 |"indoorDeviceKey":{"cf":"rowkey", "col":"key", "type":"string"},
                                                 |"indoorDeviceId":{"cf":"DailyData", "col":"indoorDeviceId", "type":"string"},
                                                 |"month":{"cf":"DailyData", "col":"month", "type":"string"},
                                                 |"date":{"cf":"DailyData", "col":"date", "type":"string"},
                                                 |"avgParticleConcentration":{"cf":"DailyData", "col":"avgParticleConcentration", "type":"string"},
                                                 |"noOfRecords":{"cf":"DailyData", "col":"noOfRecords", "type":"string"},
                                                 |"createdTS":{"cf":"DailyData", "col":"createdTS", "type":"string"},
                                                 |"modifiedTS":{"cf":"DailyData", "col":"modifiedTS", "type":"string"}
                                                 |}
                                                 |}""".stripMargin

  val INDOOR_DAILY_AVG_QUERY = "SELECT cast(CONCAT(indoorDeviceId,'_',month,'_', date) as string) as indoorDeviceKey, cast(indoorDeviceId as string) as indoorDeviceId, "+
    "cast(month as string) as month, cast(date as string) as date, cast(totalParticleConcentration/noOfRecords as string) as avgParticleConcentration, cast(noOfRecords as string) as noOfRecords,  "+
    "cast(FROM_UNIXTIME(UNIX_TIMESTAMP()) as string) as createdTS, cast(FROM_UNIXTIME(UNIX_TIMESTAMP()) as string) as modifiedTS "+
    "from (select indoorDeviceId, month, date, sum(totalParticleConcentration) as totalParticleConcentration, sum(noOfRecords) as noOfRecords from IndoorFinalHourlyTemp group by indoorDeviceId, month, date)"

  val INDOOR_DAILY_SQL_QUERY = "SELECT indoorDeviceId, month, date, avgParticleConcentration, createdTS, modifiedTS from IndoorFinalDailyAverage"

  val HBASE_INDOOR_DEVICE_MONTHLYDATA_CATALOG = s"""{
                                                   |"table":{"namespace":"default", "name":"IndoorAirQuality_Monthly", "tableCoder":"PrimitiveType"},
                                                   |"rowkey":"key",
                                                   |"columns":{
                                                   |"indoorDeviceKey":{"cf":"rowkey", "col":"key", "type":"string"},
                                                   |"indoorDeviceId":{"cf":"MonthlyData", "col":"indoorDeviceId", "type":"string"},
                                                   |"year":{"cf":"MonthlyData", "col":"year", "type":"string"},
                                                   |"month":{"cf":"MonthlyData", "col":"month", "type":"string"},
                                                   |"avgParticleConcentration":{"cf":"MonthlyData", "col":"avgParticleConcentration", "type":"string"},
                                                   |"noOfRecords":{"cf":"MonthlyData", "col":"noOfRecords", "type":"string"},
                                                   |"createdTS":{"cf":"MonthlyData", "col":"createdTS", "type":"string"},
                                                   |"modifiedTS":{"cf":"MonthlyData", "col":"modifiedTS", "type":"string"}
                                                   |}
                                                   |}""".stripMargin

  val INDOOR_ALL_DATES_MONTHLY_AVG_QUERY = "SELECT A.indoorDeviceId, A.month, A.date, substr(A.date,0,4) as year, A.avgParticleConcentration, (A.avgParticleConcentration * A.noOfRecords) as totalParticleConcentration ,A.noOfRecords "+
    " FROM IndoorAllDatesMonthlyAvgTemp A join IndoorFinalDailyAverage B on A.month = B.month and A.indoorDeviceId = B.indoorDeviceId"

  val INDOOR_MONTHLY_AVG_QUERY = "SELECT cast(CONCAT(indoorDeviceId,'_',year,'_', month) as string) as indoorDeviceKey, cast(indoorDeviceId as string) as indoorDeviceId, "+
    "cast(year as string) as year, cast(month as string) as month, cast(totalParticleConcentration/noOfRecords as string) as avgParticleConcentration, cast(noOfRecords as string) as noOfRecords,  "+
    "cast(FROM_UNIXTIME(UNIX_TIMESTAMP()) as string) as createdTS, cast(FROM_UNIXTIME(UNIX_TIMESTAMP()) as string) as modifiedTS "+
    "from (select indoorDeviceId, year, month, sum(totalParticleConcentration) as totalParticleConcentration, sum(noOfRecords) as noOfRecords from IndoorAllDatesMonthlyAvg group by indoorDeviceId, year, month)"

  val INDOOR_MONTHLY_SQL_QUERY = "SELECT indoorDeviceId, year, month, avgParticleConcentration, createdTS, modifiedTS from IndoorFinalMonthlyAverage"

  val HBASE_INVALID_SPECK_RAWDATA_CATALOG = s"""{
                                               |"table":{"namespace":"default", "name":"Invalid_Speck_RawData", "tableCoder":"PrimitiveType"},
                                               |"rowkey":"key",
                                               |"columns":{
                                               |"unixTS":{"cf":"rowkey", "col":"key", "type":"string"},
                                               |"date":{"cf":"InvalidJson", "col":"date", "type":"string"},
                                               |"invalidJSON":{"cf":"InvalidJson", "col":"inputJson", "type":"string"},
                                               |"errorMessage":{"cf":"InvalidJson", "col":"errorMessage", "type":"string"}
                                               |}
                                               |}""".stripMargin

  val HBASE_ERROR_SPECK_RAWDATA_CATALOG = s"""{
                                             |"table":{"namespace":"default", "name":"Error_Speck_RawData", "tableCoder":"PrimitiveType"},
                                             |"rowkey":"key",
                                             |"columns":{
                                             |"unixTS":{"cf":"rowkey", "col":"key", "type":"string"},
                                             |"date":{"cf":"ErrorData", "col":"date", "type":"string"},
                                             |"errorJSON":{"cf":"ErrorData", "col":"inputJson", "type":"string"},
                                             |"errorMessage":{"cf":"ErrorData", "col":"errorMessage", "type":"string"}
                                             |}
                                             |}""".stripMargin

  val INACTIVE_DEVICES_QUERY = "SELECT A.indoorDeviceKey, A.indoorDeviceToken, A.unixTS, A.particleConcentration, A.temperature, A.humidity, A.utcYear, A.utcDate, A.createdTS, A.modifiedTS FROM IndoorDevices A LEFT JOIN IndoorDevicesLocalTS B ON A.indoorDeviceToken = B.indoorDeviceToken WHERE B.indoorDeviceToken IS NULL"

  val HBASE_INACTIVE_SPECK_RAWDATA_CATALOG = s"""{
                                                |"table":{"namespace":"default", "name":"Inactive_Devices_Speck_RawData", "tableCoder":"PrimitiveType"},
                                                |"rowkey":"key",
                                                |"columns":{
                                                |"indoorDeviceKey":{"cf":"rowkey", "col":"key", "type":"string"},
                                                |"indoorDeviceToken":{"cf":"DeviceData", "col":"indoorDeviceToken", "type":"string"},
                                                |"unixTS":{"cf":"DeviceData", "col":"unixTS", "type":"string"},
                                                |"particleConcentration":{"cf":"DeviceData", "col":"particleConcentration", "type":"string"},
                                                |"temperature":{"cf":"DeviceData", "col":"temperature", "type":"string"},
                                                |"humidity":{"cf":"DeviceData", "col":"humidity", "type":"string"},
                                                |"utcYear":{"cf":"DeviceData", "col":"utcYear", "type":"string"},
                                                |"createdTS":{"cf":"DeviceData", "col":"createdTS", "type":"string"},
                                                |"modifiedTS":{"cf":"DeviceData", "col":"modifiedTS", "type":"string"}
                                                |}
                                                |}""".stripMargin
}

