package com.filter.streaming.dbutils

object FilterConstants {
  /*
   * Filter SQL DB and query details
   */
  val DB_SERVER_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

  val SEL_FILTER_INFO = "select filterID as filterid, batteryLife as batterylife, batteryLifeTS as batterylifets from 3M_FILTER_TABLE order by filterid, batteryLifeTS"
}

