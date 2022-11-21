/*
package com.filter.streaming.dbutils


import java.io.Serializable
import com.microsoft.sqlserver.jdbc._
import com.microsoft.sqlserver.jdbc.SQLServerMetaData
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

class TableData (
                  tableSchema: StructType,
                  rowsArray: Array[Row]
                ) extends Serializable with ISQLServerDataRecord {
  var currentRow: Int = 0
  val totalRecords: Int = rowsArray.length
  def getColumnCount: Int = {
    tableSchema.fields.length
  }
  override def getRowData: Array[AnyRef] = {
    val r: Row = rowsArray(currentRow)
    currentRow += 1
    r.toSeq.map(any => if (any != null) any.asInstanceOf[AnyRef] else null).toArray
  }
  override def next(): Boolean = {
    totalRecords > currentRow
  }
  def getColumnMetaData(columnIndex: Int): SQLServerMetaData = {
    val field: StructField = this.tableSchema.fields(columnIndex - 1)
    field.dataType.typeName match {
      case "integer" =>
        new SQLServerMetaData(field.name, java.sql.Types.INTEGER)
      case "long" =>
        new SQLServerMetaData(field.name, java.sql.Types.BIGINT)
      case "string" =>
        new SQLServerMetaData(field.name, java.sql.Types.NVARCHAR)
      case "boolean" =>
        new SQLServerMetaData(field.name, java.sql.Types.BIT)
      case "double" =>
        new SQLServerMetaData(field.name, java.sql.Types.DOUBLE)
      case "float" =>
        new SQLServerMetaData(field.name, java.sql.Types.FLOAT)
      case "short" =>
        new SQLServerMetaData(field.name, java.sql.Types.SMALLINT)
      case "byte" =>
        new SQLServerMetaData(field.name, java.sql.Types.TINYINT)
      case "binary" =>
        new SQLServerMetaData(field.name, java.sql.Types.BLOB)
      case "timestamp" =>
        new SQLServerMetaData(field.name, java.sql.Types.TIMESTAMP)
      case "date" =>
        new SQLServerMetaData(field.name, java.sql.Types.DATE)
      case "decimal(4,2)" =>
        new SQLServerMetaData(field.name, java.sql.Types.DECIMAL)
      case "number" =>
        new SQLServerMetaData(field.name, java.sql.Types.DECIMAL)
      case _ =>
        null
    }
  }
}

*/
