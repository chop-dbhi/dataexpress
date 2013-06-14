package edu.chop.cbmi.dataExpress.backends

import edu.chop.cbmi.dataExpress.dataModels.sql._
import java.sql.ResultSetMetaData
import edu.chop.cbmi.dataExpress.dataModels.DataType

/**
 * This is a "generic" implementation of a [[edu.chop.cbmi.dataExpress.backends.SqlDialect]]. It 
 * is designed to work reasonably well in databases that conform to the SQL standard. In general, this
 * object should only be used as a last resort when attempting to connect to an unsupported database.
 * 
 * @see [[edu.chop.cbmi.dataExpress.dataModels]]
 * 
 */
case object GenericSqlDialect extends SqlDialect {
  val identifierQuote = "\""
  val tableCreate = "CREATE TABLE"
  val tableTruncate = "TRUNCATE TABLE"
  val tableDrop = "DROP TABLE"
  val tableCascade = "CASCADE"
  val insertStatement = "INSERT INTO"
  val insertValues = "VALUES"
  val transactionStart = "BEGIN TRANSACTION"
  val transactionEnd = "END TRANSACTION"
  val transactionCommit = "COMMIT"
  val transactionRollback = "ROLLBACK"

  def quoteIdentifier(id: String) = {
    "%s%s%s".format(identifierQuote, id, identifierQuote)
  }

  def createTable(name: String, columns: List[(String, DataType)], schemaName: Option[String]) = {
    var quotedSchemaPrefix: String = null
    if (schemaName.isEmpty) quotedSchemaPrefix = "" else quotedSchemaPrefix = quoteIdentifier(schemaName.get) + "."

    val createBlock = columns.map {
      (t: Tuple2[String, DataType]) => "%s %s".format(quoteIdentifier(t._1), toSqlString(t._2))
    }
    val createString = "%s %s%s (%s)".format(tableCreate, quotedSchemaPrefix, quoteIdentifier(name), createBlock.mkString(", "))
    createString
  }

  def dropTable(name: String, cascade: Boolean = false, schemaName: Option[String] = None) = {
    var quotedSchemaPrefix: String = null
    if (schemaName.isEmpty) quotedSchemaPrefix = "" else quotedSchemaPrefix = quoteIdentifier(schemaName.get) + "."

    val fmtString = if (cascade) "%s %s%s " + tableCascade else "%s %s%s"
    fmtString.format(tableDrop, quotedSchemaPrefix, quoteIdentifier(name))
  }

  def truncate(table: String, schemaName: Option[String] = None) = {
    var quotedSchemaPrefix: String = null
    if (schemaName.isEmpty) quotedSchemaPrefix = "" else quotedSchemaPrefix = quoteIdentifier(schemaName.get) + "."

    "%s %s%s".format(tableTruncate, quotedSchemaPrefix, quoteIdentifier(table))
  }

  def commit() = transactionCommit

  def rollback() = transactionRollback

  def endTransaction() = transactionEnd

  def startTransaction() = transactionStart

  def insertRecord(tableName: String, columnNames: List[String], schemaName: Option[String] = None) = {
    val valuesList = "%s".format(List.fill(columnNames.size)("?").mkString(","))

    var quotedSchemaPrefix: String    =   null

    if (schemaName.isEmpty)
      quotedSchemaPrefix = ""
    else quotedSchemaPrefix = quoteIdentifier(schemaName.get) + "."

    val sqlString = "%s %s%s (%s) %s (%s)".format(insertStatement,
      quotedSchemaPrefix,
      quoteIdentifier(tableName),
      columnNames.map {
        n => quoteIdentifier(n)
      }.mkString(","),
      insertValues,
      valuesList)
    sqlString
  }

  //TODO: Create a "WHERE" clause object so we can do more than equality here
  def updateRecords(tableName: String, columnNames: List[String], filter: List[(String, Any)], schemaName: Option[String] = None) = {
    var quotedSchemaPrefix: String = null
    if (schemaName.isEmpty) quotedSchemaPrefix = "" else quotedSchemaPrefix = quoteIdentifier(schemaName.get) + "."

    val setString = columnNames.map {
      i => "%s = ?".format(quoteIdentifier(i))
    }.mkString(", ")
    val whereString = if (filter.size > 0) {
      "WHERE %s".format(filter.map {
        i => quoteIdentifier(i._1) + " = ?"
      }.mkString(" AND "))
    }
    else {
      "" //Empty string so we don't cause errors when doing bulk updates
    }
    val sqlString = "UPDATE %s%s SET %s %s".format(quotedSchemaPrefix, quoteIdentifier(tableName), setString, whereString)
    sqlString
  }


  def toSqlString(dataType: DataType): String = {
    dataType match {
      case IntegerDataType                  => "INTEGER"
      case SmallIntegerDataType             => "SMALLINT"
      //TODO: Remove blatant Postgresql hack for lack of tinyint support below
      case TinyIntegerDataType              => "SMALLINT"
      case FloatDataType(p)                   => "FLOAT"
      case DecimalDataType(p, s)              => "DECIMAL(%d,%d)".format(p, s)
      case DateDataType                     => "DATE"
      case DateTimeDataType(true)             => "TIMESTAMP WITH TIME ZONE"
      case DateTimeDataType(false)            => "TIMESTAMP"
      case TimeDataType(true)                 => "TIME WITH TIME ZONE"
      case TimeDataType(false)                => "TIME"
      case CharacterDataType(length, fixed)   => {
        if (fixed) "CHAR(%d)".format(length)
        else "VARCHAR(%d)".format(length)
      }
      case TextDataType                     => "TEXT"
      case BigBinaryDataType                => "BYTEA"
      case BooleanDataType                  => "BOOLEAN"
      case BitDataType                      => "BIT"
      case _                                  => ""
    }
  }

}