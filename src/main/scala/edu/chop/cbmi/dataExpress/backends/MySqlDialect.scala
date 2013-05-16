package edu.chop.cbmi.dataExpress.backends

import edu.chop.cbmi.dataExpress.dataModels.sql._
import edu.chop.cbmi.dataExpress.dataModels.DataType

/**
 * SQL dialect for MySQL 
 * @see [[edu.chop.cbmi.dataExpress.backends.SqlDialect]]
 * 
 */

case object MySqlDialect extends SqlDialect {
  val identifierQuote = "`"
  val tableCreate = "CREATE TABLE"
  val tableTruncate = "TRUNCATE TABLE"
  val tableDrop = "DROP TABLE"
  val tableCascade = "CASCADE"
  val insertStatement = "INSERT INTO"
  val insertValues = "VALUES"
  val transactionStart = "START TRANSACTION"
  val transactionEnd = "COMMIT"
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

  def dropTable(name: String, cascade: Boolean = false, schemaName: Option[String] = None)  = {
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

    var quotedSchemaPrefix: String = null
    if (schemaName.isEmpty) quotedSchemaPrefix = "" else quotedSchemaPrefix = quoteIdentifier(schemaName.get) + "."

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
      case IntegerDataType                                => "INTEGER"
      case SmallIntegerDataType                           => "SMALLINT"
      //TODO: Remove blatant Postgresql hack for lack of tinyint support below
      case TinyIntegerDataType                            => "SMALLINT"
      case FloatDataType(p)                                 => {
        if (p <= 53)    "FLOAT(%d)".format(p)
        else            "FLOAT(53)"
      }
      case DecimalDataType(p, s)                            => "DECIMAL(%d,%d)".format(p, s)
      case DateDataType                                   => "DATE"
      //case DateTimeDataType(true)                           => "TIMESTAMP WITH TIME ZONE"
      //MySql does not have TIME STAMP WITH TIMEZONE
      //Must run off   jdbcCompliantTruncation for these to work by setting jdbcCompliantTruncation=false
      //For Connection
      //http://bugs.mysql.com/bug.php?id=21544
      //case DateTimeDataType(true)                           => "DATETIME"
      case DateTimeDataType(true)                           => "TIMESTAMP"
      case DateTimeDataType(false)                          => "TIMESTAMP"
      //Was Causing : MysqlDataTruncation: Data truncation: Incorrect datetime value, with certain dates
      //MySQL does not accept TIMESTAMP values that include a zero in the day or month column
      //or values that are not a valid date. The sole exception to this rule is the special “zero” value '0000-00-00 00:00:00'.
      //http://dev.mysql.com/doc/refman/5.1/en/news-5-1-4.html
      //http://dev.mysql.com/doc/refman/5.1/en/datetime.html
      //case DateTimeDataType(false)                          => "DATETIME"
      //MySQL does not have TIME WITH TIME ZONE
      case TimeDataType(true)                               => "TIME WITH TIME ZONE"
      case TimeDataType(false)                              => "TIME"
      case CharacterDataType(length, fixed)                 => {
        if (fixed) {
          if    (length <= 255)   "CHAR(%d)".format(length)
          else                    "TEXT"
        }
        else "VARCHAR(%d)".format(length)
      }
      case TextDataType                                   => "TEXT"
      case BigBinaryDataType                              => "BLOB"
      case BooleanDataType                                => "BOOLEAN"
      case BitDataType                                    => "BIT"
      case _                                                => ""
    }
  }

}