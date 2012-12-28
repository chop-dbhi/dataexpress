package edu.chop.cbmi.dataExpress.backends

import edu.chop.cbmi.dataExpress.dataModels.sql._
import edu.chop.cbmi.dataExpress.dataModels.DataType

/**
 * SQL dialect for SQLite 
 * @see [[edu.chop.cbmi.dataExpress.backends.SqlDialect]]
 * 
 */
case object SqLiteDialect extends SqlDialect {
  val identifierQuote = "\""
  val tableCreate = "CREATE TABLE"
  val tableTruncate = "DELETE FROM"
  val tableDrop = "DROP TABLE"
  val tableCascade = "" //TODO: Add some logic to dynamically set this based on the connection
  val insertStatement = "INSERT INTO"
  val insertValues = "VALUES"
  val transactionStart = "BEGIN TRANSACTION"
  val transactionEnd = "END TRANSACTION"
  val transactionCommit = "COMMIT"
  val transactionRollback = "ROLLBACK"
  
  val reservedWords = List("ABORT",
							"ACTION",
							"ADD",
							"AFTER",
							"ALL",
							"ALTER",
							"ANALYZE",
							"AND",
							"AS",
							"ASC",
							"ATTACH",
							"AUTOINCREMENT",
							"BEFORE",
							"BEGIN",
							"BETWEEN",
							"BY",
							"CASCADE",
							"CASE",
							"CAST",
							"CHECK",
							"COLLATE",
							"COLUMN",
							"COMMIT",
							"CONFLICT",
							"CONSTRAINT",
							"CREATE",
							"CROSS",
							"CURRENT_DATE",
							"CURRENT_TIME",
							"CURRENT_TIMESTAMP",
							"DATABASE",
							"DEFAULT",
							"DEFERRABLE",
							"DEFERRED",
							"DELETE",
							"DESC",
							"DETACH",
							"DISTINCT",
							"DROP",
							"EACH",
							"ELSE",
							"END",
							"ESCAPE",
							"EXCEPT",
							"EXCLUSIVE",
							"EXISTS",
							"EXPLAIN",
							"FAIL",
							"FOR",
							"FOREIGN",
							"FROM",
							"FULL",
							"GLOB",
							"GROUP",
							"HAVING",
							"IF",
							"IGNORE",
							"IMMEDIATE",
							"IN",
							"INDEX",
							"INDEXED",
							"INITIALLY",
							"INNER",
							"INSERT",
							"INSTEAD",
							"INTERSECT",
							"INTO",
							"IS",
							"ISNULL",
							"JOIN",
							"KEY",
							"LEFT",
							"LIKE",
							"LIMIT",
							"MATCH",
							"NATURAL",
							"NO",
							"NOT",
							"NOTNULL",
							"NULL",
							"OF",
							"OFFSET",
							"ON",
							"OR",
							"ORDER",
							"OUTER",
							"PLAN",
							"PRAGMA",
							"PRIMARY",
							"QUERY",
							"RAISE",
							"REFERENCES",
							"REGEXP",
							"REINDEX",
							"RELEASE",
							"RENAME",
							"REPLACE",
							"RESTRICT",
							"RIGHT",
							"ROLLBACK",
							"ROW",
							"SAVEPOINT",
							"SELECT",
							"SET",
							"TABLE",
							"TEMP",
							"TEMPORARY",
							"THEN",
							"TO",
							"TRANSACTION",
							"TRIGGER",
							"UNION",
							"UNIQUE",
							"UPDATE",
							"USING",
							"VACUUM",
							"VALUES",
							"VIEW",
							"VIRTUAL",
							"WHEN",
							"WHERE")  
    
  def quoteIdentifier(id: String) = {
    //Can only quote identifiers, everything else actually USES the quotes
   	if (reservedWords.contains(id.toUpperCase())){
   		"%s%s%s".format(identifierQuote, id, identifierQuote)
   	}
   	else {
   	      id
   	}
    
  }

  def createTable(name: String, columns: List[(String, DataType)], schemaName: Option[String]) = {
    val createBlock = columns.map {
      (t: Tuple2[String, DataType]) => "%s %s".format(quoteIdentifier(t._1), toSqlString(t._2))
    }
    val createString = "%s %s (%s)".format(tableCreate, quoteIdentifier(name), createBlock.mkString(", "))
    createString
  }

  def dropTable(name: String, cascade: Boolean = false, schemaName: Option[String] = None) = {
    val fmtString = if (cascade) "%s %s " + tableCascade else "%s %s"
    fmtString.format(tableDrop, quoteIdentifier(name))
  }

  def truncate(table: String, schemaName: Option[String] = None) = {
    "%s %s".format(tableTruncate, quoteIdentifier(table))
  }

  def commit() = transactionCommit

  def rollback() = transactionRollback

  def endTransaction() = transactionEnd

  def startTransaction() = transactionStart

  def insertRecord(tableName: String, columnNames: List[String], schemaName: Option[String] = None) = {
    val valuesList = "%s".format(List.fill(columnNames.size)("?").mkString(","))

    val sqlString = "%s %s (%s) %s (%s)".format(insertStatement,
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
    val sqlString = "UPDATE %s SET %s %s".format(quoteIdentifier(tableName), setString, whereString)
    sqlString
  }


  def toSqlString(dataType: DataType): String = {
    dataType match {
      case IntegerDataType()                  => "INTEGER"
      case SmallIntegerDataType()             => "SMALLINT"
      //TODO: Remove blatant Postgresql hack for lack of tinyint support below
      case TinyIntegerDataType()              => "SMALLINT"
      case FloatDataType(p)                   => "FLOAT"
      case DecimalDataType(p, s)              => "DECIMAL(%d,%d)".format(p, s)
      case DateDataType()                     => "DATE"
      case DateTimeDataType(true)             => "TIMESTAMP WITH TIME ZONE"
      case DateTimeDataType(false)            => "TIMESTAMP"
      case TimeDataType(true)                 => "TIME WITH TIME ZONE"
      case TimeDataType(false)                => "TIME"
      case CharacterDataType(length, fixed)   => {
        if (fixed) "CHAR(%d)".format(length)
        else {
          if (length > 10485760){"TEXT"}
          else  {"VARCHAR(%d)".format(length)}
        }
      }
      case TextDataType()                     => "TEXT"
      case BigBinaryDataType()                => "BLOB"
      case BooleanDataType()                  => "INTEGER"
      case BitDataType()                      => "INTEGER"
      case _                                  => ""
    }
  }

}

