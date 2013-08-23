package edu.chop.cbmi.dataExpress.backends
import edu.chop.cbmi.dataExpress.dataModels.sql._
import java.sql.ResultSetMetaData
import edu.chop.cbmi.dataExpress.dataModels.DataType
import collection.Seq

/**
 * This trait is to be used when constructing new SQL backends for DataExpress. It provides basic SQL
 * syntax for supported databases. This trait allows for customization of the SQL Syntax
 * used in each RDBMS, while keeping the DataExpress API as generic as possible. More importantly, this trait
 * provides baseline JDBC -> DataExpress type mappings which can be used across most database implementations.
 *
 *
 */
trait SqlDialect {
  /**
   * Returns a database identifier (such as a table or column name) using the proper quote format. Note that
   * in some databases it is considered an error to quote non-system strings, so this method should carefully
   * implement this logic if necessary.
   *
   * @param id The identifier that must be returned quoted
   */
  def quoteIdentifier(id: String): String

  /**
   * Returns a complete `CREATE TABLE` SQL statement
   *
   * @param name the table name
   * @param columns a list of column tuples where each tuple is a column name and [[edu.chop.cbmi.dataExpress.dataModels.DataType]]
   * @param schemaName the schema where the table will be created
   */
  def createTable(name: String, columns: List[(String, DataType)], schemaName: Option[String]): String

  /**
   * Returns a complete `DROP TABLE` SQL Statement
   *
   * @param name The name of the table
   * @param cascade Indicates whether the cascade option should be used (by default it is not)
   * @param schemaName The schema where the table should be dropped
   */
  def dropTable(name: String, cascade: Boolean = false, schemaName: Option[String] = None): String

  /**
   * Returns a complete `TRUNCATE TABLE` SQL Statement
   *
   * @param table The name of the table to truncate
   * @param schemaName The schema where the table should be truncated
   */
  def truncate(table: String, schemaName: Option[String] = None): String

  /**
   * Returns a `COMMIT` SQL Statement
   *
   */
  def commit(): String

  /**
   * Returns a `ROLLBACK` SQL statement
   */
  def rollback(): String

  /**
   * Returns an end SQL transaction statement
   */
  def endTransaction(): String

  /**
   * Returns a start SQL transaction statement
   */
  def startTransaction(): String

  /**
   * Returns an `INSERT` statement (where the values to be inserted are placeholder variables to be assigned during
   * JDBC prepared statement construction
   *
   * @param tableName The name of the table where the insert will be applied
   * @param columnNames A list of the column names to use when performing the insert
   * @param schemaName  The schema where the table is located
   */
  def insertRecord(tableName: String, columnNames: List[String], schemaName: Option[String] = None): String

  /**
   *  Returns a SQL `UPDATE` statement that using a filter on table values. In order to allow specificity,
   *  the user passes in a list of (`columnName`, `value`) filters. The expected behavior is that these tuples
   *  get converted into the `WHERE` clause. For example:
   *
   *  {{{List(("id",12345),("type","Luggage Combination"))}}}
   *
   *  passed in as a filter is converted to the SQL `WHERE id = 12345 AND type = 'Luggage Combination'`
   *
   *  @param tableName The name of the table where the update will be applied
   *  @param columnNames The list of column names to be updated
   *  @param filter A list of (`columnName`,`value`) tuples to be used when constructing the `WHERE` clause
   *
   *
   */
  def updateRecords(tableName: String, columnNames: List[String],
    filter: List[(String, Any)], schemaName: Option[String] = None): String
  /**
   * Returns the database-appropriate SQL type representation of a [[edu.chop.cbmi.dataExpress.dataModels.DataType]]
   */
  def toSqlString(dataType: DataType): String

  /**
   * Returns the DataExpress [[edu.chop.cbmi.dataExpress.dataModels.DataType]] values for columns in a JDBC `ResultSet`
   *
   * @param column_names A sequence of column names
   * @param meta JDBC `ResultSet` metadata
   */
  def mapDataTypes(column_names: Seq[String], meta: ResultSetMetaData) = {
    column_names map ((name: String) => column_names.indexOf(name)) map ((j: Int) => {
      val i = j + 1
      meta.getColumnType(i) match {
        case java.sql.Types.BIGINT => BigIntegerDataType
        case java.sql.Types.INTEGER => IntegerDataType
        case java.sql.Types.SMALLINT => SmallIntegerDataType
        case java.sql.Types.TINYINT => TinyIntegerDataType
        case java.sql.Types.FLOAT => {
          val precision = meta.getPrecision(i)
          FloatDataType(precision)
        }
        case java.sql.Types.REAL | java.sql.Types.DOUBLE => {
          val precision = meta.getPrecision(i)
          FloatDataType(precision)
        }
        case java.sql.Types.NUMERIC | java.sql.Types.DECIMAL => {
          val precision = meta.getPrecision(i)
          val scale = meta.getScale(i)
          //fix for Oracle FLOATS
          if (scale == -127) FloatDataType(precision) else DecimalDataType(precision, scale)
        }
        case java.sql.Types.CHAR => CharacterDataType(meta.getColumnDisplaySize(i), fixedWidth = true)
        case java.sql.Types.VARCHAR => CharacterDataType(meta.getColumnDisplaySize(i), fixedWidth = false)
        case java.sql.Types.TIMESTAMP => {
          val tzSupport = meta.getColumnTypeName(i).toUpperCase.contains("WITH TIME ZONE")
          DateTimeDataType(tzSupport)
        }
        case -101 => DateTimeDataType(withZone = true) //-101 = jdbc: TIME STAMP WITH TIME ZONE
        case java.sql.Types.DATE => DateDataType
        case java.sql.Types.TIME => {
          val tzSupport = meta.getColumnTypeName(i).toUpperCase.contains("WITH TIME ZONE")
          TimeDataType(tzSupport)
        }
        case java.sql.Types.LONGVARCHAR | java.sql.Types.CLOB => TextDataType
        case java.sql.Types.LONGVARBINARY | java.sql.Types.BLOB | java.sql.Types.BINARY => BigBinaryDataType
        case java.sql.Types.BOOLEAN => BooleanDataType
        // JDBC Spec suggests that portable code should represent the BIT type as
        // a smallInt: http://download.oracle.com/javase/6/docs/technotes/guides/jdbc/getstart/mapping.html#999005
        //Postgres maps Boolean to Bit and so data such as 't' for true are failing on the attempted storage as integer
        //values with this one:
        //case java.sql.Types.BIT                               => SmallIntegerDataType()
        //Using this instead so that we can handle the variations on a BIT datatype on a DBMS to DBMS basis
        case java.sql.Types.BIT => BitDataType
        case _ => {
          throw new RuntimeException("Can't map JDBC type to a known DataExpress type " +
            meta.getColumnType(i) + " for column " + meta.getColumnLabel(i))
        }
      }
    })
  }

}