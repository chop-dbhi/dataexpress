package edu.chop.cbmi.dataExpress.backends

import java.util.Properties
import scala.util.Try
import edu.chop.cbmi.dataExpress.dataModels.{DataRow, DataTable}
import edu.chop.cbmi.dataExpress.dataModels.DataType

/**
 * Snowflake READ-ONLY backend
 *
 * - Uses JDBC connectivity via the existing SqlBackend.connect implementation.
 * - Blocks all DDL/DML and batch/insert/update operations.
 * - Only allows read-style statements.
 */
class SnowflakeReadOnlyBackend(
  connectionProperties: Properties,
  sqlDialect: SqlDialect = null,
  driverClassName: String = null
) extends SqlBackend(
  connectionProperties,
  sqlDialect,
  SnowflakeReadOnlyBackend.resolveDriverClassName(connectionProperties, driverClassName)
) {

  private val allowedPrefixes = Set("SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN")

  private def assertReadOnlySql(sql: String): Unit = {
    val s = Option(sql).getOrElse("").trim.toUpperCase
    val ok = allowedPrefixes.exists(s.startsWith)
    if (!ok) {
      throw new UnsupportedOperationException(
        s"Snowflake backend is read-only; blocked SQL: ${sql.take(160)}"
      )
    }
  }

  // ----------------------------
  // READ OPERATIONS
  // ----------------------------

  override def executeQuery(
    sqlStatement: String,
    bindvars: Seq[Option[_]] = Seq.empty[Option[_]],
    fetchSize: Int = 20
  ): java.sql.ResultSet = {
    assertReadOnlySql(sqlStatement)

    val effectiveFetchSize =
      Option(connectionProperties.getProperty("fetchSize"))
        .flatMap(v => Try(v.trim.toInt).toOption)
        .getOrElse(100)

    if (connection == null) connect()
    super.executeQuery(sqlStatement, bindvars, effectiveFetchSize)
  }

  /**
   * SqlBackend.execute can run INSERT/UPDATE/DELETE.
   * For Snowflake RO, allow only read-like statements.
   *
   * Note: SqlBackend.execute returns true when the first result is a ResultSet.
   * For SELECT, that will be true; for blocked statements we throw.
   */
  override def execute(sqlStatement: String, bindVars: Seq[Option[_]] = Seq.empty[Option[_]]): Boolean = {
    assertReadOnlySql(sqlStatement)
    if (connection == null) connect()
    super.execute(sqlStatement, bindVars)
  }

  /**
   * Returning keys implies insert/update semantics. Block.
   */
  override def executeReturningKeys(sqlStatement: String, bindVars: Seq[Option[_]]): DataRow[_] =
    throw new UnsupportedOperationException("Snowflake backend is read-only (executeReturningKeys blocked).")

  // ----------------------------
  // TRANSACTION HELPERS
  // ----------------------------
  // Keep these safe even if sqlDialect is null.
  // They are not writes, but SqlBackend’s default implementations require sqlDialect.

  override def commit(): Boolean = {
    if (connection == null) connect()
    connection.commit()
    true
  }

  override def rollback(): Boolean = {
    if (connection == null) connect()
    connection.rollback()
    true
  }

  // These are effectively no-ops for this backend because connect() sets autoCommit(false)
  override def startTransaction(): Boolean = true
  override def endTransaction(): Boolean = true

  // ----------------------------
  // WRITE/DDL/DML METHODS: BLOCK
  // ----------------------------

  override def createTable(tableName: String, columnNames: List[String], dataTypes: List[edu.chop.cbmi.dataExpress.dataModels.DataType], schemaName: Option[String] = None): Boolean =
    throw new UnsupportedOperationException("Snowflake backend is read-only (createTable blocked).")

  override def truncateTable(tableName: String, schemaName: Option[String] = None): Boolean =
    throw new UnsupportedOperationException("Snowflake backend is read-only (truncateTable blocked).")

  override def dropTable(tableName: String, cascade: Boolean = false, schemaName: Option[String] = None): Boolean =
    throw new UnsupportedOperationException("Snowflake backend is read-only (dropTable blocked).")

  override def insertReturningKeys(tableName: String, row: DataRow[_], schemaName: Option[String] = None): DataRow[_] =
    throw new UnsupportedOperationException("Snowflake backend is read-only (insertReturningKeys blocked).")

  override def insertRow(tableName: String, row: DataRow[_], schemaName: Option[String] = None): Boolean =
    throw new UnsupportedOperationException("Snowflake backend is read-only (insertRow blocked).")

  override def batchInsert(tableName: String, table: edu.chop.cbmi.dataExpress.dataModels.DataTable[_], schemaName: Option[String] = None): Int =
    throw new UnsupportedOperationException("Snowflake backend is read-only (batchInsert blocked).")

  override def batchInsertRows(tableName: String, rows: Iterator[DataRow[_]], columnNames: List[String], schemaName: Option[String] = None): Int =
    throw new UnsupportedOperationException("Snowflake backend is read-only (batchInsertRows blocked).")

  override def updateRow(tableName: String, updated_row: DataRow[_], filter: List[(String, Any)], schemaName: Option[String] = None): Boolean =
    throw new UnsupportedOperationException("Snowflake backend is read-only (updateRow blocked).")
}

object SnowflakeReadOnlyBackend {

  /**
   * Resolve the driver class name, supporting Snowflake JDBC 4.x and 3.x driver class names.
   *
   * - 4.x: net.snowflake.client.api.driver.SnowflakeDriver
   * - 3.x: net.snowflake.client.jdbc.SnowflakeDriver
   */
  def resolveDriverClassName(props: Properties, provided: String): String = {
    val fromProps = Option(props.getProperty("driverClassName"))
    val candidate = Option(provided).orElse(fromProps)

    candidate.getOrElse {
      val v4 = "net.snowflake.client.api.driver.SnowflakeDriver"
      val v3 = "net.snowflake.client.jdbc.SnowflakeDriver"
      if (Try(Class.forName(v4)).isSuccess) v4 else v3
    }
  }
}