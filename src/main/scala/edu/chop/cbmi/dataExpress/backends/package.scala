package edu.chop.cbmi.dataExpress
/**
 * Provides database wrappers and connectivity for MySQL, Postgres, Oracle, and SQLite JDBC drivers.
 * 
 * ==Overview==
 * The support for individual databases is broken out into several classes:
 *
 *  - A [[edu.chop.cbmi.dataExpress.backends.SqlBackend]] which is the wrapper around the various JDBC method calls
 *  - A [[edu.chop.cbmi.dataExpress.backends.SqlDialect]] that contains the database-specific SQL quirks and implementation details
 *  - A [[edu.chop.cbmi.dataExpress.backends.SqlBackendProvider]] (optional) that allows a third-party backend to be supplied outside DataExpress 
 * 
 * The SqlBackend for the corresponding database is the primary means of establishing a datsbase connection when using DataExpress.
 * However, '''It is important to note that it is generally easier, and safer to use the [[edu.chop.cbmi.dataExpress.backends.SqlBackendFactory]] to establish the necessary backend and set up the connection'''
 *  
 * In general the SQLDialect and BackendProvider are used as part of a SQLBackend, and not directly.
 * 
 * ==Usage==
 * 
 * The [[edu.chop.cbmi.dataExpress.backends.SqlBackend]] requires properties to fully configure itself. Typically
 * these are loaded from a file, but can also be supplied through a standard [[java.util.Properties]] object. The 
 * SQL backend has very simple requirements for properties files. They must contain the following keys:
 *   - ```driverClassName```: The class name for the actual database driver
 *   - ```jdbcUri```: This is the URI required by the database driver. It is passed through directly to JDBC
 *    so you are free to specify schemas and other information here
 *   - ```username```: The username to authenticate against the database (not 
 *   required for non-authenticated RDBMS systems such as SQLite)
 *   -```password```: The password for accessing the database
 *
 * DataExpress will pass along any other properties it finds directly to JDBC, so in cases where a database
 * requires specific connection configuration (e.g. to enable SSL for Postgres), those settings can be passed along.
 * 
 * In some cases, DataExpress will set specific properties on the connection to ensure predictable behavior. Most of these
 * have been empirically determined by our testing given the narrow use cases for DataExpress. Where possible, we have made it possible
 * for you to override these defaults in your own configuration file, however this is generally not recommended.
 * 
 * @example {{{
 * scala> import edu.chop.cbmi.dataExpress.backends._
 *import edu.chop.cbmi.dataExpress.backends._
 *
 *scala> val backend = SqlBackendFactory "src/test/resources/sqlite_test.properties"
 *backend: edu.chop.cbmi.dataExpress.backends.SqlBackend = SqlBackend({jdbcUri=jdbc:sqlite:target/source.sqlite},SqLiteDialect,org.sqlite.JDBC)
 *
 *scala> backend connect
 *res0: java.sql.Connection = org.sqlite.Conn@7168f171
 *
 *scala> backend execute """CREATE TABLE de_test(id INTEGER, first_name VARCHAR, last_name VARCHAR)"""
 *res1: Boolean = false
 *
 *scala> backend commit
 *res3: Boolean = false
 *
 *scala> backend executeQuery "select count(*) from de_test"
 *res4: java.sql.ResultSet = org.sqlite.RS@3b070e76
 *
 *scala> res4.getInt(1)
 *res5: Int = 0
 *}}}
 *
 *@note The SQL backends return jdbc ResultSets which are typically a little more "low-level" than what you would
 *probably want for ETL purporses. The [[edu.chop.cbmi.dataExpress.dataModels.sql.SqlRelation]] is a much more convenient object 
 *to use for ETL.
 * 
 */
package object backends {}