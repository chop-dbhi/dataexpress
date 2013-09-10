package edu.chop.cbmi.dataExpress.test.backends

import org.scalatest.{FeatureSpec, FlatSpec}
import edu.chop.cbmi.dataExpress.backends._
import org.scalatest.prop.TableDrivenPropertyChecks._
import edu.chop.cbmi.dataExpress.backends.SqlBackend
import edu.chop.cbmi.dataExpress.test.util.TestProps


class SqlBackendSpec extends FlatSpec {

  val sqlBackendsToTest = Table(("backend", "dialect", "driver"),
                             //   ("postgres", PostgresSqlDialect, "org.postgresql.Driver"),
                             //   ("mysql", MySqlDialect, "com.mysql.jdbc.Driver"),
                                ("sqlite", SqLiteDialect, "org.sqlite.JDBC")
                                )
  private def propertiesForBackend(backendName: String) = TestProps.getDbPropFilePath(backendName)

  forAll(sqlBackendsToTest) {
    (backend: String, dialect: SqlDialect, driver:String) =>

    val backendForTest = SqlBackendFactory(propertiesForBackend(backend), dialect, driver)

    def withSourceDatabase(testCode: (SqlBackend,String) => Any) {
      val source = backendForTest
      val tablePrefix = java.util.UUID.randomUUID.toString.substring(0,8)
      try testCode(source,tablePrefix)
      finally {
        val schema = try {source.connection.getSchema()}
        catch { case e:AbstractMethodError => null} //Some databases don't implement this abstract method (I'm looking at you Sqlite)
        val rs = source.connection.getMetaData.getTables(null, schema, s"$tablePrefix%", null)
        val tables = scala.collection.mutable.ListBuffer[String]()
        while (rs.next) tables += rs.getString(3)
        tables.foreach{t:String => source.execute(s"DROP TABLE $t")}
        source.close()
    }
  }
    def withTargetDatabase(testCode: SqlBackend => Any) {
    val target = backendForTest
    try testCode(target)
    finally target.close()
  }

   behavior of s"A $backend backend"

   it should "Allow a user to connect" in withSourceDatabase { (source: SqlBackend, prefix: String) =>
     source.connect()
   }

  }



}