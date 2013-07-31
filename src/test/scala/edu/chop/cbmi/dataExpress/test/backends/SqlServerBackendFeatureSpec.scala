package edu.chop.cbmi.dataExpress.test.backends


import org.scalatest.FeatureSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.GivenWhenThen
import edu.chop.cbmi.dataExpress.backends.SqlServerBackend
import edu.chop.cbmi.dataExpress.test.util._
import edu.chop.cbmi.dataExpress.dataModels._
import edu.chop.cbmi.dataExpress.dataModels.sql._
import edu.chop.cbmi.dataExpress.dataModels.sql.IntegerDataType
import scala.language.reflectiveCalls


class SqlServerBackendFeatureSpec extends FeatureSpec with GivenWhenThen with ShouldMatchers {

  def fixture =
    new {
      val props = TestProps.getDbProps("sqlserver")
    }

  val identifierQuote = "`"

  def dataSetupFixture =
    new {
      val tf = fixture
      val targetBackend = new SqlServerBackend(tf.props)
      targetBackend.connect
      val targetConnection = targetBackend.connection
      val targetStatement = targetConnection.createStatement()
    }

  val setup = dataSetupFixture


  def removeTestDataSetup: Boolean = {
    setup.targetStatement.execute("DROP TABLE cars_deba_a")
    setup.targetBackend.commit
    true
  }


  scenario("The user can create a table with four columns") {
    val f = fixture
    val tableName = "cars_deba_a"
    val columnFixedWidth: Boolean = false
    val columnNames: List[String] = List("carid", "carnumber", "carmake", "carmodel")
    val dataTypes = List(CharacterDataType(20, columnFixedWidth), IntegerDataType, CharacterDataType(20, columnFixedWidth), CharacterDataType(20, columnFixedWidth))
    val verifyTableStatement: String = "SELECT COUNT(*) as 'count' FROM sys.tables WHERE name = '%s'".format(tableName)
    val backend = new SqlServerBackend(f.props)
    val cascade: Boolean = true

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("the user issues a valid create table instruction for a table that does not exist")

    val startTableCount = backend.executeQuery(verifyTableStatement)
    assert(startTableCount.next())

    if (startTableCount.getInt("count") != 0) {
      backend.dropTable(tableName,cascade)
    }

    startTableCount.close()

    val endTableCount = backend.executeQuery(verifyTableStatement)
    assert(endTableCount.next())
    if (endTableCount.getInt("count") != 0){
      fail( "Unable to drop existing table " + tableName )
    }


    /* Table should be dropped now if it existed) */

    backend.createTable(tableName,columnNames,dataTypes)
    Then("the table should exist")
    val tableExistResult = backend.executeQuery(verifyTableStatement)
    assert(tableExistResult.next())
    tableExistResult.getInt("count") should equal(1)
    backend.commit()
    backend.close()
  }



  scenario("The user can truncate a table and commit") {
    val f = fixture
    val tableName: String = "cars_deba_a"
    val countStatement: String = """select count(*) as 'count' from """ + tableName
    val backend = new SqlServerBackend(f.props)

    Given("an active connection and a populated table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)


    When("the user issues truncate and Then commit instructions for that table")
    backend.truncateTable(tableName)
    backend.commit()

    Then("the table should be truncated")
    val countResult = backend.executeQuery(countStatement)
    assert(countResult.next())
    countResult.getInt("count") should equal (0)
    backend.close()

  }



  //TODO: This test needs to be re-written with an auto-incrementing sequence in the table to fully test insert returning keys
  scenario("The inserted row can be committed") {
    val f = fixture
    val backend = new SqlServerBackend(f.props)
    val tableName = "cars_deba_a"
    val columnNames:List[String] = List("carid","carnumber","carmake","carmodel")
    val valuesHolders:List[String] = for (i <- (0 to (columnNames.length - 1)).toList) yield "?"
    val sqlStatement = """insert into %s(%s) values(%s)""".format(tableName,
      columnNames.mkString(", "),
      valuesHolders.mkString(", "))
    val carId = "K0000001"
    val carNumber = 1234567890
    val carMake = "MiniCoopeRa"
    val carModel = "One"
    val valuesList = List(carId,carNumber,carMake,carModel)

    val bindVars:DataRow[Any] = DataRow((columnNames(0),valuesList(0)),
      (columnNames(1),valuesList(1)),
      (columnNames(2),valuesList(2)),
      (columnNames(3),valuesList(3)))

    var isDataRow = false

    var insertedRow:DataRow[Any] = DataRow.empty

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("an insert query is executed and committed")
    backend.execute(sqlStatement,bindVars)
    backend.commit()

    Then("the inserted row should be in the database")
    val rs = backend.executeQuery("select count(*) from %s where %s = ?".format(tableName, columnNames(0)), List(Option(carId)))
    rs.next
    rs.getInt(1) should equal(1)
    backend.close()
  }



  scenario("The user can obtain a record from executing a select query") {
    //Prerequisites:  ignore 1:  Passed

    val f = fixture
    val backend = new SqlServerBackend(f.props)
    val tableName = "cars_deba_a"
    val sqlStatement = "select * from " + tableName + " where carid  = ?"
    val valuesList: List[String] = List("K0000001")
    val columnNames: List[String] = List("carid")
    val bindVars: DataRow[String] = DataRow((columnNames(0), valuesList(0)))
    var hasResults: Boolean = false

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection])
    backend.connection.setAutoCommit(false)

    When("a query that should generate results is executed")
    val resultSet = backend.executeQuery(sqlStatement, bindVars)

    Then("one or more results should be returned")
    hasResults = resultSet.next()
    hasResults should be(true)

    backend.close()
  }

  scenario("The user can determine whether a select query has returned a record") {

    //Prerequisites:  ignore 1:  Passed

    val f = fixture
    val backend  = new SqlServerBackend(f.props)
    val tableName = "cars_deba_a"
    val sqlStatement = "select * from %s where carid  = ?".format(tableName)
    val columnNames = List("carid")
    val valuesList = List("K0000001")
    val bindVars:DataRow[String] = DataRow((columnNames(0),valuesList(0)))

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("a select query that has a non empty result is executed")
    //resultSetReturned                             = backend.execute(sqlStatement,bindVars)
    //resultSetReturned seems to only be true only if it is an update count or if execute does not return anything at all
    val results = backend.executeQuery(sqlStatement, bindVars)
    Then("the query should have returned a non empty result set")
    val nonEmptyResultSet: Boolean = results.next()
    nonEmptyResultSet should be(true)
    backend.close()


  }


  scenario("The user can commit an open transaction") {

    val f = fixture
    var backend = new SqlServerBackend(f.props)
    val tableName  = "cars_deba_a"
    val columnNames = List("carid","carnumber","carmake","carmodel")
    val valuesHolders = for (i <- (0 to (columnNames.length - 1)).toList ) yield "?"
    val sqlStatement = """insert into %s(%s) values(%s)""".format(tableName,
      columnNames.mkString(", "),
      valuesHolders.mkString(", "))
    val carId = "K0000002"
    val carNumber = 1234567899
    val carMake = "MiniCoopeRa"
    val carModel = "Two"
    val valuesList = List(carId, carNumber, carMake, carModel)
    val bindVars: DataRow[Any] = DataRow(("carid", carId), ("carnumber", carNumber), ("carmake", carMake), ("carmodel", carModel))

    Given("an active connection with an open transaction ")
    assert(backend.connect().isInstanceOf[java.sql.Connection])
    backend.connection.setAutoCommit(false)
    backend.startTransaction()
    backend.execute(sqlStatement, bindVars)
    When("the user issues a commit instruction")
    backend.commit()
    Then("the data should be persisted")
    backend.close()
    backend.connection.isClosed should be (true)

    val confirmSqlStatement  = """select * from %s
      					          where carid = ?
    							  and carnumber = ?
                                  and carmake = ?
                                  and carmodel = ?""".format(tableName)

    val newFixture = fixture
    backend = new SqlServerBackend(newFixture.props)
    assert(backend.connect().isInstanceOf[java.sql.Connection])
    backend.execute(confirmSqlStatement, bindVars) should be(true)
    backend.close()
  }


  scenario("The user can truncate a populated table") {
    val f = fixture
    val tableName  = "cars_deba_a"
    val countStatement = "select count(1) as 'count' from %s".format(tableName)
    val backend = new SqlServerBackend(f.props)

    Given("an active connection and a populated table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    var countResult = backend.executeQuery(countStatement)
    countResult.next() should  be (true)
    countResult.getInt("count") should be > (0)

    When("the user issues a truncate table instruction for that table")
    backend.truncateTable(tableName)

    Then("the table should be truncated")
    countResult = backend.executeQuery(countStatement)
    assert(countResult.next())
    countResult.getInt("count") should equal(0)

    backend.close()

  }


  scenario("The user can roll back an open transaction") {
    val f = fixture
    val backend = new SqlServerBackend(f.props)
    val tableName = "cars_deba_a"
    val columnNames: List[String] = List("carid", "carnumber", "carmake", "carmodel")
    val valuesHolders: List[String] = for (i <- (0 to (columnNames.length - 1)).toList) yield "?"
    val sqlStatement = """insert into %s(%s) values(%s)""".format(tableName,
      columnNames.mkString(", "),
      valuesHolders.mkString(", "))

    val carId = "K0000050"
    val carNumber = 1234567777
    val carMake = "MiniCoopeRa"
    val carModel = "Fifty"
    val valuesList = List(carId, carNumber, carMake, carModel)
    val bindVars: DataRow[Any] = DataRow(("carid", carId), ("carnumber", carNumber), ("carmake", carMake), ("carmodel", carModel))

    Given("an active connection with an open transaction ")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    backend.startTransaction()
    backend.execute(sqlStatement,bindVars)
    //can't depend on the row coming back
    //assert(insertedRow.isInstanceOf[DataRow[Any]])

    When("the user issues a rollback instruction")
    backend.rollback()

    Then("the data should not be persisted")
    backend.close()
    backend.connection.isClosed should be(true)
    val sqlVerifyStatement = """select count(*) as count from %s
      										  where carid = ?
                                                and carnumber = ?
                                                and carmake = ?
                                                and carmodel = ?""".format(tableName)
    val newFixture = fixture
    val newBackend = new SqlServerBackend(newFixture.props)
    assert(newBackend.connect().isInstanceOf[java.sql.Connection])
    val persistentDataCount = newBackend.executeQuery(sqlVerifyStatement, bindVars)
    assert(persistentDataCount.next())
    persistentDataCount.getInt("count") should equal(0)
    newBackend.close()

  }


  scenario("The user can open a transaction, insert a row, and end the transaction") {

    val f = fixture
    val backend = new SqlServerBackend(f.props)
    val tableName  = "cars_deba_a"
    val columnNames = List("carid","carnumber","carmake","carmodel")
    val valuesHolders = for (i <- (0 to (columnNames.length - 1)).toList ) yield "?"
    val sqlStatement = """insert into %s(%s) values(%s)""".format(tableName,
      columnNames.mkString(", "),
      valuesHolders.mkString(", "))

    val carId = "K0000055"
    val carNumber = 1234567755
    val carMake = "MiniCoopeRa"
    val carModel = "FiftyFive"
    val valuesList = List(carId,carNumber,carMake,carModel)
    val bindVars:DataRow[Any] = DataRow((columnNames(0),valuesList(0)),
      (columnNames(1),valuesList(1)),
      (columnNames(2),valuesList(2)),
      (columnNames(3),valuesList(3)))




    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    When("the user issues a start transaction instruction")
    backend.startTransaction()
    And("the user inserts a row")
    backend.execute(sqlStatement,bindVars)
    And("the user ends the transaction")
    backend.endTransaction()
    backend.commit()
    Then("the data should be persisted")
    backend.close()
    backend.connection.isClosed should be (true)
    val sqlVerifyStatement = """select count(*) as count from %s
      										  where carid = ?
                                                and carnumber = ?
                                                and carmake = ?
                                                and carmodel = ?""".format(tableName)

    val newFixture = fixture
    val newBackend = new SqlServerBackend(newFixture.props)
    assert(newBackend.connect().isInstanceOf[java.sql.Connection] )
    val persistentDataCount = newBackend.executeQuery(sqlVerifyStatement,bindVars)
    assert(persistentDataCount.next() )
    persistentDataCount.getInt("count") should equal (1)
    newBackend.close()

  }


  scenario("The user can create a table with 32 columns") {
    val f = fixture
    val tableName                             =     "cars_deba_b"
    val columnFixedWidth:Boolean              =     false
    val columnNames:List[String]              =     List("carid","carnumber","carmake","carmodel")
    val dataTypes                             =     List( CharacterDataType(20,columnFixedWidth),IntegerDataType,CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),
      CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),
      CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),
      CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),
      CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),
      CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),
      CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),
      CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth)
    )

    val verifyTableStatement: String = "SELECT COUNT(*) as 'count' FROM sys.tables WHERE name = '%s'".format(tableName)
    val backend = new SqlServerBackend(f.props)

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("the user issues a valid create table instruction")
    backend.createTable(tableName,columnNames,dataTypes)

    Then("the table should exist")
    val tableExistResult  =  backend.executeQuery(verifyTableStatement)
    assert(tableExistResult.next())
    tableExistResult.getInt("count") should equal (1)


    backend.close()
  }


  scenario("The user can insert a row without constructing an insert statement") {
    val f = fixture
    val tableName = "cars_deba_a"
    val columnNames = List("carid","carnumber","carmake","carmodel")
    val carId = "K0000003"
    val carNumber  = 1234567888
    val carMake = "MiniCoopeRa"
    val carModel = "Three"
    val valuesList = List(carId,carNumber,carMake,carModel)
    val row = DataRow(("carid",carId),("carnumber",carNumber),("carmake",carMake),("carmodel",carModel))
    val backend = new SqlServerBackend(f.props)
    val verifyRecordStatement = "select count(*) as count from %s where carid = '%s'".format(tableName, row.carid.get)

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("the user issues a valid insert command for an existing table and a unique record")
    var recordCountResult = backend.executeQuery(verifyRecordStatement)
    assert(recordCountResult.next())
    var recordCount = recordCountResult.getInt("count")
    recordCount should be (0)
    backend.insertRow(tableName,row)


    And("the row should be inserted")
    recordCountResult = backend.executeQuery(verifyRecordStatement)
    assert(recordCountResult.next())
    recordCount = recordCountResult.getInt("count")
    recordCount  should be (1)

    backend.commit()

    backend.close()

  }

  scenario("The user can insert a batch of rows and commit without having to construct the insert statements") {
    val f = fixture
    val tableName = "cars_deba_a"
    val columnNames = List("carid","carnumber","carmake","carmodel")

    val rows = Seq(Seq("K0000201",1234901,"MiniCoopeRb","One"),
      Seq("K0000202",1234902,"MiniCoopeRb","Two"),
      Seq("K0000203",1234903,"MiniCoopeRb","Three"),
      Seq("K0000204",1234904,"MiniCoopeRb","Four"),
      Seq("K0000205",1234905,"MiniCoopeRb","Five"),
      Seq("K0000206",1234906,"MiniCoopeRb","Six"),
      Seq("K0000207",1234907,"MiniCoopeRb","Seven"),
      Seq("K0000208",1234908,"MiniCoopeRb","Eight"),
      Seq("K0000209",1234909,"MiniCoopeRb","Nine"),
      Seq("K0000210",1234910,"MiniCoopeRb","Ten"))
    val table = DataTable(columnNames, rows: _*)
    val backend = new SqlServerBackend(f.props)
    var successfulStatementCount = 0
    val verifyRowsStatement = "select count(*) as count from %s where carid in (%s)".format(tableName, rows.map{r => "'%s'".format(r.head)}.mkString(", ") )

    Given("an active connection and an empty table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    backend.truncateTable(tableName)

    When("the user issues a batch insert command (with commit) to insert multiple rows into the table ")
    successfulStatementCount = backend.batchInsert(tableName, table)
    backend.commit()

    Then("the batch insert command should be successful")
    successfulStatementCount  should equal  (rows.length)

    And("the rows should be inserted")
    val recordCountResult = backend.executeQuery(verifyRowsStatement)
    assert(recordCountResult.next())
    val recordCount = recordCountResult.getInt("count")
    recordCount should be (10)
    backend.close()
  }

  scenario("The user can drop a table") {
    val f = fixture
    val tableName = "cars_deba_c"
    val columnFixedWidth = false
    val columnNames = List("carid","carnumber","carmake","carmodel")
    val dataTypes  = List(CharacterDataType(20,columnFixedWidth),
      IntegerDataType,
      CharacterDataType(20,columnFixedWidth),
      CharacterDataType(20,columnFixedWidth))
    val verifyTableStatement: String = "SELECT COUNT(*) as 'count' FROM sys.tables WHERE name = '%s'".format(tableName)
    val backend = new SqlServerBackend(f.props)

    Given("an active connection and an existing table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    backend.createTable(tableName,columnNames,dataTypes)
    val tableVerifiedResult = backend.executeQuery(verifyTableStatement)
    assert(tableVerifiedResult.next())
    tableVerifiedResult.getInt("count") should be (1)
    tableVerifiedResult.close()

    When("the user issues a drop table command for that table")
    backend.dropTable(tableName)

    Then("the table should be dropped")
    val tableExistResult  = backend.executeQuery(verifyTableStatement)
    assert(tableExistResult.next())
    tableExistResult.getInt("count") should be (0)
    backend.close()

  }

  //Cascade on drop table is unsupported in sql server
  scenario("The user gets an exception when trying to drop a table with cascade") {
    val f = fixture
    val tableName = "cars_deba_c"
    val viewName = "cars_deba_c_v"
    val columnFixedWidth = false
    val columnNames = List("carid", "carnumber", "carmake", "carmodel")
    val dataTypes = List(CharacterDataType(20, columnFixedWidth),
      IntegerDataType,
      CharacterDataType(20, columnFixedWidth),
      CharacterDataType(20, columnFixedWidth))
    val verifyTableStatement = "SELECT COUNT(*) as 'count' FROM sys.tables WHERE name = '%s'".format(tableName)
    val backend = new SqlServerBackend(f.props)
    val cascade = true

    Given("an active connection, an existing table, and a view on the existing table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    backend.createTable(tableName,columnNames,dataTypes)

    val tableVerifiedResult = backend.executeQuery(verifyTableStatement)
    assert(tableVerifiedResult.next())
    tableVerifiedResult.getInt("count") should equal(1)

    When("the user issues a drop table command with cascade for that table")
    Then("A runtime exception should be thrown")
    intercept[RuntimeException] {
      backend.dropTable(tableName, cascade)
    }
    backend.close()

  }

  scenario("The user can iterate over the results of a select query") {
    //Prerequisites:  Need Multiple Row in table cars_deba_a
    val f = fixture
    val backend = new SqlServerBackend(f.props)
    val tableName = "cars_deba_a"
    val sqlStatement = "select * from %s".format(tableName)
    val bindVars: DataRow[String] = DataRow.empty

    var resultsCount: Int = 0

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("a query that should generate multiple results is executed")
    val resultSet = backend.executeQuery(sqlStatement,bindVars)

    Then("the user should be able to iterate over the results")
    while (resultSet.next()) { resultsCount+=1 }

    And("multiple results should be returned")
    resultsCount should be > (1)

    backend.close()
  }

  scenario("The user can update a record in a table using a valid update statement") {
    //Prerequisites:  Need Multiple Row in table cars_deba_a

    val f = fixture
    val backend = new SqlServerBackend(f.props)
    val tableName = "cars_deba_a"
    val columnNames = List("carid", "carnumber", "carmake", "carmodel")
    val carId = "K0000210"
    val carNumber = 1234567899
    val carMake = "MiniCoopeRa"
    val carModel = "FourteenMillion"
    val sqlStatement = "update %s set %s where carid = '%s'".format(tableName,
      columnNames.map("%s = ?".format(_)).mkString(", "),
      carId)
    val valuesList = List(carId, carNumber, carMake, carModel, carId)
    val bindVars = DataRow(("carid", carId),
      ("carnumber", carNumber),
      ("carmake", carMake),
      ("carmodel", carModel))
    var resultsCount: Int = 0

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    When("an update query is executed")
    backend.execute(sqlStatement,bindVars)

    Then("the record(s) should be updated")
    val sqlVerifyStatement = "select count(*) as count from %s where carmodel = ?".format(tableName)

    val recordCountResult = backend.executeQuery(sqlVerifyStatement, Seq(Some(carModel)))
    assert(recordCountResult.next())
    recordCountResult.getInt("count") should be(1)
    backend.close()
  }


  scenario("The user can update a multiple records in a table using a valid update statement") {
    //Prerequisites:  Need Multiple Row in table cars_deba_a

    val f = fixture
    val backend = new SqlServerBackend(f.props)
    val tableName = "cars_deba_a"
    val columnNames = List("carmodel")
    val sqlStatement = "update %s set %s".format(tableName,
      columnNames.map("%s = ?".format(_)).mkString(", "))

    val carModel = "SeventeenMillion"
    val valuesList = List(carModel)
    val bindVars = DataRow((columnNames(0), carModel))

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection])
    backend.connection.setAutoCommit(false)

    When("an update query for multiple records is executed")
    backend.execute(sqlStatement, bindVars)

    Then("multiple record(s) should be updated")
    val sqlVerifyStatement = "select count(*) as count from %s where carmodel = ?".format(tableName)

    val recordCountResult = backend.executeQuery(sqlVerifyStatement, bindVars)
    assert(recordCountResult.next())
    recordCountResult.getInt("count") should be > (1)
    //It would be better to compare the number of rows updated to the count  i.e.:
    //http://www.coderanch.com/t/426288/JDBC/java/Row-count-update-statement
    //Statement st = connection.createStatement("update t_number set number = 2 where name='abcd");
    //int rowCount = st.executeUpdate();
    //However, this executeUpdate is not yet available on the backend

    backend.close()
  }


  scenario("The user can update a multiple records in a table without constructing update statement") {
    //Prerequisites:  Need Multiple Row in table cars_deba_a   with carmake = 'MiniCoopeRb'

    val f = fixture
    val backend = new SqlServerBackend(f.props)
    val tableName = "cars_deba_a"
    var columnNames = List("carnumber", "carmake", "carmodel")
    val carNumber = 192837465
    val carMake = "MiniCoopeRaStyle004"
    val carModel = "SeventeenMillion"
    val valuesList = List(carNumber, carMake, carModel)
    val filter = List(("carmake", "MiniCoopeRb"))
    val updatesBindVars = DataRow((columnNames(0),valuesList(0)),
      (columnNames(1),valuesList(1)),
      (columnNames(2),valuesList(2)))

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("an update row instruction for multiple records is executed")
    backend.updateRow(tableName,updatesBindVars,filter)

    Then("multiple record(s) should be updated")
    val sqlStatement = "select count(*) as count from %s where carmake = ?".format(tableName)

    val bindVars = DataRow((columnNames(1), valuesList(1)))

    val recordCountResult = backend.executeQuery(sqlStatement, bindVars)
    assert(recordCountResult.next())
    recordCountResult.getInt("count") should be > (1)
    //It would be better to compare the number of rows updated to the count  i.e.:
    //http://www.coderanch.com/t/426288/JDBC/java/Row-count-update-statement
    //Statement st = connection.createStatement("update t_number set number = 2 where name='abcd");
    //int rowCount = st.executeUpdate();
    //However, this executeUpdate is not yet available on the backend
    backend.close()
  }


  scenario("The user can insert a multiple rows using a loop without constructing an insert statement") {
    //Prerequisites:  None of theses record should exist
    val f = fixture
    val tableName = "cars_deba_a"
    val columnNames = List("carid", "carnumber", "carmake", "carmodel")
    val carIds = List("K0000500", "K0000501", "K0000502", "K0000503", "K0000504")
    val carNumbers = List(1234561000, 1234561001, 1234561002, 1234561003, 1234561004)
    val carMakes = List("MiniCoopeRd", "MiniCoopeRd", "MiniCoopeRd", "MiniCoopeRd", "MiniCoopeRd")
    val carModels = List("Zero", "One", "Ten", "Ten", "Ten")
    val backend = new SqlServerBackend(f.props)

    val verifyRecordsStatement = """select count(*) as count
      								from %s where carid in (%s)""".format(tableName, carIds.map("'%s'".format(_)).mkString(", "))

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("the user issues valid insert row commands in a loop for an existing table")
    carIds.zipWithIndex.foreach{
      case (carId,index) => backend.insertRow(tableName, DataRow(("carid", carId),
        ("carnumber", carNumbers(index)),
        ("carmake", carMakes(index)),
        ("carmodel", carModels(index))))
    }


    Then("the rows should be inserted")
    val recordCountResult = backend.executeQuery(verifyRecordsStatement)
    assert(recordCountResult.next())
    recordCountResult.getInt("count") should equal(carIds.length)
    backend.close()

  }



  scenario("Remove Test Data Setup")  {
    /**** Remove Test Data    ****/
    removeTestDataSetup
    /****                     ****/

  }

  scenario("Close Test SetUp Connections")  {
    setup.targetBackend.close
  }





}

