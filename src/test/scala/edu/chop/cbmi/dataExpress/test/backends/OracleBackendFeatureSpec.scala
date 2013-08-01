package edu.chop.cbmi.dataExpress.test.backends

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 11/22/11
 * Time: 1:22 PM
 * To change this template use File | Settings | File Templates.
 */



import org.scalatest.{FeatureSpec}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.GivenWhenThen
import edu.chop.cbmi.dataExpress.test.util._
import edu.chop.cbmi.dataExpress.dataModels._
import edu.chop.cbmi.dataExpress.dataModels.sql._
import edu.chop.cbmi.dataExpress.backends.OracleBackend
import scala.language.reflectiveCalls
import edu.chop.cbmi.dataExpress.test.util.cars.dataSetup.backends.OracleDataSetup

class OracleBackendFeatureSpec extends FeatureSpec with GivenWhenThen with ShouldMatchers {

   def fixture =
    new {
	  val props = TestProps.getDbProps("oracle")
    }
   
  //TODO: Hard-coding schema names and users in the tests like this is confusing. It's not clear how they relate to the properties file
  //This stuff all needs to be re-worked
  val dbSchema:Option[String] = Some("QE10C01")
  val targetDbUserName:String = "QE10C01"




  def dataSetupFixture =
    new {
      val tf                            =       fixture
      val targetBackend                 =       new OracleBackend(tf.props)
      targetBackend.connect
      val targetConnection              =       targetBackend.connection
      val targetStatement               =       targetConnection.createStatement()
      val dataSetup                     =       new OracleDataSetup()
      dataSetup.targetDBSchemaName      =       dbSchema.get
      dataSetup.targetDbUserName        =       targetDbUserName
    }


  val setup                             =       dataSetupFixture

  def setUpTestData: Boolean   = {
    for ((statementName, statement) <- setup.dataSetup.createTargetSchema) {
      setup.targetStatement.execute(statement)
    }
    setup.targetBackend.commit
    true
  }


  def removeTestDataSetup: Boolean = {
    setup.targetStatement.execute(setup.dataSetup.dropTargetSchema)
    setup.targetBackend.commit
    true
  }

  scenario("Data Setup", OracleTest)  {
    /**** SetUp Test Data   ****/
    setUpTestData
    /****                   ****/

  }

  scenario("The user can create a table with four columns", OracleTest) {

    val f = fixture
    val tableName = "cars_deba_a"
    val columnFixedWidth: Boolean = false
    val columnNames: List[String] = List("carid", "carnumber", "carmake", "carmodel")
    val dataTypes = List(CharacterDataType(20, columnFixedWidth), IntegerDataType, CharacterDataType(20, columnFixedWidth), CharacterDataType(20, columnFixedWidth))
    val verifyTableStatement: String =
      "SELECT count(*) as count FROM dba_tables WHERE upper(table_name) = '%s' AND upper(owner) = '%s'".format(tableName.toUpperCase(), targetDbUserName.toUpperCase())

    val backend = new OracleBackend(f.props)

    val cascade: Boolean = true

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    
    When("the user issues a valid create table instruction for a table that does not exist")
    try {
      var tableExistResult                    =   backend.executeQuery(verifyTableStatement)

      assert(tableExistResult.next())

      if (tableExistResult.getInt("count") != 0)
      {
        try  {
                    println("dropping table")
          backend.dropTable(tableName,cascade,dbSchema)
          println("dropped table")
        }
        catch {
        case e:java.sql.SQLException =>
            println(e.getMessage + "\n")
            fail( "backend.dropTable(" + "\"" + tableName + "," + dbSchema.get + "\"" + ")produced java.sql.SQLException" +
                  "when attempting to drop existing table" )
        }

      tableExistResult.close()

      tableExistResult                      =     backend.executeQuery(verifyTableStatement)
      assert(tableExistResult.next())

          if (tableExistResult.getInt("count") != 0)
          {

            fail( "Unable to drop existing table " + dbSchema.get   +   "."   + tableName )
          }


      }
    }
    catch {
    case e:java.sql.SQLException =>
            println(e.getMessage + "\n")
            fail("backend.executeQuery(" + verifyTableStatement + ")produced java.sql.SQLException" )
    }

    /* Table should be dropped now if it existed) */

    backend.createTable(tableName,columnNames,dataTypes, schemaName = dbSchema)
    backend.commit()
    Then("the table should exist")
    val tableExistResult                          =     backend.executeQuery(verifyTableStatement)
    assert(tableExistResult.next())
    tableExistResult.getInt("count") should equal (1)


    backend.close()
  }

  scenario("The user can truncate a table and commit", OracleTest) {
     val f = fixture

     val tableName:String                            =     "cars_deba_a"

     val countStatement:String                       =     "select count(1) as count from "  + dbSchema.get + "."  + tableName

     val backend                                     =     new OracleBackend(f.props)

     Given("an active connection and a populated table")
     assert(backend.connect().isInstanceOf[java.sql.Connection] )
     backend.connection.setAutoCommit(false)


     When("the user issues truncate and then commit instructions for that table")
     try
       backend.truncateTable(tableName, schemaName = dbSchema)
     catch {
     case e:java.sql.SQLException =>
             fail("backend.truncateTable(" + "\"" + tableName + "," + dbSchema.get + "\"" + ")produced java.sql.SQLException" )
     }

     try
       backend.commit()
     catch {
     case e:java.sql.SQLException =>
             fail("backend.commit()produced java.sql.SQLException" )
     }

     Then("the table should be truncated")
     val countResult                                 =     backend.executeQuery(countStatement)
     assert(countResult.next())
     countResult.getInt("count") should equal (0)

     backend.close()

   }

  scenario("The inserted row can be committed by executing an insert query", OracleTest) {
    val f = fixture
    val backend = new OracleBackend(f.props)
    val tableName = "cars_deba_a"
    val columnNames: List[String] = List("carid", "carnumber", "carmake", "carmodel")
    val valuesHolders: List[String] = for (i <- (0 to (columnNames.length - 1)).toList) yield "?"
    val sqlStatement = """insert into %s.%s(%s) values(%s)""".format(dbSchema.get,
    																 tableName, 
                           											 columnNames.mkString(", "), 
                           											 valuesHolders.mkString(", "))

                 											 
    val carId = "K0000001"
    val carNumber = 1234567890
    val carMake = "MiniCoopeRa"
    val carModel = "One"
    val verifySqlStatement = """select count(*) from %s.%s where carid = '%s'""".format(dbSchema.get, tableName, carId)       
    val valuesList = List(carId, carNumber, carMake, carModel)
    val bindVars: DataRow[Any] =
      DataRow((columnNames(0), valuesList(0)), (columnNames(1), valuesList(1)), (columnNames(2), valuesList(2)), (columnNames(3), valuesList(3)))
    var isDataRow: Boolean = false
    var insertedRow:DataRow[Any]        = DataRow.empty

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    
    
    When("an insert query is executed and committed")
      backend.execute(sqlStatement,bindVars)
      backend.commit()

      Then("the insert should be successful")
      val verifyResults = backend.executeQuery(verifySqlStatement)
      verifyResults.next
      verifyResults.getInt(1) should be(1)
      verifyResults.close()
      backend.close()
  }

  scenario("The user can obtain a record from executing a select query", OracleTest) {
    //Prerequisites:  scenario 1:  Passed

    val f                     = fixture

    val backend               = new OracleBackend(f.props)

    val tableName             = "cars_deba_a"

    val sqlStatement          = "select * from " + dbSchema.get + "." + tableName + " where carid = ?"

    val valuesList:List[String]                   = List("K0000001")

    val columnNames:List[String]                  = List("carid")

    val bindVars:DataRow[String]                  = DataRow((columnNames(0),valuesList(0)))

    var hasResults:Boolean                        = false

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("a query that should generate results is executed")

    val resultSet = backend.executeQuery(sqlStatement,bindVars)

    Then("one or more results should be returned")
    hasResults                                    = resultSet.next()
    hasResults should be (true)

    backend.close()
  }
  //THIS SHOULD WORK
  scenario("The user can determine whether a select query has returned a record", OracleTest) {
    //Prerequisites:  scenario 1:  Passed

    val f             =   fixture

    val backend       =   new OracleBackend(f.props)

    val tableName     =   "cars_deba_a"

    val sqlStatement  =   "select * from " + dbSchema.get + "." + tableName + " where carid = ?"

    val columnNames                               = List("carid")

    val valuesList                                = List("K0000001")

    val bindVars:DataRow[String]                  = DataRow((columnNames(0),valuesList(0)))

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("a select query that has a non empty result is executed")
    //resultSetReturned                             = backend.execute(sqlStatement,bindVars)
    //resultSetReturned seems to only be true only if it is an update count or if execute does not return anything at all
    try {
          val results =  backend.executeQuery(sqlStatement,bindVars)
          Then("the query should have returned a non empty result set")
          val nonEmptyResultSet:Boolean                 =   results.next()
          nonEmptyResultSet should be (true)
    }
    catch {
    case  e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n" + sqlStatement )
    }


    backend.close()


  }

  scenario("The user can commit an open transaction", OracleTest) {

    val f                             = fixture

    var backend                       = new OracleBackend(f.props)

    val tableName                     = "cars_deba_a"

    val columnNames:List[String]      = List("carid","carnumber","carmake","carmodel")

    val valuesHolders:List[String]    = for (i <- (0 to (columnNames.length - 1)).toList ) yield "?"

    var sqlStatement = "insert into " + dbSchema.get + "." + tableName + "(" +
      columnNames.map(i => i + ",").mkString.dropRight(1) +")" +
      " values (" + valuesHolders.map(i => i + ",").mkString.dropRight(1) + ")"

    val carId:String              = "K0000002"

    val carNumber:Int                       = 1234567899

    val carMake                     = "MiniCoopeRa"

    val carModel                      = "Two"

    val valuesList                    = List(carId,carNumber,carMake,carModel)

    val bindVars:DataRow[Any]         = DataRow(("carid",carId),("carnumber",carNumber),("carmake",carMake),("carmodel",carModel))

    var committed:Boolean             = false

    var connectionClosed:Boolean      = false



    Given("an active connection with an open transaction ")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    //In Oracle a transaction is started automatically with the first statement
    try   {
      val insertedRow                   = backend.executeReturningKeys(sqlStatement,bindVars)

    }
    catch  {
    case e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n" + sqlStatement + "\n")
    }

    When("the user issues a commit instruction")
    try
      backend.commit()
    catch {
    case e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n" + sqlStatement )
    }

    Then("the data should be persisted")
    backend.close()
    connectionClosed                    = backend.connection.isClosed
    connectionClosed  should be (true)
    sqlStatement                        = """select count(*) as count 
      										 from %s.%s 
      										 where carid = ? and carnumber = ? and carmake = ? and carmodel = ?""".format(dbSchema.get,tableName)
                       
    val newFixture                      = fixture
    backend                             = new OracleBackend(newFixture.props)
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    val dataPersistentCountResult       = backend.executeQuery(sqlStatement,bindVars)
    assert(dataPersistentCountResult.next())
    dataPersistentCountResult.getInt("count") should equal (1)

    backend.close()

  }

  scenario("The user can truncate a populated table", OracleTest) {
    val f = fixture

    val tableName:String                            =     "cars_deba_a"

    val countStatement:String                       =     "select count(*) as count from %s.%s".format(dbSchema.get,tableName)

    val backend                                     =     new OracleBackend(f.props)

    Given("an active connection and a populated table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    var countResult                                 =     backend.executeQuery(countStatement)
    countResult.next() should  be (true)
    countResult.getInt("count") should be > (0)

    When("the user issues a truncate table instruction for that table")
    try
      backend.truncateTable(tableName, schemaName = dbSchema)
    catch {
    case e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n")
    }

    Then("the table should be truncated")
    countResult                                     =     backend.executeQuery(countStatement)
    assert(countResult.next())
    countResult.getInt("count") should equal (0)

    backend.close()

  }

  scenario("The user can roll back an open transaction", OracleTest) {

    val f                             = fixture

    val backend                       = new OracleBackend(f.props)

    val tableName                     = "cars_deba_a"

    val columnNames:List[String]      = List("carid","carnumber","carmake","carmodel")

    val valuesHolders:List[String]    = for (i <- (0 to (columnNames.length - 1)).toList ) yield "?"

    var sqlStatement                  = "insert into %s.%s (%s) values(%s)".format(dbSchema.get, 
    																		tableName, 
    																		columnNames.mkString(","), 
    																		valuesHolders.mkString(","))


    val carId:String              = "K0000050"
    val carNumber:Int                       = 1234567777
    val carMake                     = "MiniCoopeRa"
    val carModel                      = "Fifty"
    val valuesList                    = List(carId,carNumber,carMake,carModel)
    val bindVars:DataRow[Any]         = DataRow(("carid",carId),("carnumber",carNumber),("carmake",carMake),("carmodel",carModel))
    var connectionClosed:Boolean      = false
    var dataPersistent:Boolean        = false

    Given("an active connection with an open transaction ")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    //In Oracle (11g) - Transaction is automatically started with the first statement
    val insertedRow                   = backend.executeReturningKeys(sqlStatement,bindVars)
    assert(insertedRow.isInstanceOf[DataRow[Any]])

    When("the user issues a roll back instruction")
    try
      backend.rollback()
    catch {
    case e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n" + sqlStatement )
    }

    Then("the data should not be persisted")
    backend.close()
    connectionClosed                  = backend.connection.isClosed
    connectionClosed  should be (true)
    sqlStatement                      = """select count(*) as count
      									   from %s.%s 
      									   where carid = ? and carnumber = ? and carmake = ? and carmodel = ? """.format(dbSchema.get,tableName)

    val newFixture                            = fixture
    val newBackend                            = new OracleBackend(newFixture.props)
    assert(newBackend.connect().isInstanceOf[java.sql.Connection] )
    val persistentDataCount                   = newBackend.executeQuery(sqlStatement,bindVars)
    assert(persistentDataCount.next() )
    persistentDataCount.getInt("count") should equal (0)
    newBackend.close()

  }

  scenario("The user can open a transaction, insert a row, and end the transaction", OracleTest) {

    val f                             = fixture

    val backend                       = new OracleBackend(f.props)

    val tableName                     = "cars_deba_a"

    val columnNames:List[String]      = List("carid","carnumber","carmake","carmodel")

    val valuesHolders:List[String]    = for (i <- (0 to (columnNames.length - 1)).toList ) yield "?"

    var sqlStatement                  = "insert into %s.%s (%s) values(%s)".format(dbSchema.get, 
    																		tableName, 
    																		columnNames.mkString(","), 
    																		valuesHolders.mkString(","))
    val carId:String              = "K0000055"

    val carNumber:Int                       = 1234567755

    val carMake                     = "MiniCoopeRa"

    val carModel                      = "FiftyFive"

    val valuesList                    = List(carId,carNumber,carMake,carModel)

    val bindVars:DataRow[Any]         = DataRow((columnNames(0),valuesList(0)),(columnNames(1),valuesList(1)),(columnNames(2),valuesList(2)),(columnNames(3),valuesList(3)))

    var connectionClosed:Boolean      = false



    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("the user starts a transaction by issuing a valid insert statement")
    try   {
            val insertedRow                   = backend.executeReturningKeys(sqlStatement,bindVars)
            assert(insertedRow.isInstanceOf[DataRow[Any]])
    }
    catch {
    case    e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n" + sqlStatement )
    }

    And("the user ends the transaction")
    try
            //backend.endTransaction()  Not supported in Oracle 11g
            //Must use either a DDL statement or commit to end transaction programttically in
            //Oracle 11g
            backend.commit()
    catch {
    case    e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n")
    }

    Then("the inserted data should be persisted")
    backend.close()
    connectionClosed                  = backend.connection.isClosed
    connectionClosed  should be (true)
    sqlStatement                      = """select count(*) as count
      									   from %s.%s 
      									   where carid = ? and carnumber = ? and carmake = ? and carmodel = ? """.format(dbSchema.get,tableName)

    val newFixture                            = fixture
    val newBackend                            = new OracleBackend(newFixture.props)
    assert(newBackend.connect().isInstanceOf[java.sql.Connection] )
    val persistentDataCount                   = newBackend.executeQuery(sqlStatement,bindVars)
    assert(persistentDataCount.next() )
    persistentDataCount.getInt("count") should equal (1)
    newBackend.close()

  }

  scenario("The user can create a table with 32 columns", OracleTest) {
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

    val verifyTableStatement = """SELECT count(*) as count 
      							    FROM dba_tables
      							   WHERE upper(table_name) = '%s' 
                                     AND upper(owner) = '%s'""".format(tableName.toUpperCase(), targetDbUserName.toUpperCase())


    val backend                               =     new OracleBackend(f.props)

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("the user issues a valid create table instruction")
    try
      backend.createTable(tableName,columnNames,dataTypes, schemaName = dbSchema)
    catch {
    case e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n")
    }


    Then("the table should exist")
    val tableExistResult                                      =     backend.executeQuery(verifyTableStatement)
    assert(tableExistResult.next())
    tableExistResult.getInt("count") should equal (1)


    backend.close()
  }

  scenario("The user can insert a row without constructing an insert statement", OracleTest) {
    val f = fixture
    val tableName: String = "cars_deba_a"
    val columnNames: List[String] = List("carid", "carnumber", "carmake", "carmodel")
    val carId: String = "K0000003"
    val carNumber: Int = 1234567888
    val carMake: String = "MiniCoopeRa"
    val carModel: String = "Three"
    val valuesList: List[Any] = List(carId, carNumber, carMake, carModel)
    val row: DataRow[Any] = DataRow(("carid", carId), ("carnumber", carNumber), ("carmake", carMake), ("carmodel", carModel))
    val backend = new OracleBackend(f.props)
    val verifyRecordStatement: String = "select count(*) as count from %s.%s where carid = '%s'".format(dbSchema.get, tableName, row.carid.get) 


    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("the user issues a valid insert command for an existing table and a unique record")
    var recordCountResult = backend.executeQuery(verifyRecordStatement)
    assert(recordCountResult.next())
    var recordCount = recordCountResult.getInt("count")
    recordCount should be(0)
    val insertedRow = backend.insertRow(tableName, row, dbSchema)

    And("the row should be inserted")
    recordCountResult = backend.executeQuery(verifyRecordStatement)
    assert(recordCountResult.next())
    recordCount = recordCountResult.getInt("count")
    recordCount should be(1)
    backend.commit()
    backend.close()

  }

  scenario("The user can insert a batch of rows and commit without having to construct the insert statements", OracleTest) {
    val f = fixture

    val tableName:String                      =     "cars_deba_a"

    val columnNames:List[String]              =     List("carid","carnumber","carmake","carmodel")

    val rowOne:List[Any]                      =     List("K0000201",1234901,"MiniCoopeRb","One")
    val rowTwo:List[Any]                      =     List("K0000202",1234902,"MiniCoopeRb","Two")
    val rowThree:List[Any]                    =     List("K0000203",1234903,"MiniCoopeRb","Three")
    val rowFour:List[Any]                     =     List("K0000204",1234904,"MiniCoopeRb","Four")
    val rowFive:List[Any]                     =     List("K0000205",1234905,"MiniCoopeRb","Five")
    val rowSix:List[Any]                      =     List("K0000206",1234906,"MiniCoopeRb","Six")
    val rowSeven:List[Any]                    =     List("K0000207",1234907,"MiniCoopeRb","Seven")
    val rowEight:List[Any]                    =     List("K0000208",1234908,"MiniCoopeRb","Eight")
    val rowNine:List[Any]                     =     List("K0000209",1234909,"MiniCoopeRb","Nine")
    val rowTen:List[Any]                      =     List("K0000210",1234910,"MiniCoopeRb","Ten")

    val rows                                  =     List(rowOne,rowTwo,rowThree,rowFour,rowFive,rowSix,rowSeven,rowEight,rowNine,rowTen)

    val table:DataTable[Any]                  =     DataTable(columnNames, rowOne,rowTwo,rowThree,rowFour,rowFive,rowSix,rowSeven,rowEight,rowNine,rowTen)

    val backend                               =     new OracleBackend(f.props)

    var successfulStatementCount:Int          =     0

    val verifyRowsStatement = "select count(*) as count from %s.%s where carid in (%s)".format(dbSchema.get, tableName, rows.map{r => "'%s'".format(r.head)}.mkString(", ") )

    Given("an active connection and an empty table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    try
      backend.truncateTable(tableName, dbSchema)
    catch {
    case e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n")
    }

    When("the user issues a batch insert command (with commit) to insert multiple rows into the table ")
    successfulStatementCount                      =     backend.batchInsert(tableName, table, dbSchema )
    try
      backend.commit()
    catch {
    case e:java.sql.SQLException =>
            fail("backend.commit() produced java.sql.SQLException" )
    }

    Then("the batch insert command should be successful")
    successfulStatementCount  should equal  (rows.length)


    And("the rows should be inserted")
    val recordCountResult                         =     backend.executeQuery(verifyRowsStatement)
    assert(recordCountResult.next())
    val recordCount                               =     recordCountResult.getInt("count")
    recordCount  should be (10)

    backend.close()



  }

  scenario("The user can drop a table", OracleTest) {
    val f = fixture

    val tableName:String                      =     "cars_deba_c"

    val columnFixedWidth:Boolean              =     false

    val columnNames:List[String]              =     List("carid","carnumber","carmake","carmodel")

    val dataTypes                             =     List(CharacterDataType(20,columnFixedWidth),IntegerDataType,CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth))

    val verifyTableStatement:String           =     "SELECT table_name FROM user_tables WHERE table_name = " + "'" + tableName   + "'"

    val backend                               =     new OracleBackend(f.props)

    var tableCreated:Boolean                  =     false

    var tableVerified:Boolean                 =     false

    var tableDropped:Boolean                  =     false

    Given("an active connection and an existing table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    try
      backend.createTable(tableName,columnNames,dataTypes, dbSchema)
    catch {
    case e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n")
    }
    tableVerified                             =     backend.execute(verifyTableStatement)
    tableVerified should be (true)

    When("the user issues a drop table command for that table")
    try
      backend.dropTable(tableName, schemaName = dbSchema)
    catch {
    case e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n")
    }


    Then("the table should be dropped")
    val tableExistResult                    =     backend.executeQuery(verifyTableStatement)
    val tableExist                          =     tableExistResult.next()
    tableExist should be (false)

    backend.close()

  }

  scenario("The user can drop a table with cascade", OracleTest) {
    val f = fixture
    val tableName: String = "cars_deba_c"
    val viewName: String = "cars_deba_c_v"
    val columnFixedWidth: Boolean = false
    val columnNames: List[String] = List("carid", "carnumber", "carmake", "carmodel")
    val dataTypes = List(CharacterDataType(20, columnFixedWidth),
    					 IntegerDataType,
    					 CharacterDataType(20, columnFixedWidth),
    					 CharacterDataType(20, columnFixedWidth))

    val verifyTableStatement =  "SELECT count(*) as count FROM dba_tables WHERE upper(table_name) = '%s' AND upper(owner) = '%s'".format(tableName.toUpperCase(), targetDbUserName.toUpperCase())
    val verifyViewStatement =  "SELECT count(*) as count FROM dba_views WHERE upper(view_name) = '%s' AND upper(owner) = '%s'".format(viewName.toUpperCase(), targetDbUserName.toUpperCase())
    val backend = new OracleBackend(f.props)
    val cascade = true

    Given("an active connection and an existing table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    backend.createTable(tableName,columnNames,dataTypes, dbSchema)
    var tableVerifiedResult = backend.executeQuery(verifyTableStatement)
    assert(tableVerifiedResult.next())
    tableVerifiedResult.getInt("count") should  equal  (1)

    When("the user issues a drop table command with cascade for that table")
    backend.dropTable(tableName, cascade, schemaName = dbSchema)

    Then("the table and its associated view should be dropped")
    val tableExistResult = backend.executeQuery(verifyTableStatement)
    assert(tableExistResult.next())
    tableExistResult.getInt("count") should be(0)

    val viewExistResult = backend.executeQuery(verifyViewStatement)
    assert(viewExistResult.next())
    viewExistResult.getInt("count") should be(0)
    backend.close()

  }

  scenario("The user can iterate over the results of a select query", OracleTest) {
    //Prerequisites:  Need Multiple Row in table cars_deba_a

    val f                     = fixture

    val backend               = new OracleBackend(f.props)

    val tableName             = "cars_deba_a"

    val sqlStatement          = "select * from %s.%s".format(dbSchema.get, tableName)

    val bindVars:DataRow[String]                  = DataRow.empty

    var resultsCount:Int                          = 0

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

  scenario("The user can update a record in a table using a valid update statement", OracleTest) {
    //Prerequisites:  Need Multiple Row in table cars_deba_a

    val f                             = fixture

    val backend                       = new OracleBackend(f.props)

    val tableName                     = "cars_deba_a"

    var columnNames:List[String]      = List("carid","carnumber","carmake","carmodel")


    val carId:String              = "K0000210"

    val carNumber:Int                       = 1234567899

    val carMake                     = "MiniCoopeRa"

    val carModel                      = "FourteenMillion"

    var sqlStatement                  = "update %s.%s set %s where carid = '%s' ".format(dbSchema.get,
    																				   tableName,
    																				   columnNames.map{"%s = ?".format(_)}.mkString(","),
    																				   carId)

    var valuesList:List[Any]          = List(carId,carNumber,carMake,carModel,carId)

    var bindVars:DataRow[Any]         = DataRow(("carid",carId),("carnumber",carNumber),("carmake",carMake),("carmodel",carModel))

    var resultsCount:Int              = 0

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("an update query is executed")
    try
      backend.execute(sqlStatement,bindVars)
    catch {
    case e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n" + sqlStatement )
    }


    Then("the record(s) should be updated")
    columnNames                                   = List("carmodel")
    valuesList                                    = List(carModel)
    bindVars                                      = DataRow(("carmodel",carModel))
    sqlStatement                                  = "select count(*) as count from %s.%s where carmodel = ?".format(dbSchema.get, tableName)

    val recordCountResult                         = backend.executeQuery(sqlStatement,bindVars)
    assert(recordCountResult.next())
    resultsCount                                  = recordCountResult.getInt("count")
    resultsCount  should be (1)


    backend.close()
  }

  scenario("The user can update a multiple records in a table using a valid update statement", OracleTest) {
    //Prerequisites:  Need Multiple Row in table cars_deba_a

    val f                             = fixture

    val backend                       = new OracleBackend(f.props)

    val tableName                     = "cars_deba_a"

    var columnNames:List[String]      = List("carmodel")

    var sqlStatement                  = "update %s.%s set %s ".format(dbSchema.get, 
    																  tableName, 
    																  columnNames.map{"%s = ?".format(_)}.mkString(","))


    val carModel                                  = "SeventeenMillion"

    val valuesList:List[Any]                      = List(carModel)

    val bindVars:DataRow[Any]                     = DataRow((columnNames(0),carModel))

    var resultsCount:Int                          = 0

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("an update query for multiple records is executed")
    try
      backend.execute(sqlStatement,bindVars)
    catch {
    case e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n" + sqlStatement )
    }

    Then("multiple record(s) should be updated")
    sqlStatement                                  = "select count(*) as count from %s.%s where carmodel = ?".format(dbSchema.get, tableName)

    val recordCountResult                         = backend.executeQuery(sqlStatement,bindVars)
    assert(recordCountResult.next())
    resultsCount                                  = recordCountResult.getInt("count")
    resultsCount  should be > (1)
    //It would be better to compare the number of rows updated to the count  i.e.:
    //http://www.coderanch.com/t/426288/JDBC/java/Row-count-update-statement
    //Statement st = connection.createStatement("update t_number set number = 2 where name='abcd");
    //int rowCount = st.executeUpdate();
    //However, this executeUpdate is not yet available on the backend


    backend.close()
  }

  scenario("The user can update a multiple records in a table without constructing update statement", OracleTest) {
    //Prerequisites:  Need Multiple Row in table cars_deba_a   with carmake = 'MiniCoopeRb'

    val f                                         = fixture

    val backend                                   = new OracleBackend(f.props)

    val tableName                                 = "cars_deba_a"

    var columnNames:List[String]                  = List("carnumber","carmake","carmodel")

    val carNumber:Int                             = 192837465

    val carMake                                   = "MiniCoopeRaStyle02"

    val carModel                                  = "SeventeenMillion"

    val valuesList:List[Any]                      = List(carNumber,carMake,carModel)

    val filter:List[(String, Any)]                = List(("carmake","MiniCoopeRb"))

    val updatesBindVars:DataRow[Any]              =
      DataRow((columnNames(0),valuesList(0)),(columnNames(1),valuesList(1)),(columnNames(2),valuesList(2)))

    var resultsCount:Int                          = 0

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("an update row instruction for multiple records is executed")
    try
      backend.updateRow(tableName,updatesBindVars,filter,dbSchema)
    catch {
    case e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n")
    }

    Then("multiple record(s) should be updated")
    val sqlStatement                              = "select count(*) as count from %s.%s where carmake = ?".format(dbSchema.get, tableName)


    val bindVars:DataRow[Any]                     =
      DataRow((columnNames(1),valuesList(1)))

    val recordCountResult                         = backend.executeQuery(sqlStatement,bindVars)
    assert(recordCountResult.next())
    resultsCount                                  = recordCountResult.getInt("count")
    resultsCount  should be > (1)
    //It would be better to compare the number of rows updated to the count  i.e.:
    //http://www.coderanch.com/t/426288/JDBC/java/Row-count-update-statement
    //Statement st = connection.createStatement("update t_number set number = 2 where name='abcd");
    //int rowCount = st.executeUpdate();
    //However, this executeUpdate is not yet available on the backend


    backend.close()
  }

  scenario("The user can insert a multiple rows using a loop without constructing an insert statement", OracleTest) {
    //Prerequisites:  None of theses record should exist
    val f = fixture

    val tableName:String                      =     "cars_deba_a"

    val columnNames:List[String]              =     List("carid","carnumber","carmake","carmodel")

    val carIds:List[String]               =     List("K0000500","K0000501","K0000502","K0000503","K0000504")

    val carNumbers:List[Int]                        =     List(1234561000,1234561001,1234561002,1234561003,1234561004)

    val carMakes:List[String]               =     List("MiniCoopeRd","MiniCoopeRd","MiniCoopeRd","MiniCoopeRd","MiniCoopeRd")

    val carModels:List[String]                =     List("Zero","One","Ten","Ten","Ten")

    val backend                               =     new OracleBackend(f.props)

    val verifyRecordsStatement:String         =     "select count(*) as count from %s.%s where carid in (%s)".format(dbSchema.get,
    																											     tableName,
    																											     carIds.map("'%s'".format(_)).mkString(","))
    																											     
                           

    var recordCount:Int                       =     0

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("the user issues valid insert row commands in a loop for an existing table")

    for (i <- 0 to (carIds.length -1))
    {

      var row:DataRow[Any]                     =     DataRow(("carid",carIds(i)),("carnumber",carNumbers(i)),("carmake",carMakes(i)),("carmodel",carModels(i)))

      assert(backend.insertReturningKeys(tableName,row, schemaName = dbSchema).isInstanceOf[DataRow[Any]]  )


    }


    Then("the rows should be inserted")
    val recordCountResult                       =     backend.executeQuery(verifyRecordsStatement)
    assert(recordCountResult.next())
    recordCount                                 =     recordCountResult.getInt("count")
    recordCount  should equal (carIds.length)

    backend.close()

  }

  scenario("The user can delete multiple records in a table using a valid delete statement", OracleTest) {
    //Prerequisites:  Need Multiple Row in table cars_deba_a

    val f                             = fixture

    val backend                       = new OracleBackend(f.props)

    val tableName                     = "cars_deba_a"

    val columnNames:List[String]      = List("carmodel")

    var sqlStatement                  = "delete from %s.%s where %s".format(dbSchema.get,
                                        		  							tableName,
                                        		  							columnNames.map("%s = ?".format(_)).mkString(" and ") )

    val carModel                                  = "Ten"

    val valuesList:List[Any]                      = List(carModel)

    val bindVars:DataRow[Any]                     = DataRow((columnNames(0),valuesList(0)))



    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("a delete query for multiple records is executed")
    backend.execute(sqlStatement,bindVars)

    Then("those records should be deleted")
    sqlStatement = "select count(*) as count from %s.%s where carmodel = ?".format(dbSchema.get, tableName)

    val countResult = backend.executeQuery(sqlStatement,bindVars)
    assert(countResult.next())
    countResult.getInt("count") should equal  (0)
    backend.close()

  }

  scenario("The user can drop all tables that begin with a certain string", OracleTest) {

    /*http://stackoverflow.com/questions/3476765/mysql-drop-all-tables-ignoring-foreign-keys*/

    val f = fixture

    val searchString: String = "cars_deba"

    val objectStatement: String = "select objectname from " +
      "(" +
      "(select table_name 	as objectname from user_tables 	WHERE    table_name like '" + searchString + "%'" + ")" +
      ") a"

    val verifyObjectCountStatement: String = "select count(objectname) as count from " +
      "(" +
      "(select table_name 	as objectname from user_tables 	WHERE    table_name like '" + searchString + "%'" + ")" +
      ") a"

    val backend = new OracleBackend(f.props)

    val cascade: Boolean = true

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("the user issues drop table commands for tables that begin with a certain string")
    try {

    val  objectResult                         =         backend.executeQuery(objectStatement)

        try {

          while(objectResult.next())  {

            backend.dropTable(objectResult.getString("objectname"), cascade, schemaName = dbSchema)

          }

        objectResult.close()
        }
        catch {
        case e:java.sql.SQLException =>
            fail(e.getMessage + "\n" + e.getCause + "\n")
        }
    }
    catch {
    case e:java.sql.SQLException =>
            println(e.getMessage)
            fail(e.getMessage + "\n" + e.getCause + "\n")
    }


    Then("then those tables should be dropped")
    val objectExistResult                      =     backend.executeQuery(verifyObjectCountStatement)
    assert(objectExistResult.next())
    objectExistResult.getInt("count")  should be (0)

    backend.commit()

    backend.close()

  }

  scenario("Remove Test Data Setup", OracleTest)  {
    /**** Remove Test Data    ****/
    removeTestDataSetup
    /****                     ****/

  }

  scenario("Close Test SetUp Connections", OracleTest)  {

    setup.targetBackend.close

  }



}

