package edu.chop.cbmi.dataExpress.test.backends

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 11/22/11
 * Time: 1:22 PM
 * To change this template use File | Settings | File Templates.
 */

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.Spec
import org.scalatest.FeatureSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.GivenWhenThen

import java.util.Properties

import edu.chop.cbmi.dataExpress.backends.PostgresBackend
import edu.chop.cbmi.dataExpress.dataModels._
import edu.chop.cbmi.dataExpress.dataModels.sql._

import edu.chop.cbmi.dataExpress.test.util.TestProps
import edu.chop.cbmi.dataExpress.test.util.cars.dataSetup.backends.PostgresDataSetup
import scala.language.reflectiveCalls

class PostgresBackendFeatureSpec extends FeatureSpec with GivenWhenThen with ShouldMatchers {

  def fixture =
    new {
	  val props = TestProps.getDbProps("postgres")
    }

  val dbSchema:Option[String]         = Option("qe10c01")

  val targetDbUserName:String         = "qe10c01"


  def dataSetupFixture =
    new {

      val tf                      =       fixture

      val targetBackend           =       new PostgresBackend(tf.props)

      targetBackend.connect

      val targetConnection        =       targetBackend.connection
      val targetStatement         =       targetConnection.createStatement()

      val dataSetup                     =   new PostgresDataSetup()

      dataSetup.targetDBSchemaName      =   dbSchema.get

      dataSetup.targetDbUserName        =   targetDbUserName

    }


  val setup                    =       dataSetupFixture

  def setUpTestData: Boolean   = {

    setup.targetStatement.execute(setup.dataSetup.createTargetSchema)

    setup.targetBackend.commit

    true
  }


  def removeTestDataSetup: Boolean = {
    try {	
    	setup.targetStatement.execute(setup.dataSetup.dropTargetSchema)
    }
    catch {
    	case e:org.postgresql.util.PSQLException => {
    	  setup.targetBackend.rollback
    	  setup.targetStatement.execute(setup.dataSetup.dropTargetSchema)
    	}
    }
    finally {
    	setup.targetBackend.commit
    }


    true
  }

  scenario("Data Setup")  {
    /**** SetUp Test Data   ****/
    setUpTestData
    /****                   ****/

  }


  scenario("The user can create a table with four columns") {
    val f = fixture

    val tableName                             =     "cars_deba_a"

    val columnFixedWidth:Boolean              =     false

    val columnNames:List[String]              =     List("carid","carnumber","carmake","carmodel")

    val dataTypes                             =     List(CharacterDataType(20,columnFixedWidth),IntegerDataType(),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth))

    val verifyTableStatement:String           =     "SELECT count(1) as count FROM pg_tables WHERE tablename = " + "'" + tableName + "'" +  " and schemaname = " + "'" + dbSchema.get  + "'"

    val backend                               =     new PostgresBackend(f.props)

    val cascade:Boolean                       =     true

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
          backend.dropTable(tableName,cascade,dbSchema)
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

    try
      backend.createTable(tableName,columnNames,dataTypes, schemaName = dbSchema)
    catch {
    case e:java.sql.SQLException =>
            println(e.getMessage + "\n")
            fail("backend.createTable(" + "\"" + tableName + "," + dbSchema.get + "\"" + ")produced java.sql.SQLException" )
    }


    Then("the table should exist")
    val tableExistResult                          =     backend.executeQuery(verifyTableStatement)
    assert(tableExistResult.next())
    tableExistResult.getInt("count") should equal (1)

    backend.commit()

    backend.close()
  }



  scenario("The user can truncate a table and commit") {
     val f = fixture

     val tableName:String                            =     "cars_deba_a"

     val countStatement:String                       =     "select count(1) from "  + dbSchema.get +  "."  + tableName

     val backend                                     =     new PostgresBackend(f.props)

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




  scenario("The inserted row can be committed and returned by executing an insert query") {
    val f             = fixture

    val backend       = new PostgresBackend(f.props)

    val tableName     = "cars_deba_a"

    val columnNames:List[String]      = List("carid","carnumber","carmake","carmodel")

    val valuesHolders:List[String]    = for (i <- (0 to (columnNames.length - 1)).toList) yield "?"

    val sqlStatement                  = "insert into " + dbSchema.get + "." + tableName                                 +
                                      "("                                                                           +
                                      columnNames.map(i => i + ",").mkString.dropRight(1)                           +
                                      ")"                                                                           +
                                      " values (" + valuesHolders.map(i => i + ",").mkString.dropRight(1)  + ")"


    val carId:String                  = "K0000001"

    val carNumber:Int                 = 1234567890

    val carMake                       = "MiniCoopeRa"

    val carModel                      = "One"

    val valuesList                    = List(carId,carNumber,carMake,carModel)

    val bindVars:DataRow[Any]         =
      DataRow((columnNames(0),valuesList(0)),(columnNames(1),valuesList(1)),(columnNames(2),valuesList(2)),(columnNames(3),valuesList(3)))

    var isDataRow:Boolean             = false

    var insertedRow:DataRow[Any]           = DataRow.empty

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("an insert query is executed and committed with returning keys")
    try
      insertedRow                     = backend.executeReturningKeys(sqlStatement,bindVars)
    catch {
    case e:java.sql.SQLException =>
            fail("backend.executeReturningKeys(" + "\"" + sqlStatement + "\"" + ")produced java.sql.SQLException" )
    }


    try
      backend.commit()
    catch {
    case e:java.sql.SQLException =>
            fail("backend.commit()produced java.sql.SQLException" )
    }

    Then("the inserted row should be returned")
    isDataRow                         = insertedRow.isInstanceOf[DataRow[Any]]
    isDataRow  should be (true)
    insertedRow.length should equal  (bindVars.length)

    backend.close()
  }



  scenario("The user can obtain a record from executing a select query") {
    //Prerequisites:  scenario 1:  Passed

    val f                     = fixture

    val backend               = new PostgresBackend(f.props)

    val tableName             = "cars_deba_a"

    val sqlStatement          = "select * from " + dbSchema.get + "." + tableName + " where carid  = ?"

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





  scenario("The user can determine whether a select query has returned a record") {
    //Prerequisites:  scenario 1:  Passed

    val f             =   fixture

    val backend       =   new PostgresBackend(f.props)

    val tableName     =   "cars_deba_a"

    val sqlStatement  =   "select * from " + dbSchema.get + "." + tableName + " where carid  = ?"

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
            fail("backend.executeQuery(" + sqlStatement + ")produced java.sql.SQLException" )
    }


    backend.close()


  }







  scenario("The user can commit an open transaction") {

    val f                             = fixture

    var backend                       = new PostgresBackend(f.props)

    val tableName                     = "cars_deba_a"

    val columnNames:List[String]      = List("carid","carnumber","carmake","carmodel")

    val valuesHolders:List[String]    = for (i <- (0 to (columnNames.length - 1)).toList ) yield "?"

    var sqlStatement                  = "insert into " + dbSchema.get + "." + tableName                     +
                                      "("                                                               +
                                      columnNames.map(i => i + ",").mkString.dropRight(1)               +
                                      ")"                                                               +
                                      " values (" + valuesHolders.map(i => i + ",").mkString.dropRight(1)  + ")"

    val carId:String                  = "K0000002"

    val carNumber:Int                 = 1234567899

    val carMake                       = "MiniCoopeRa"

    val carModel                      = "Two"

    val valuesList                    = List(carId,carNumber,carMake,carModel)

    val bindVars:DataRow[Any]         = DataRow(("carid",carId),("carnumber",carNumber),("carmake",carMake),("carmodel",carModel))

    var committed:Boolean             = false

    var connectionClosed:Boolean      = false

    var dataPersistent:Boolean        = false

    Given("an active connection with an open transaction ")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    backend.execute("START TRANSACTION")          //This should be replaced with backend.startTransaction when available
    val insertedRow                   = backend.executeReturningKeys(sqlStatement,bindVars)

    When("the user issues a commit instruction")
    try
      backend.commit()
    catch {
    case e:java.sql.SQLException =>
            fail("backend.commit()produced java.sql.SQLException" )
    }

    Then("the data should be persisted")
    backend.close()
    connectionClosed                    = backend.connection.isClosed
    connectionClosed  should be (true)
    sqlStatement                        = "select * from " + dbSchema.get + "." + tableName       +
                                          " where "                                           +
                                          " carid  = ? "            +      " and "        +
                                          " carnumber  = ? "                  +      " and "        +
                                          " carmake  = ? "            +      " and "        +
                                          " carmodel  = ? "

    val newFixture                      = fixture
    backend                             = new PostgresBackend(newFixture.props)
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    dataPersistent                      = backend.execute(sqlStatement,bindVars)
    dataPersistent should be (true)

    backend.close()

  }




  scenario("The user can truncate a populated table") {
    val f = fixture

    val tableName:String                            =     "cars_deba_a"

    val countStatement:String                       =     "select count(1) from "  + dbSchema.get +  "."  + tableName

    val backend                                     =     new PostgresBackend(f.props)

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
            fail("backend.truncateTable(" + "\"" + tableName + "," + dbSchema.get + "\"" + ")produced java.sql.SQLException" )
    }

    Then("the table should be truncated")
    countResult                                     =     backend.executeQuery(countStatement)
    assert(countResult.next())
    countResult.getInt("count") should equal (0)

    backend.close()

  }




  scenario("The user can roll back an open transaction") {

    val f                             = fixture

    val backend                       = new PostgresBackend(f.props)

    val tableName                     = "cars_deba_a"

    val columnNames:List[String]      = List("carid","carnumber","carmake","carmodel")

    val valuesHolders:List[String]    = for (i <- (0 to (columnNames.length - 1)).toList ) yield "?"

    var sqlStatement                  = "insert into " + dbSchema.get + "." + tableName                     +
                                      "("                                                               +
                                      columnNames.map(i => i + ",").mkString.dropRight(1)               +
                                      ")"                                                               +
                                      " values (" + valuesHolders.map(i => i + ",").mkString.dropRight(1)  + ")"

    val carId:String                  = "K0000050"

    val carNumber:Int                 = 1234567777

    val carMake                       = "MiniCoopeRa"

    val carModel                      = "Fifty"

    val valuesList                    = List(carId,carNumber,carMake,carModel)

    val bindVars:DataRow[Any]         = DataRow(("carid",carId),("carnumber",carNumber),("carmake",carMake),("carmodel",carModel))

    var connectionClosed:Boolean      = false

    var dataPersistent:Boolean        = false

    Given("an active connection with an open transaction ")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    backend.execute("START TRANSACTION")          //This should be replaced with backend.startTransaction when available
    val insertedRow                   = backend.executeReturningKeys(sqlStatement,bindVars)
    assert(insertedRow.isInstanceOf[DataRow[Any]])

    When("the user issues a roll back instruction")
    try
      backend.rollback()
    catch {
    case e:java.sql.SQLException =>
            fail("backend.rollback(" + "\"" + tableName + "," + dbSchema.get + "\"" + ")produced java.sql.SQLException" )
    }

    Then("the data should not be persisted")
    backend.close()
    connectionClosed                  = backend.connection.isClosed
    connectionClosed  should be (true)
    sqlStatement                      = "select count(1) as count from " + dbSchema.get + "." + tableName     +
                                        " where "                                                         +
                                        " carid  = ? "            +      " and "                      +
                                        " carnumber  = ? "                  +      " and "                      +
                                        " carmake  = ? "            +      " and "                      +
                                        " carmodel  = ? "

    val newFixture                            = fixture
    val newBackend                            = new PostgresBackend(newFixture.props)
    assert(newBackend.connect().isInstanceOf[java.sql.Connection] )
    val persistentDataCount                   = newBackend.executeQuery(sqlStatement,bindVars)
    assert(persistentDataCount.next() )
    persistentDataCount.getInt("count") should equal (0)
    newBackend.close()

  }






  scenario("The user can open a transaction, insert a row, and end the transaction") {

    val f                             = fixture

    val backend                       = new PostgresBackend(f.props)

    val tableName                     = "cars_deba_a"

    val columnNames:List[String]      = List("carid","carnumber","carmake","carmodel")

    val valuesHolders:List[String]    = for (i <- (0 to (columnNames.length - 1)).toList ) yield "?"

    var sqlStatement                  = "insert into " + dbSchema.get + "." + tableName                     +
                                      "("                                                               +
                                      columnNames.map(i => i + ",").mkString.dropRight(1)               +
                                      ")"                                                               +
                                      " values (" + valuesHolders.map(i => i + ",").mkString.dropRight(1)  + ")"

    val carId:String                  = "K0000055"

    val carNumber:Int                 = 1234567755

    val carMake                       = "MiniCoopeRa"

    val carModel                      = "FiftyFive"

    val valuesList                    = List(carId,carNumber,carMake,carModel)

    val bindVars:DataRow[Any]         = DataRow((columnNames(0),valuesList(0)),(columnNames(1),valuesList(1)),(columnNames(2),valuesList(2)),(columnNames(3),valuesList(3)))

    var connectionClosed:Boolean      = false



    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("the user issues a start transaction instruction")
    try
            backend.startTransaction()          //This should be replaced with backend.startTransaction when available
    catch {
    case    e:java.sql.SQLException =>
            println(e.getMessage)
            fail("backend.startTransaction() produced java.sql.SQLException" )
    }

    And("the user inserts a row")
    try   {
            val insertedRow                   = backend.executeReturningKeys(sqlStatement,bindVars)
            assert(insertedRow.isInstanceOf[DataRow[Any]])
    }
    catch {
    case    e:java.sql.SQLException =>
            println(e.getMessage)
            fail("backend.executeReturningKeys(" + sqlStatement + ") produced java.sql.SQLException" )
    }

    And("the user ends the transaction")
    try
            backend.endTransaction()
    catch {
    case    e:java.sql.SQLException =>
            fail("backend.endTransaction() produced java.sql.SQLException" )
    }

    Then("the data should be persisted")
    backend.close()
    connectionClosed                  = backend.connection.isClosed
    connectionClosed  should be (true)
    sqlStatement                      = "select count(1) as count from " + dbSchema.get + "." + tableName     +
                                        " where "                                                         +
                                        " carid  = ? "            +      " and "                          +
                                        " carnumber  = ? "                  +      " and "                +
                                        " carmake  = ? "            +      " and "                        +
                                        " carmodel  = ? "

    val newFixture                            = fixture
    val newBackend                            = new PostgresBackend(newFixture.props)
    assert(newBackend.connect().isInstanceOf[java.sql.Connection] )
    val persistentDataCount                   = newBackend.executeQuery(sqlStatement,bindVars)
    assert(persistentDataCount.next() )
    persistentDataCount.getInt("count") should equal (1)
    newBackend.close()

  }



  scenario("The user can create a table with 32 columns") {
    val f = fixture

    val tableName                             =     "cars_deba_b"

    val columnFixedWidth:Boolean              =     false

    val columnNames:List[String]              =     List("carid","number","make","model")

    val dataTypes                             =     List( CharacterDataType(20,columnFixedWidth),IntegerDataType(),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),
                                                          CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),
                                                          CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),
                                                          CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),
                                                          CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),
                                                          CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),
                                                          CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),
                                                          CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth)
                                                        )

    val verifyTableStatement:String           =     "SELECT count(1) as count FROM pg_tables WHERE tablename = " + "'" + tableName + "'" +  " and schemaname = " + "'" + dbSchema.get  + "'"

    val backend                               =     new PostgresBackend(f.props)

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("the user issues a valid create table instruction")
    try
      backend.createTable(tableName,columnNames,dataTypes, schemaName = dbSchema)
    catch {
    case e:java.sql.SQLException =>
            fail("backend.createTable(" + "\"" + tableName + "," + dbSchema.get + "\"" + ")produced java.sql.SQLException" )
    }


    Then("the table should exist")
    val tableExistResult                                      =     backend.executeQuery(verifyTableStatement)
    assert(tableExistResult.next())
    tableExistResult.getInt("count") should equal (1)


    backend.close()
  }









  scenario("The user can insert a row without constructing an insert statement") {
    val f = fixture

    val tableName:String                      =     "cars_deba_a"

    val columnNames:List[String]              =     List("carid","number","make","model")

    val carId:String                          =     "K0000003"

    val carNumber:Int                         =     1234567888

    val carMake:String                        =     "MiniCoopeRa"

    val carModel:String                       =     "Three"

    val valuesList:List[Any]                  =     List(carId,carNumber,carMake,carModel)

    val row:DataRow[Any]                      =     DataRow(("carid",carId),("carnumber",carNumber),("carmake",carMake),("carmodel",carModel))

    val backend                               =     new PostgresBackend(f.props)

    val verifyRecordStatement:String          =     "select count(1) as count from " + dbSchema.get + "." + tableName + " where "    +
                                                    "carid = " + "'" + row.carid.get  + "'"



    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("the user issues a valid insert command for an existing table and a unique record")
    var recordCountResult                         =     backend.executeQuery(verifyRecordStatement)
    assert(recordCountResult.next())
    var recordCount                               =     recordCountResult.getInt("count")
    recordCount should be (0)
    val insertedRow                               =     backend.insertReturningKeys(tableName,row,dbSchema)


    Then("the insert command should be successful")
    val isInstanceOfDataRow                       =     insertedRow.isInstanceOf[DataRow[Any]]
    isInstanceOfDataRow   should be (true)
    insertedRow.length    should equal (valuesList.length)


    And("the row should be inserted")
    recordCountResult                             =     backend.executeQuery(verifyRecordStatement)
    assert(recordCountResult.next())
    recordCount                                   =     recordCountResult.getInt("count")
    recordCount  should be (1)

    backend.commit()
    backend.close()

  }







  scenario("The user can insert a batch of rows and commit without having to construct the insert statements") {
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

    val backend                               =     new PostgresBackend(f.props)

    var successfulStatementCount:Int          =     0

    val verifyRowsStatement:String            =     "select count(1) as count from " + dbSchema.get + "." + tableName + " where "     +
                                                    "carid in "                                                                           +
                                                    " ("                                                                                      +
                                                    {for (i <- 0 to (rows.length - 1)) yield "'" + rows(i).toList.head.toString + "'"}.toString().dropRight(1).drop(7) +
                                                    ")"

    Given("an active connection and an empty table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    try
      backend.truncateTable(tableName, dbSchema)
    catch {
    case e:java.sql.SQLException =>
            fail("backend.truncateTable(" + "\"" + tableName + "," + dbSchema.get + "\"" + ")produced java.sql.SQLException" )
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




  scenario("The user can drop a table") {
    val f = fixture

    val tableName:String                      =     "cars_deba_c"

    val columnFixedWidth:Boolean              =     false

    val columnNames:List[String]              =     List("carid","number","make","model")

    val dataTypes                             =     List(CharacterDataType(20,columnFixedWidth),IntegerDataType(),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth))

    val verifyTableStatement:String           =     "SELECT tablename FROM pg_tables WHERE tablename = " + "'" + tableName + "'" +  " and schemaname = " + "'" + dbSchema.get  + "'"

    val backend                               =     new PostgresBackend(f.props)

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
            fail("backend.createTable(" + "\"" + tableName + "," + dbSchema.get + "\"" + ")produced java.sql.SQLException" )
    }
    tableVerified                             =     backend.execute(verifyTableStatement)
    tableVerified should be (true)

    When("the user issues a drop table command for that table")
    try
      backend.dropTable(tableName, schemaName = dbSchema)
    catch {
    case e:java.sql.SQLException =>
            fail("backend.dropTable(" + "\"" + tableName + "," + dbSchema.get + "\"" + ")produced java.sql.SQLException" )
    }


    Then("the table should be dropped")
    val tableExistResult                    =     backend.executeQuery(verifyTableStatement)
    val tableExist                          =     tableExistResult.next()
    tableExist should be (false)

    backend.close()

  }






  scenario("The user can drop a table with cascade") {
    val f = fixture

    val tableName:String                      =     "cars_deba_c"

    val viewName:String                       =     "cars_deba_c_v"

    val columnFixedWidth:Boolean              =     false

    val columnNames:List[String]              =     List("carid","number","make","model")

    val dataTypes                             =     List(CharacterDataType(20,columnFixedWidth),IntegerDataType(),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth))

    val verifyTableStatement:String           =     "SELECT count(1) as count FROM pg_tables WHERE tablename = " + "'" + tableName + "'" +  " and schemaname = " + "'" + dbSchema.get  + "'"

    val createViewStatement:String            =     "create view " + dbSchema.get + "." + viewName + " as "             +
                                                    "select * from " + dbSchema.get + "." + tableName

    val verifyViewStatement:String            =     "SELECT count(1) as count FROM pg_views WHERE viewname = " + "'" + viewName + "'" +  " and schemaname = " + "'" + dbSchema.get  + "'"

    val backend                               =     new PostgresBackend(f.props)



    val cascade                               =     true

    Given("an active connection, an existing table, and a view on the existing table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    try
      backend.createTable(tableName,columnNames,dataTypes, dbSchema)
    catch {
    case  e:java.sql.SQLException =>
          println(e.getMessage)
          fail("backend.createTable(" + "\"" + tableName + "," + dbSchema.get + "\"" + ")produced java.sql.SQLException" )
    }
    var tableVerifiedResult                   =     backend.executeQuery(verifyTableStatement)
    assert(tableVerifiedResult.next())
    tableVerifiedResult.getInt("count") should  equal  (1)




    try

      backend.execute(createViewStatement)
    catch {
    case  e:java.sql.SQLException =>
          println(e.getMessage)
          fail("backend.execute(" + createViewStatement + ")produced java.sql.SQLException" )
    }

    var viewVerifiedResult                    =     backend.executeQuery(verifyViewStatement)
    assert(viewVerifiedResult.next())
    viewVerifiedResult.getInt("count")  should   equal  (1)



    When("the user issues a drop table command with cascade for that table")
    try
      backend.dropTable(tableName, cascade, schemaName = dbSchema)
    catch {
    case e:java.sql.SQLException =>
            println(e.getMessage)
            fail("backend.dropTable(" + "\"" + tableName + "," + dbSchema.get + "\"" + ")produced java.sql.SQLException" )
    }


    Then("the table and its associated view should be dropped")
    val tableExistResult                    =     backend.executeQuery(verifyTableStatement)
    assert(tableExistResult.next())
    tableExistResult.getInt("count")  should be (0)


    val viewExistResult                     =     backend.executeQuery(verifyViewStatement)
    assert(viewExistResult.next())
    viewExistResult.getInt("count")   should be (0)


    backend.close()

  }





  scenario("The user can iterate over the results of a select query") {
    //Prerequisites:  Need Multiple Row in table cars_deba_a

    val f                     = fixture

    val backend               = new PostgresBackend(f.props)

    val tableName             = "cars_deba_a"

    val sqlStatement          = "select * from " + dbSchema.get + "." + tableName

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




  scenario("The user can update a record in a table using a valid update statement") {
    //Prerequisites:  Need Multiple Row in table cars_deba_a

    val f                             = fixture

    val backend                       = new PostgresBackend(f.props)

    val tableName                     = "cars_deba_a"

    var columnNames:List[String]      = List("carid","carnumber","carmake","carmodel")


    val carId:String                  = "K0000210"

    val carNumber:Int                 = 1234567899

    val carMake                       = "MiniCoopeRa"

    val carModel                      = "FourteenMillion"

    var sqlStatement                  = "update " + dbSchema.get + "." + tableName                            +
                                        " set "                                                           +
                                        columnNames.map(i => i + " = ?,").mkString.dropRight(1)           +
                                        " where carid = " + "'" + carId  + "'"

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
            fail("backend.execute(" + "\"" + sqlStatement + "\"" + ")produced java.sql.SQLException" )
    }


    Then("the record(s) should be updated")
    columnNames                                   = List("carmodel")
    valuesList                                    = List(carModel)
    bindVars                                      = DataRow(("carmodel",carModel))
    sqlStatement                                  = "select count(1) as count from " + dbSchema.get + "."  + tableName + " where "     +
                                                    "carmodel = ?"

    val recordCountResult                         = backend.executeQuery(sqlStatement,bindVars)
    assert(recordCountResult.next())
    resultsCount                                  = recordCountResult.getInt("count")
    resultsCount  should be (1)


    backend.close()
  }


  scenario("The user can update a multiple records in a table using a valid update statement") {
    //Prerequisites:  Need Multiple Row in table cars_deba_a

    val f                             = fixture

    val backend                       = new PostgresBackend(f.props)

    val tableName                     = "cars_deba_a"

    var columnNames:List[String]      = List("carmodel")

    var sqlStatement                  = "update " + dbSchema.get + "." + tableName                            +
                                        " set "                                                           +
                                        columnNames.map(i => i + " = ?,").mkString.dropRight(1)


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
            fail("backend.execute(" + "\"" + sqlStatement + "\"" + ")produced java.sql.SQLException" )
    }

    Then("multiple record(s) should be updated")
    sqlStatement                                  = "select count(1) as count from " + dbSchema.get + "." + tableName + " where "     +
                                                    "carmodel = ?"

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





  scenario("The user can update a multiple records in a table without constructing update statement") {
    //Prerequisites:  Need Multiple Row in table cars_deba_a   with carmake = 'MiniCoopeRb'

    val f                                         = fixture

    val backend                                   = new PostgresBackend(f.props)

    val tableName                                 = "cars_deba_a"

    var columnNames:List[String]                  = List("carnumber","carmake","carmodel")

    val carNumber:Int                             = 192837465

    val carMake                                   = "MiniCoopeRa1973"

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
            println(e.getMessage())
            fail("backend.updateRow()produced java.sql.SQLException" )
    }

    Then("multiple record(s) should be updated")
    val sqlStatement                              = "select count(1) as count from " + dbSchema.get + "." + tableName + " where "     +
                                                    "carmake = ?"


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





  scenario("The user can insert a multiple rows using a loop without constructing an insert statement") {
    //Prerequisites:  None of theses record should exist
    val f = fixture

    val tableName:String                      =     "cars_deba_a"

    val columnNames:List[String]              =     List("carid","number","make","model")

    val carIds:List[String]                   =     List("K0000500","K0000501","K0000502","K0000503","K0000504")

    val carNumbers:List[Int]                  =     List(1234561000,1234561001,1234561002,1234561003,1234561004)

    val carMakes:List[String]                 =     List("MiniCoopeRd","MiniCoopeRd","MiniCoopeRd","MiniCoopeRd","MiniCoopeRd")

    val carModels:List[String]                =     List("Zero","One","Ten","Ten","Fen")

    val backend                               =     new PostgresBackend(f.props)

    val verifyRecordsStatement:String         =     "select count(1) as count from " + dbSchema.get + "." + tableName   + 
                                                    " where "                                                       +
                                                    "carid in "                                                     +
                                                    "("                                                             +
                                                    carIds.map(i => "'" + i + "'" + ",").mkString.dropRight(1)  +
                                                    ")"

    var recordCount:Int                       =     0

    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("the user issues valid insert row commands in a loop for an existing table")

    for (i <- 0 to (carIds.length -1))
    {

      val row:DataRow[Any]                     =     DataRow(("carid",carIds(i)),("carnumber",carNumbers(i)),("carmake",carMakes(i)),("carmodel",carModels(i)))
      assert(backend.insertReturningKeys(tableName,row, schemaName = dbSchema).isInstanceOf[DataRow[Any]])


    }


    Then("the rows should be inserted")
    val recordCountResult                       =     backend.executeQuery(verifyRecordsStatement)
    assert(recordCountResult.next())
    recordCount                                 =     recordCountResult.getInt("count")
    recordCount  should equal (carIds.length)

    backend.close()

  }





  scenario("The user can delete multiple records in a table using a valid delete statement") {
    //Prerequisites:  Need Multiple Row in table cars_deba_a

    val f                             = fixture

    val backend                       = new PostgresBackend(f.props)

    val tableName                     = "cars_deba_a"

    val columnNames:List[String]      = List("carmodel")

    var sqlStatement                  = if    (columnNames.length > 1)  {
                                          "delete from " + dbSchema.get + "." + tableName                                 +
                                          " where "                                                                   +
                                          columnNames.map(i => i + " = ?").mkString(" and ")
                                        }
                                        else{
                                          "delete from " + dbSchema.get + "." + tableName                                 +
                                          " where "                                                                   +
                                          columnNames.map(i => i + " = ?").mkString
                                        }

    val carModel                                  = "Ten"

    val valuesList:List[Any]                      = List(carModel)

    val bindVars:DataRow[Any]                     = DataRow((columnNames(0),valuesList(0)))



    Given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    When("a delete query for multiple records is executed")
    try
      backend.execute(sqlStatement,bindVars)
    catch {
    case e:java.sql.SQLException =>
            println(e.getMessage)
            fail("backend.execute(" + "\"" + sqlStatement + "\"" + ")produced java.sql.SQLException" )
    }

    Then("those records should be deleted")
    sqlStatement                      =   "select count(1) as count from "   + dbSchema.get + "." + tableName             +
                                          " where carmodel = ?"

    try   {
          val countResult = backend.executeQuery(sqlStatement,bindVars)
          assert(countResult.next())
          countResult.getInt("count") should equal  (0)
    }
    catch {
    case  e:java.sql.SQLException =>
            println(e.getMessage + "\n\n")
            fail("backend.executeQuery(" + "\"" + sqlStatement + "\"" + ")produced java.sql.SQLException" )






    }


    backend.close()







  }






  scenario("The user can drop all tables that begin with a certain string") {

    /*http://stackoverflow.com/questions/3476765/mysql-drop-all-tables-ignoring-foreign-keys*/

    val f = fixture

    val searchString:String                   =         "cars_deba"


    val oldobjectStatement:String                =      "select objectname from "                                                                                                                         +
                                                        "("                                                                                                                                               +
                                                        "(select tablename 	as objectname from pg_tables 	WHERE    tablename like '" + searchString + "%'" + " and schemaname = '" + dbSchema.get + "')"      +
                                                        " UNION ALL     "                                                                                                                                 +
                                                        "(select viewname 	as objectname from pg_views 	WHERE    viewname like '" + searchString + "%'" + " and schemaname = '" + dbSchema.get + "')"       +
                                                        ") a"



    val oldverifyObjectCountStatement:String     =      "select count(objectname) as count from "                                                                                                         +
                                                        "("                                                                                                                                               +
                                                        "(select tablename 	as objectname from pg_tables 	WHERE    tablename like '" + searchString + "%'" + " and schemaname = '" + dbSchema.get + "')"      +
                                                        " UNION ALL     "                                                                                                                                 +
                                                        "(select viewname 	as objectname from pg_views 	WHERE    viewname like '" + searchString + "%'" + " and schemaname = '" + dbSchema.get + "')"       +
                                                        ") a"



    val objectStatement:String                =         "select objectname from "                                                                                                                         +
                                                        "("                                                                                                                                               +
                                                        "(select tablename 	as objectname from pg_tables 	WHERE    tablename like '" + searchString + "%'" + " and schemaname = '" + dbSchema.get + "')"      +
                                                        ") a"



    val verifyObjectCountStatement:String     =         "select count(objectname) as count from "                                                                                                         +
                                                        "("                                                                                                                                               +
                                                        "(select tablename 	as objectname from pg_tables 	WHERE    tablename like '" + searchString + "%'" + " and schemaname = '" + dbSchema.get + "')"      +
                                                        ") a"




    val backend                               =         new PostgresBackend(f.props)

    val cascade:Boolean                       =         true

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
            println(e.getMessage)
            fail("backend.dropTable(" + "\"" + dbSchema.get + "," + objectResult.getString("objectname") + "\"" + ")produced java.sql.SQLException" )
        }
    }
    catch {
    case e:java.sql.SQLException =>
            println(e.getMessage)
            fail("backend.executeQuery(" + objectStatement + ")produced java.sql.SQLException" )
    }


    Then("then those tables should be dropped")
    val objectExistResult                      =     backend.executeQuery(verifyObjectCountStatement)
    assert(objectExistResult.next())
    objectExistResult.getInt("count")  should be (0)

    backend.commit()

    backend.close()

  }




  scenario("Remove Test Data Setup")  {
    /**** Remove Test Data    ****/
      removeTestDataSetup 

  }

  scenario("Close Test SetUp Connections")  {

    setup.targetBackend.close

  }



}

