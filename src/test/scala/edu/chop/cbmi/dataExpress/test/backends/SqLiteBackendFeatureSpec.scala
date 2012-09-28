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
import edu.chop.cbmi.dataExpress.backends.SqLiteBackend
import edu.chop.cbmi.dataExpress.test.util._
import edu.chop.cbmi.dataExpress.dataModels._
import edu.chop.cbmi.dataExpress.dataModels.sql._

@RunWith(classOf[JUnitRunner])
class SqLiteBackendFeatureSpec extends FeatureSpec with GivenWhenThen with ShouldMatchers {
//TODO: Remove this, just temporary for compilation
          val dbSchema = ""

  def fixture =
    new {
        val prop_path = TestProps.property("sqlite_db_prop_file")
        val inputStream:java.io.FileInputStream = new java.io.FileInputStream(prop_path)
        val props = new Properties()
        props.load(inputStream)
        inputStream.close()
    }
  ignore("The user can create a table with four columns") {
    val f = fixture

    val tableName                             =     "cars_deba_a"
    val columnFixedWidth:Boolean              =     false
    val columnNames:List[String]              =     List("carid","number","make","model")
    val dataTypes                             =     List(CharacterDataType(20,columnFixedWidth),IntegerDataType(),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth))
    val verifyTableStatement:String           =     "SELECT count(1) as count FROM sqlite_master WHERE tbl_name = ?"
    val backend                               =     new SqLiteBackend(f.props)
    val cascade:Boolean                       =     true

    given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    when("the user issues a valid create table instruction for a table that does not exist")
    //Make sure the table doesn't exist, failure if it does  
    var tableExistResult                    =   backend.executeQuery(verifyTableStatement,Seq(Option(tableName)))
    tableExistResult.next() should be (true)
    withClue("Table %s exists already".format(tableName)) {tableExistResult.getInt("count") should equal (0)}
    
    withClue("Create Table: %s".format(tableName)) {backend.createTable(tableName, columnNames, dataTypes, null)}
    backend.commit()
    tableExistResult = backend.executeQuery(verifyTableStatement, Seq(Option(tableName)))
    tableExistResult.next() should be (true)
    tableExistResult.getInt("count") should equal (1)
    backend.close()
  }



  ignore("The user can truncate a table and commit") {
     val f = fixture
     val tableName:String                            =     "cars_deba_a"
     val countStatement:String                       =     "select count(1) from %s".format(tableName)

     val backend                                     =     new SqLiteBackend(f.props)

     given("an active connection and a populated table")
     assert(backend.connect().isInstanceOf[java.sql.Connection] )
     backend.connection.setAutoCommit(false)


     when("the user issues truncate and commit for table %s".format(tableName))
     backend.truncateTable(tableName)
     backend.commit()

     then("the table should be truncated")
     val countResult = backend.executeQuery(countStatement)
     countResult.next() should be (true)
     countResult.getInt("count(1)") should equal (0)

     backend.close()

   }




  ignore("The inserted row can be committed and returned by executing an insert query") {
    val f             = fixture

    val backend       = new SqLiteBackend(f.props)

    val tableName     = "cars_deba_a"

    val columnNames:List[String]      = List("carid","number","make","model")

    val valuesHolders:List[String]    = for (i <- (0 to (columnNames.length - 1)).toList) yield "?"

    val sqlStatement                  = "insert into " + dbSchema + "." + tableName                                 +
                                      "("                                                                           +
                                      columnNames.map(i => i + ",").mkString.dropRight(1)                           +
                                      ")"                                                                           +
                                      " values (" + valuesHolders.map(i => i + ",").mkString.dropRight(1)  + ")"


    val patientId:String              = "Z0000001"

    val mrn:Int                       = 1234567890

    val firstName                     = "Utesta"

    val lastName                      = "One"

    val valuesList                    = List(patientId,mrn,firstName,lastName)

    val bindVars:DataRow[Any]         =
      DataRow((columnNames(0),valuesList(0)),(columnNames(1),valuesList(1)),(columnNames(2),valuesList(2)),(columnNames(3),valuesList(3)))

    var isDataRow:Boolean             = false

    var insertedRow:DataRow[Any]           = DataRow.empty

    given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    when("an insert query is executed and committed with returning keys")
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

    then("the inserted row should be returned")
    isDataRow                         = insertedRow.isInstanceOf[DataRow[Any]]
    isDataRow  should be (true)
    insertedRow.length should equal  (bindVars.length)

    backend.close()
  }



  ignore("The user can obtain a record from executing a select query") {
    //Prerequisites:  ignore 1:  Passed

    val f                     = fixture

    val backend               = new SqLiteBackend(f.props)

    val tableName             = "cars_deba_a"

    val sqlStatement          = "select * from " + dbSchema + "." + tableName + " where patientid  = ?"

    val valuesList:List[String]                   = List("Z0000001")

    val columnNames:List[String]                  = List("patientid")

    val bindVars:DataRow[String]                  = DataRow((columnNames(0),valuesList(0)))

    var hasResults:Boolean                        = false

    given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    when("a query that should generate results is executed")
    val resultSet = backend.executeQuery(sqlStatement,bindVars)

    then("one or more results should be returned")
    hasResults                                    = resultSet.next()
    hasResults should be (true)

    backend.close()
  }





  ignore("The user can determine whether a select query has returned a record") {
    //Prerequisites:  ignore 1:  Passed

    val f             =   fixture

    val backend       =   new SqLiteBackend(f.props)

    val tableName     =   "cars_deba_a"

    val sqlStatement  =   "select * from " + dbSchema + "." + tableName + " where patientid  = ?"

    val columnNames                               = List("patientid")

    val valuesList                                = List("Z0000001")

    val bindVars:DataRow[String]                  = DataRow((columnNames(0),valuesList(0)))

    given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    when("a select query that has a non empty result is executed")
    //resultSetReturned                             = backend.execute(sqlStatement,bindVars)
    //resultSetReturned seems to only be true only if it is an update count or if execute does not return anything at all
    try {
          val results =  backend.executeQuery(sqlStatement,bindVars)
          then("the query should have returned a non empty result set")
          val nonEmptyResultSet:Boolean                 =   results.next()
          nonEmptyResultSet should be (true)
    }
    catch {
    case  e:java.sql.SQLException =>
            fail("backend.executeQuery(" + sqlStatement + ")produced java.sql.SQLException" )
    }


    backend.close()


  }







  ignore("The user can commit an open transaction") {

    val f                             = fixture

    var backend                       = new SqLiteBackend(f.props)

    val tableName                     = "cars_deba_a"

    val columnNames:List[String]      = List("carid","number","make","model")

    val valuesHolders:List[String]    = for (i <- (0 to (columnNames.length - 1)).toList ) yield "?"

    var sqlStatement                  = "insert into " + dbSchema + "." + tableName                     +
                                      "("                                                               +
                                      columnNames.map(i => i + ",").mkString.dropRight(1)               +
                                      ")"                                                               +
                                      " values (" + valuesHolders.map(i => i + ",").mkString.dropRight(1)  + ")"

    val patientId:String              = "Z0000002"

    val mrn:Int                       = 1234567899

    val firstName                     = "Utesta"

    val lastName                      = "Two"

    val valuesList                    = List(patientId,mrn,firstName,lastName)

    val bindVars:DataRow[Any]         = DataRow(("patientid",patientId),("mrn",mrn),("firstname",firstName),("lastname",lastName))

    var committed:Boolean             = false

    var connectionClosed:Boolean      = false

    var dataPersistent:Boolean        = false

    given("an active connection with an open transaction ")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    backend.execute("START TRANSACTION")          //This should be replaced with backend.startTransaction when available
    val insertedRow                   = backend.executeReturningKeys(sqlStatement,bindVars)

    when("the user issues a commit instruction")
    try
      backend.commit()
    catch {
    case e:java.sql.SQLException =>
            fail("backend.commit()produced java.sql.SQLException" )
    }

    then("the data should be persisted")
    backend.close()
    connectionClosed                    = backend.connection.isClosed
    connectionClosed  should be (true)
    sqlStatement                        = "select * from " + dbSchema + "." + tableName       +
                                          " where "                                           +
                                          " patientid  = ? "            +      " and "        +
                                          " mrn  = ? "                  +      " and "        +
                                          " firstname  = ? "            +      " and "        +
                                          " lastname  = ? "

    val newFixture                      = fixture
    backend                             = new SqLiteBackend(newFixture.props)
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    dataPersistent                      = backend.execute(sqlStatement,bindVars)
    dataPersistent should be (true)

    backend.close()

  }




  ignore("The user can truncate a populated table") {
    val f = fixture

    val tableName:String                            =     "cars_deba_a"

    val countStatement:String                       =     "select count(1) from "  + dbSchema +  "."  + tableName

    val backend                                     =     new SqLiteBackend(f.props)

    given("an active connection and a populated table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    var countResult                                 =     backend.executeQuery(countStatement)
    countResult.next() should  be (true)
    countResult.getInt("count") should be > (0)

    when("the user issues a truncate table instruction for that table")
    try
      backend.truncateTable(tableName, schemaName = Option(dbSchema))
    catch {
    case e:java.sql.SQLException =>
            fail("backend.truncateTable(" + "\"" + tableName + "," + dbSchema + "\"" + ")produced java.sql.SQLException" )
    }

    then("the table should be truncated")
    countResult                                     =     backend.executeQuery(countStatement)
    assert(countResult.next())
    countResult.getInt("count") should equal (0)

    backend.close()

  }




  ignore("The user can roll back an open transaction") {

    val f                             = fixture

    val backend                       = new SqLiteBackend(f.props)

    val tableName                     = "cars_deba_a"

    val columnNames:List[String]      = List("carid","number","make","model")

    val valuesHolders:List[String]    = for (i <- (0 to (columnNames.length - 1)).toList ) yield "?"

    var sqlStatement                  = "insert into " + dbSchema + "." + tableName                     +
                                      "("                                                               +
                                      columnNames.map(i => i + ",").mkString.dropRight(1)               +
                                      ")"                                                               +
                                      " values (" + valuesHolders.map(i => i + ",").mkString.dropRight(1)  + ")"

    val patientId:String              = "Z0000050"

    val mrn:Int                       = 1234567777

    val firstName                     = "Utesta"

    val lastName                      = "Fifty"

    val valuesList                    = List(patientId,mrn,firstName,lastName)

    val bindVars:DataRow[Any]         = DataRow(("patientid",patientId),("mrn",mrn),("firstname",firstName),("lastname",lastName))

    var connectionClosed:Boolean      = false

    var dataPersistent:Boolean        = false

    given("an active connection with an open transaction ")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    backend.execute("START TRANSACTION")          //This should be replaced with backend.startTransaction when available
    val insertedRow                   = backend.executeReturningKeys(sqlStatement,bindVars)
    assert(insertedRow.isInstanceOf[DataRow[Any]])

    when("the user issues a roll back instruction")
    try
      backend.rollback()
    catch {
    case e:java.sql.SQLException =>
            fail("backend.rollback(" + "\"" + tableName + "," + dbSchema + "\"" + ")produced java.sql.SQLException" )
    }

    then("the data should not be persisted")
    backend.close()
    connectionClosed                  = backend.connection.isClosed
    connectionClosed  should be (true)
    sqlStatement                      = "select count(1) as count from " + dbSchema + "." + tableName     +
                                        " where "                                                         +
                                        " patientid  = ? "            +      " and "                      +
                                        " mrn  = ? "                  +      " and "                      +
                                        " firstname  = ? "            +      " and "                      +
                                        " lastname  = ? "

    val newFixture                            = fixture
    val newBackend                            = new SqLiteBackend(newFixture.props)
    assert(newBackend.connect().isInstanceOf[java.sql.Connection] )
    val persistentDataCount                   = newBackend.executeQuery(sqlStatement,bindVars)
    assert(persistentDataCount.next() )
    persistentDataCount.getInt("count") should equal (0)
    newBackend.close()

  }






  ignore("The user can open a transaction, insert a row, and end the transaction") {

    val f                             = fixture

    val backend                       = new SqLiteBackend(f.props)

    val tableName                     = "cars_deba_a"

    val columnNames:List[String]      = List("carid","number","make","model")

    val valuesHolders:List[String]    = for (i <- (0 to (columnNames.length - 1)).toList ) yield "?"

    var sqlStatement                  = "insert into " + dbSchema + "." + tableName                     +
                                      "("                                                               +
                                      columnNames.map(i => i + ",").mkString.dropRight(1)               +
                                      ")"                                                               +
                                      " values (" + valuesHolders.map(i => i + ",").mkString.dropRight(1)  + ")"

    val patientId:String              = "Z0000055"

    val mrn:Int                       = 1234567755

    val firstName                     = "Utesta"

    val lastName                      = "FiftyFive"

    val valuesList                    = List(patientId,mrn,firstName,lastName)

    val bindVars:DataRow[Any]         = DataRow((columnNames(0),valuesList(0)),(columnNames(1),valuesList(1)),(columnNames(2),valuesList(2)),(columnNames(3),valuesList(3)))

    var connectionClosed:Boolean      = false



    given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    when("the user issues a start transaction instruction")
    try
            backend.startTransaction()          //This should be replaced with backend.startTransaction when available
    catch {
    case    e:java.sql.SQLException =>
            println(e.getMessage)
            fail("backend.startTransaction() produced java.sql.SQLException" )
    }

    and("the user inserts a row")
    try   {
            val insertedRow                   = backend.executeReturningKeys(sqlStatement,bindVars)
            assert(insertedRow.isInstanceOf[DataRow[Any]])
    }
    catch {
    case    e:java.sql.SQLException =>
            println(e.getMessage)
            fail("backend.executeReturningKeys(" + sqlStatement + ") produced java.sql.SQLException" )
    }

    and("the user ends the transaction")
    try
            backend.endTransaction()
    catch {
    case    e:java.sql.SQLException =>
            fail("backend.endTransaction() produced java.sql.SQLException" )
    }

    then("the data should be persisted")
    backend.close()
    connectionClosed                  = backend.connection.isClosed
    connectionClosed  should be (true)
    sqlStatement                      = "select count(1) as count from " + dbSchema + "." + tableName     +
                                        " where "                                                         +
                                        " patientid  = ? "            +      " and "                      +
                                        " mrn  = ? "                  +      " and "                      +
                                        " firstname  = ? "            +      " and "                      +
                                        " lastname  = ? "

    val newFixture                            = fixture
    val newBackend                            = new SqLiteBackend(newFixture.props)
    assert(newBackend.connect().isInstanceOf[java.sql.Connection] )
    val persistentDataCount                   = newBackend.executeQuery(sqlStatement,bindVars)
    assert(persistentDataCount.next() )
    persistentDataCount.getInt("count") should equal (1)
    newBackend.close()

  }



  ignore("The user can create a table with 32 columns") {
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

    val verifyTableStatement:String           =     "SELECT count(1) as count FROM pg_tables WHERE tablename = " + "'" + tableName + "'" +  " and schemaname = " + "'" + dbSchema  + "'"

    val backend                               =     new SqLiteBackend(f.props)

    given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    when("the user issues a valid create table instruction")
    try
      backend.createTable(tableName,columnNames,dataTypes, schemaName = Option(dbSchema))
    catch {
    case e:java.sql.SQLException =>
            fail("backend.createTable(" + "\"" + tableName + "," + dbSchema + "\"" + ")produced java.sql.SQLException" )
    }


    then("the table should exist")
    val tableExistResult                                      =     backend.executeQuery(verifyTableStatement)
    assert(tableExistResult.next())
    tableExistResult.getInt("count") should equal (1)


    backend.close()
  }









  ignore("The user can insert a row without constructing an insert statement") {
    val f = fixture

    val tableName:String                      =     "cars_deba_a"

    val columnNames:List[String]              =     List("carid","number","make","model")

    val patientId:String                      =     "Z0000003"

    val mrn:Int                               =     1234567888

    val firstName:String                      =     "Utesta"

    val lastName:String                       =     "Three"

    val valuesList:List[Any]                  =     List(patientId,mrn,firstName,lastName)

    val row:DataRow[Any]                      =     DataRow(("patientid",patientId),("mrn",mrn),("firstname",firstName),("lastname",lastName))

    val backend                               =     new SqLiteBackend(f.props)

    val verifyRecordStatement:String          =     "select count(1) as count from " + dbSchema + "." + tableName + " where "    +
                                                    "patientid = " + "'" + row.patientid.get  + "'"



    given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    when("the user issues a valid insert command for an existing table and a unique record")
    var recordCountResult                         =     backend.executeQuery(verifyRecordStatement)
    assert(recordCountResult.next())
    var recordCount                               =     recordCountResult.getInt("count")
    recordCount should be (0)
    val insertedRow                               =     backend.insertReturningKeys(tableName,row,Option(dbSchema))


    then("the insert command should be successful")
    val isInstanceOfDataRow                       =     insertedRow.isInstanceOf[DataRow[Any]]
    isInstanceOfDataRow   should be (true)
    insertedRow.length    should equal (valuesList.length)


    and("the row should be inserted")
    recordCountResult                             =     backend.executeQuery(verifyRecordStatement)
    assert(recordCountResult.next())
    recordCount                                   =     recordCountResult.getInt("count")
    recordCount  should be (1)

    backend.commit()

    backend.close()

  }







  ignore("The user can insert a batch of rows and commit without having to construct the insert statements") {
    val f = fixture

    val tableName:String                      =     "cars_deba_a"

    val columnNames:List[String]              =     List("carid","number","make","model")

    val rowOne:List[Any]                      =     List("Z0000201",1234901,"Utestb","One")
    val rowTwo:List[Any]                      =     List("Z0000202",1234902,"Utestb","Two")
    val rowThree:List[Any]                    =     List("Z0000203",1234903,"Utestb","Three")
    val rowFour:List[Any]                     =     List("Z0000204",1234904,"Utestb","Four")
    val rowFive:List[Any]                     =     List("Z0000205",1234905,"Utestb","Five")
    val rowSix:List[Any]                      =     List("Z0000206",1234906,"Utestb","Six")
    val rowSeven:List[Any]                    =     List("Z0000207",1234907,"Utestb","Seven")
    val rowEight:List[Any]                    =     List("Z0000208",1234908,"Utestb","Eight")
    val rowNine:List[Any]                     =     List("Z0000209",1234909,"Utestb","Nine")
    val rowTen:List[Any]                      =     List("Z0000210",1234910,"Utestb","Ten")

    val rows                                  =     List(rowOne,rowTwo,rowThree,rowFour,rowFive,rowSix,rowSeven,rowEight,rowNine,rowTen)

    val table:DataTable[Any]                  =     DataTable(columnNames, rowOne,rowTwo,rowThree,rowFour,rowFive,rowSix,rowSeven,rowEight,rowNine,rowTen)

    val backend                               =     new SqLiteBackend(f.props)

    var successfulStatementCount:Int          =     0

    val verifyRowsStatement:String            =     "select count(1) as count from " + dbSchema + "." + tableName + " where "     +
                                                    "patientid in "                                                                           +
                                                    " ("                                                                                      +
                                                    {for (i <- 0 to (rows.length - 1)) yield "'" + rows(i).toList.head.toString + "'"}.toString().dropRight(1).drop(7) +
                                                    ")"

    given("an active connection and an empty table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    try
      backend.truncateTable(tableName, Option(dbSchema))
    catch {
    case e:java.sql.SQLException =>
            fail("backend.truncateTable(" + "\"" + tableName + "," + dbSchema + "\"" + ")produced java.sql.SQLException" )
    }

    when("the user issues a batch insert command (with commit) to insert multiple rows into the table ")
    successfulStatementCount                      =     backend.batchInsert(tableName, table, Option(dbSchema))
    try
      backend.commit()
    catch {
    case e:java.sql.SQLException =>
            fail("backend.commit() produced java.sql.SQLException" )
    }

    then("the batch insert command should be successful")
    successfulStatementCount  should equal  (rows.length)


    and("the rows should be inserted")
    val recordCountResult                         =     backend.executeQuery(verifyRowsStatement)
    assert(recordCountResult.next())
    val recordCount                               =     recordCountResult.getInt("count")
    recordCount  should be (10)

    backend.close()



  }




  ignore("The user can drop a table") {
    val f = fixture

    val tableName:String                      =     "cars_deba_c"

    val columnFixedWidth:Boolean              =     false

    val columnNames:List[String]              =     List("carid","number","make","model")

    val dataTypes                             =     List(CharacterDataType(20,columnFixedWidth),IntegerDataType(),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth))

    val verifyTableStatement:String           =     "SELECT tablename FROM pg_tables WHERE tablename = " + "'" + tableName + "'" +  " and schemaname = " + "'" + dbSchema  + "'"

    val backend                               =     new SqLiteBackend(f.props)

    var tableCreated:Boolean                  =     false

    var tableVerified:Boolean                 =     false

    var tableDropped:Boolean                  =     false

    given("an active connection and an existing table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)
    try
      backend.createTable(tableName,columnNames,dataTypes, Option(dbSchema))
    catch {
    case e:java.sql.SQLException =>
            fail("backend.createTable(" + "\"" + tableName + "," + dbSchema + "\"" + ")produced java.sql.SQLException" )
    }
    tableVerified                             =     backend.execute(verifyTableStatement)
    tableVerified should be (true)

    when("the user issues a drop table command for that table")
    try
      backend.dropTable(tableName, schemaName = Option(dbSchema))
    catch {
    case e:java.sql.SQLException =>
            fail("backend.dropTable(" + "\"" + tableName + "," + dbSchema + "\"" + ")produced java.sql.SQLException" )
    }


    then("the table should be dropped")
    val tableExistResult                    =     backend.executeQuery(verifyTableStatement)
    val tableExist                          =     tableExistResult.next()
    tableExist should be (false)

    backend.close()

  }






  ignore("The user can drop a table with cascade") {
    val f = fixture

    val tableName:String                      =     "cars_deba_c"

    val viewName:String                       =     "cars_deba_c_v"

    val columnFixedWidth:Boolean              =     false

    val columnNames:List[String]              =     List("carid","number","make","model")

    val dataTypes                             =     List(CharacterDataType(20,columnFixedWidth),IntegerDataType(),CharacterDataType(20,columnFixedWidth),CharacterDataType(20,columnFixedWidth))

    val verifyTableStatement:String           =     "SELECT count(1) as count FROM pg_tables WHERE tablename = " + "'" + tableName + "'" +  " and schemaname = " + "'" + dbSchema  + "'"

    val createViewStatement:String            =     "create view " + dbSchema + "." + viewName + " as "             +
                                                    "select * from " + dbSchema + "." + tableName

    val verifyViewStatement:String            =     "SELECT count(1) as count FROM pg_views WHERE viewname = " + "'" + viewName + "'" +  " and schemaname = " + "'" + dbSchema  + "'"

    val backend                               =     new SqLiteBackend(f.props)



    val cascade                               =     true

    given("an active connection, an existing table, and a view on the existing table")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    try
      backend.createTable(tableName,columnNames,dataTypes, Option(dbSchema))
    catch {
    case  e:java.sql.SQLException =>
          println(e.getMessage)
          fail("backend.createTable(" + "\"" + tableName + "," + dbSchema + "\"" + ")produced java.sql.SQLException" )
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



    when("the user issues a drop table command with cascade for that table")
    try
      backend.dropTable(tableName, cascade, schemaName = Option(dbSchema))
    catch {
    case e:java.sql.SQLException =>
            println(e.getMessage)
            fail("backend.dropTable(" + "\"" + tableName + "," + dbSchema + "\"" + ")produced java.sql.SQLException" )
    }


    then("the table and its associated view should be dropped")
    val tableExistResult                    =     backend.executeQuery(verifyTableStatement)
    assert(tableExistResult.next())
    tableExistResult.getInt("count")  should be (0)


    val viewExistResult                     =     backend.executeQuery(verifyViewStatement)
    assert(viewExistResult.next())
    viewExistResult.getInt("count")   should be (0)


    backend.close()

  }





  ignore("The user can iterate over the results of a select query") {
    //Prerequisites:  Need Multiple Row in table cars_deba_a

    val f                     = fixture

    val backend               = new SqLiteBackend(f.props)

    val tableName             = "cars_deba_a"

    val sqlStatement          = "select * from " + dbSchema + "." + tableName

    val bindVars:DataRow[String]                  = DataRow.empty

    var resultsCount:Int                          = 0

    given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    when("a query that should generate multiple results is executed")
    val resultSet = backend.executeQuery(sqlStatement,bindVars)

    then("the user should be able to iterate over the results")
    while (resultSet.next()) { resultsCount+=1 }


    and("multiple results should be returned")
    resultsCount should be > (1)

    backend.close()
  }




  ignore("The user can update a record in a table using a valid update statement") {
    //Prerequisites:  Need Multiple Row in table cars_deba_a

    val f                             = fixture

    val backend                       = new SqLiteBackend(f.props)

    val tableName                     = "cars_deba_a"

    var columnNames:List[String]      = List("carid","number","make","model")


    val patientId:String              = "Z0000210"

    val mrn:Int                       = 1234567899

    val firstName                     = "Utesta"

    val lastName                      = "FourteenMillion"

    var sqlStatement                  = "update " + dbSchema + "." + tableName                            +
                                        " set "                                                           +
                                        columnNames.map(i => i + " = ?,").mkString.dropRight(1)           +
                                        " where patientid = " + "'" + patientId  + "'"

    var valuesList:List[Any]          = List(patientId,mrn,firstName,lastName,patientId)

    var bindVars:DataRow[Any]         = DataRow(("patientid",patientId),("mrn",mrn),("firstname",firstName),("lastname",lastName))

    var resultsCount:Int              = 0

    given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    when("an update query is executed")
    try
      backend.execute(sqlStatement,bindVars)
    catch {
    case e:java.sql.SQLException =>
            fail("backend.execute(" + "\"" + sqlStatement + "\"" + ")produced java.sql.SQLException" )
    }


    then("the record(s) should be updated")
    columnNames                                   = List("lastname")
    valuesList                                    = List(lastName)
    bindVars                                      = DataRow(("lastname",lastName))
    sqlStatement                                  = "select count(1) as count from " + dbSchema + "."  + tableName + " where "     +
                                                    "lastname = ?"

    val recordCountResult                         = backend.executeQuery(sqlStatement,bindVars)
    assert(recordCountResult.next())
    resultsCount                                  = recordCountResult.getInt("count")
    resultsCount  should be (1)


    backend.close()
  }


  ignore("The user can update a multiple records in a table using a valid update statement") {
    //Prerequisites:  Need Multiple Row in table cars_deba_a

    val f                             = fixture

    val backend                       = new SqLiteBackend(f.props)

    val tableName                     = "cars_deba_a"

    var columnNames:List[String]      = List("lastname")

    var sqlStatement                  = "update " + dbSchema + "." + tableName                            +
                                        " set "                                                           +
                                        columnNames.map(i => i + " = ?,").mkString.dropRight(1)


    val lastName                                  = "SeventeenMillion"

    val valuesList:List[Any]                      = List(lastName)

    val bindVars:DataRow[Any]                     = DataRow((columnNames(0),lastName))

    var resultsCount:Int                          = 0

    given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    when("an update query for multiple records is executed")
    try
      backend.execute(sqlStatement,bindVars)
    catch {
    case e:java.sql.SQLException =>
            fail("backend.execute(" + "\"" + sqlStatement + "\"" + ")produced java.sql.SQLException" )
    }

    then("multiple record(s) should be updated")
    sqlStatement                                  = "select count(1) as count from " + dbSchema + "." + tableName + " where "     +
                                                    "lastname = ?"

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





  ignore("The user can update a multiple records in a table without constructing update statement") {
    //Prerequisites:  Need Multiple Row in table cars_deba_a   with firstname = 'Utestb'

    val f                                         = fixture

    val backend                                   = new SqLiteBackend(f.props)

    val tableName                                 = "cars_deba_a"

    var columnNames:List[String]                  = List("mrn","firstname","lastname")

    val mrn:Int                                   = 192837465

    val firstName                                 = "Utesta@chop.cbmi"

    val lastName                                  = "SeventeenMillion"

    val valuesList:List[Any]                      = List(mrn,firstName,lastName)

    val filter:List[(String, Any)]                = List(("firstname","Utestb"))

    val updatesBindVars:DataRow[Any]              =
      DataRow((columnNames(0),valuesList(0)),(columnNames(1),valuesList(1)),(columnNames(2),valuesList(2)))

    var resultsCount:Int                          = 0

    given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    when("an update row instruction for multiple records is executed")
    try
      backend.updateRow(tableName,updatesBindVars,filter,Option(dbSchema))
    catch {
    case e:java.sql.SQLException =>
            println(e.getMessage())
            fail("backend.updateRow()produced java.sql.SQLException" )
    }

    then("multiple record(s) should be updated")
    val sqlStatement                              = "select count(1) as count from " + dbSchema + "." + tableName + " where "     +
                                                    "firstname = ?"


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





  ignore("The user can insert a multiple rows using a loop without constructing an insert statement") {
    //Prerequisites:  None of theses record should exist
    val f = fixture

    val tableName:String                      =     "cars_deba_a"

    val columnNames:List[String]              =     List("carid","number","make","model")

    val patientIds:List[String]               =     List("Z0000500","Z0000501","Z0000502","Z0000503","Z0000504")

    val mrns:List[Int]                        =     List(1234561000,1234561001,1234561002,1234561003,1234561004)

    val firstNames:List[String]               =     List("Utestd","Utestd","Utestd","Utestd","Utestd")

    val lastNames:List[String]                =     List("Zero","One","Ten","Ten","Fen")

    val backend                               =     new SqLiteBackend(f.props)

    val verifyRecordsStatement:String         =     "select count(1) as count from " + dbSchema + "." + tableName + " where "          +
                                                    "patientid in "                                                   +
                                                    "("                                                               +
                                                    patientIds.map(i => "'" + i + "'" + ",").mkString.dropRight(1)             +
                                                    ")"

    var recordCount:Int                       =     0

    given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    when("the user issues valid insert row commands in a loop for an existing table")

    for (i <- 0 to (patientIds.length -1))
    {

      var row:DataRow[Any]                     =     DataRow(("patientid",patientIds(i)),("mrn",mrns(i)),("firstname",firstNames(i)),("lastname",lastNames(i)))

      assert(backend.insertReturningKeys(tableName,row, schemaName = Option(dbSchema)).isInstanceOf[DataRow[Any]]  )


    }


    then("the rows should be inserted")
    val recordCountResult                       =     backend.executeQuery(verifyRecordsStatement)
    assert(recordCountResult.next())
    recordCount                                 =     recordCountResult.getInt("count")
    recordCount  should equal (patientIds.length)

    backend.close()

  }





  ignore("The user can delete multiple records in a table using a valid delete statement") {
    //Prerequisites:  Need Multiple Row in table cars_deba_a

    val f                             = fixture

    val backend                       = new SqLiteBackend(f.props)

    val tableName                     = "cars_deba_a"

    val columnNames:List[String]      = List("lastname")

    var sqlStatement                  = if    (columnNames.length > 1)  {
                                          "delete from " + dbSchema + "." + tableName                                 +
                                          " where "                                                                   +
                                          columnNames.map(i => i + " = ?").mkString(" and ")
                                        }
                                        else{
                                          "delete from " + dbSchema + "." + tableName                                 +
                                          " where "                                                                   +
                                          columnNames.map(i => i + " = ?").mkString
                                        }

    val lastName                                  = "Ten"

    val valuesList:List[Any]                      = List(lastName)

    val bindVars:DataRow[Any]                     = DataRow((columnNames(0),valuesList(0)))



    given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    when("a delete query for multiple records is executed")
    try
      backend.execute(sqlStatement,bindVars)
    catch {
    case e:java.sql.SQLException =>
            println(e.getMessage)
            fail("backend.execute(" + "\"" + sqlStatement + "\"" + ")produced java.sql.SQLException" )
    }

    then ("those records should be deleted")
    sqlStatement                      =   "select count(1) as count from "   + dbSchema + "." + tableName             +
                                          " where lastname = ?"

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






  ignore("The user can drop all tables that begin with a certain string") {

    /*http://stackoverflow.com/questions/3476765/mysql-drop-all-tables-ignoring-foreign-keys*/

    val f = fixture

    val searchString:String                   =         "cars_deba"


    val oldobjectStatement:String                =      "select objectname from "                                                                                                                         +
                                                        "("                                                                                                                                               +
                                                        "(select tablename 	as objectname from pg_tables 	WHERE    tablename like '" + searchString + "%'" + " and schemaname = '" + dbSchema + "')"      +
                                                        " UNION ALL     "                                                                                                                                 +
                                                        "(select viewname 	as objectname from pg_views 	WHERE    viewname like '" + searchString + "%'" + " and schemaname = '" + dbSchema + "')"       +
                                                        ") a"



    val oldverifyObjectCountStatement:String     =      "select count(objectname) as count from "                                                                                                         +
                                                        "("                                                                                                                                               +
                                                        "(select tablename 	as objectname from pg_tables 	WHERE    tablename like '" + searchString + "%'" + " and schemaname = '" + dbSchema + "')"      +
                                                        " UNION ALL     "                                                                                                                                 +
                                                        "(select viewname 	as objectname from pg_views 	WHERE    viewname like '" + searchString + "%'" + " and schemaname = '" + dbSchema + "')"       +
                                                        ") a"



    val objectStatement:String                =         "select objectname from "                                                                                                                         +
                                                        "("                                                                                                                                               +
                                                        "(select tablename 	as objectname from pg_tables 	WHERE    tablename like '" + searchString + "%'" + " and schemaname = '" + dbSchema + "')"      +
                                                        ") a"



    val verifyObjectCountStatement:String     =         "select count(objectname) as count from "                                                                                                         +
                                                        "("                                                                                                                                               +
                                                        "(select tablename 	as objectname from pg_tables 	WHERE    tablename like '" + searchString + "%'" + " and schemaname = '" + dbSchema + "')"      +
                                                        ") a"




    val backend                               =         new SqLiteBackend(f.props)

    val cascade:Boolean                       =         true

    given("an active connection")
    assert(backend.connect().isInstanceOf[java.sql.Connection] )
    backend.connection.setAutoCommit(false)

    when("the user issues drop table commands for tables that begin with a certain string")
    try {

    val  objectResult                         =         backend.executeQuery(objectStatement)

        try {

          while(objectResult.next())  {
            backend.dropTable(objectResult.getString("objectname"), cascade, schemaName = Option(dbSchema))

          }

        objectResult.close()
        }
        catch {
        case e:java.sql.SQLException =>
            println(e.getMessage)
            fail("backend.dropTable(" + "\"" + dbSchema + "," + objectResult.getString("objectname") + "\"" + ")produced java.sql.SQLException" )
        }
    }
    catch {
    case e:java.sql.SQLException =>
            println(e.getMessage)
            fail("backend.executeQuery(" + objectStatement + ")produced java.sql.SQLException" )
    }


    then("then those tables should be dropped")
    val objectExistResult                      =     backend.executeQuery(verifyObjectCountStatement)
    assert(objectExistResult.next())
    objectExistResult.getInt("count")  should be (0)

    backend.commit()

    backend.close()

  }


}

