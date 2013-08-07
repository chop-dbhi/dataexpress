package edu.chop.cbmi.dataExpress.test.util.cars.dataSetup.backends

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 3/28/12
 * Time: 4:51 PM
 * To change this template use File | Settings | File Templates.
 */

class PostgresDataSetup {

  var sourceDbUserName                = ""

  var targetDbUserName                = ""

  var sourceIdentifierQuote           = ""

  var targetIdentifierQuote           = ""

  var sourceDBSchemaName              = ""

  var targetDBSchemaName              = ""

  def createSourceSchema(): String   = {

    lazy val createSourceSchemaStatement     = "CREATE SCHEMA " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote

    createSourceSchemaStatement
  }

  def createTargetSchema(): String   = {

    lazy val createTargetSchemaStatement     = "CREATE SCHEMA " + targetIdentifierQuote + targetDBSchemaName + targetIdentifierQuote

    createTargetSchemaStatement
  }


  def dropSourceSchema(): String   = {

    lazy val dropSourceSchemaStatement     = "DROP SCHEMA IF EXISTS "  + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote + " CASCADE"

    dropSourceSchemaStatement
  }


  def dropTargetSchema(): String   = {

    lazy val dropTargetSchemaStatement     = "DROP SCHEMA IF EXISTS "  + sourceIdentifierQuote + targetDBSchemaName + sourceIdentifierQuote + " CASCADE"

    dropTargetSchemaStatement
  }

}


class OracleDataSetup {

  var sourceDbUserName                = ""

  var targetDbUserName                = ""

  var sourceIdentifierQuote           = ""

  var targetIdentifierQuote           = ""

  var sourceDBSchemaName              = ""

  var targetDBSchemaName              = ""

  def createSourceSchema()    = {

    //lazy  val  createSourceSchemaStatement     =   "CREATE USER qe10 IDENTIFIED BY qe10 DEFAULT TABLESPACE APEX_00000001"

    lazy  val  createSourceSchemaStatement      =   "CREATE USER qe10c01 IDENTIFIED BY qe10c01"

    lazy  val  setQuotaStatement                =   "ALTER USER qe10c01 QUOTA 100M ON SYSTEM"

    lazy  val  grantCreateSessionStatement      =   "grant create session to qe10c01"

    lazy  val  grantCreateTableStatement        =   "grant create table to qe10c01"

    lazy  val  grantAllPrivilegesStatement      =   "grant all on qe10c01 to qe10c01"


    lazy  val createSourceSchemaStatementMap   =
      Map (
        "createSourceSchemaStatement" -> createSourceSchemaStatement,
        "setQuotaStatement" -> setQuotaStatement,
        "grantCreateSessionStatement" -> grantCreateSessionStatement//,    Not Needed in Oracle
        //"grantCreateTableStatement" -> grantCreateTableStatement,
        //"grantAllPrivilegesStatement" -> grantAllPrivilegesStatement
      )

    createSourceSchemaStatementMap
  }

  //  Source and Target Schema are the same for this test suite
  //  This method will not be used:
  def createTargetSchema()    = {

    //lazy  val  createSourceSchemaStatement     =   "CREATE USER qe10 IDENTIFIED BY qe10 DEFAULT TABLESPACE APEX_00000001"
    lazy  val  createSourceSchemaStatement      =   "CREATE USER qe10c01 IDENTIFIED BY qe10c01"
    lazy  val  setQuotaStatement                =   "ALTER USER qe10c01 QUOTA 100M ON SYSTEM"
    lazy  val  grantCreateSessionStatement      =   "grant create session to qe10c01"
    lazy  val  grantCreateTableStatement        =   "grant create table to qe10c01"
    lazy  val  grantAllPrivilegesStatement      =   "grant all on qe10c01 to qe10c01"


    lazy  val createSourceSchemaStatementMap   =
      Map (
        "createSourceSchemaStatement" -> createSourceSchemaStatement,
        "setQuotaStatement" -> setQuotaStatement,
        "grantCreateSessionStatement" -> grantCreateSessionStatement//,    Not Needed in Oracle
        //"grantCreateTableStatement" -> grantCreateTableStatement,
        //"grantAllPrivilegesStatement" -> grantAllPrivilegesStatement
      )

    createSourceSchemaStatementMap
  }

  def dropSourceSchema(): String   = {

    lazy val dropSourceSchemaStatement     = "DROP USER  "  + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote + " CASCADE"

    dropSourceSchemaStatement
  }

  //  Will Not be Used in this test suite
  def dropTargetSchema(): String   = {

    lazy val dropTargetSchemaStatement     = "DROP USER  "  + targetIdentifierQuote + targetDBSchemaName + targetIdentifierQuote + " CASCADE"

    dropTargetSchemaStatement
  }

}


class MySqlDataSetup {

  var sourceDbUserName                = ""
  var targetDbUserName                = ""
  var sourceIdentifierQuote           = ""
  var targetIdentifierQuote           = ""
  var sourceDBSchemaName              = ""
  var targetDBSchemaName              = ""

  def createSourceSchema(): String   = {
    lazy val createSourceSchemaStatement     = "CREATE SCHEMA " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote
    createSourceSchemaStatement
  }

  def   createTargetSchema(): String   = {
    lazy val createTargetSchemaStatement     = "CREATE SCHEMA " + targetIdentifierQuote + targetDBSchemaName + targetIdentifierQuote
    createTargetSchemaStatement
  }

  def dropSourceSchema(): String   = {
    lazy val dropSourceSchemaStatement     = "DROP SCHEMA IF EXISTS "  + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote
    dropSourceSchemaStatement
  }


  def dropTargetSchema(): String   = {
    lazy val dropTargetSchemaStatement     = "DROP SCHEMA IF EXISTS "  + targetIdentifierQuote + targetDBSchemaName + targetIdentifierQuote
    dropTargetSchemaStatement
  }

}




