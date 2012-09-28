	package edu.chop.cbmi.dataExpress.test.util.cars.dataSetup.api

/**
 * Created by IntelliJ IDEA.
 * User: davidsonl2
 * Date: 3/28/12
 * Time: 4:51 PM
 * To change this template use File | Settings | File Templates.
 */

class PostgresPostgresDataSetup {

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

  lazy val createSourceTableStatement      =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                          +
      "."                                                                                                         +
      sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                       +
      """(carid character varying(20),    number_1_0 numeric(1,0), number_4_2 numeric(4,2),
      number_12_5 numeric(12,5),    char_1 character(1),    char_1000 character(1000),
      char_2000 character(2000),    varchar2_1 character varying(1),    varchar2_1000 character varying(1000),
      varchar2_4000 character varying(4000),    "time" time without time zone,    timetz time with time zone,
      "timestamp" timestamp without time zone,    timestamptz timestamp with time zone,    "interval" interval,
      tinterval tinterval,   date date,   float4 real,
      float8 double precision)"""

  lazy val setSourceTableUserStatement     =    "ALTER TABLE qe10c01.cars_deapi_01 OWNER TO qe10user01"
    //"ALTER TABLE " + sourceDBSchemaName + "." + sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "   +
    //  "OWNER TO " + sourceDbUserName

  lazy val sourceDataInsertsList: List[String]           =
    List(
    "INSERT INTO "                                                                                                                            +
      sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
      "."                                                                                                                                     +
      sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
      "(carid, number_1_0, number_4_2, number_12_5, char_1, char_1000, char_2000, varchar2_1, varchar2_1000, "                                +
      "varchar2_4000, \"time\", timetz, \"timestamp\", timestamptz, \"interval\", tinterval, date, float4, float8)"                           +
      "VALUES"                                                                                                                                +
      "('Z0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff', 'p', 'Z0000500_varchar2_data',   "                                      +
      "'Z0000500_varchar2_dataZ0000500_varchar2_dataZ0000500_varchar2_dataZ0000500_varchar2_dataZ0000500_varchar2_dataZ0000500_varchar2_dataZ0000500_varchar2_dataZ0000500_varchar2_dataZ0000500_varchar2_dataZ0000500_varchar2_data', "  +
      "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
    "INSERT INTO "                                                                                                                            +
      sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
      "."                                                                                                                                     +
      sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
      "(carid, number_1_0, number_4_2, number_12_5, char_1, char_1000, char_2000, varchar2_1, varchar2_1000,"                                 +
      "varchar2_4000, \"time\", timetz, \"timestamp\", timestamptz, \"interval\", tinterval, date, float4, float8)"                           +
      "VALUES ('Z0000502', 0, 10.21, 0.00000, 'c', 'ccccccc', 'fff', 'p', 'Z0000502_varchar2_data',"                                          +
      "'Z0000502_varchar2_dataZ0000502_varchar2_dataZ0000502_varchar2_dataZ0000502_varchar2_dataZ0000502_varchar2_dataZ0000502_varchar2_dataZ0000502_varchar2_dataZ0000502_varchar2_dataZ0000502_varchar2_dataZ0000502_varchar2_data', "  +
      "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
    "INSERT INTO "                                                                                                                            +
      sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
      "."                                                                                                                                     +
      sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
      "(carid, number_1_0, number_4_2, number_12_5, char_1, char_1000, char_2000, varchar2_1, "                                               +
      "varchar2_1000, varchar2_4000, \"time\", timetz, \"timestamp\", timestamptz, \"interval\", tinterval, date, float4, float8) "           +
      "VALUES ('Z0000504', 9, 19.21, 1234567.90123, 'e', 'eeeeeee', 'ff', 'p', 'Z0000504_varchar2_data', "                                    +
      "'Z0000504_varchar2_dataZ0000504_varchar2_dataZ0000504_varchar2_dataZ0000504_varchar2_dataZ0000504_varchar2_dataZ0000504_varchar2_dataZ0000504_varchar2_dataZ0000504_varchar2_dataZ0000504_varchar2_dataZ0000504_varchar2_data',"   +
      "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
    "INSERT INTO "                                                                                                                            +
      sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
      "."                                                                                                                                     +
      sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
      "(carid, number_1_0, number_4_2, number_12_5, char_1, char_1000, char_2000, varchar2_1, "                                               +
      "varchar2_1000, varchar2_4000, \"time\", timetz, \"timestamp\", timestamptz, \"interval\", tinterval, date, float4, float8) "           +
      "VALUES ('Z0000501', -1, -11.21, -1234500.90123, 'b', 'bbbbbb', 'ff', 'p', NULL,"                                                       +
      "'Z0000501_varchar2_dataZ0000501_varchar2_dataZ0000501_varchar2_dataZ0000501_varchar2_dataZ0000501_varchar2_dataZ0000501_varchar2_dataZ0000501_varchar2_dataZ0000501_varchar2_dataZ0000501_varchar2_dataZ0000501_varchar2_data', "  +
      "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
    "INSERT INTO "                                                                                                                            +
      sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
      "."                                                                                                                                     +
      sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
      "(carid, number_1_0, number_4_2, number_12_5, char_1, char_1000, char_2000, varchar2_1, "                                               +
      "varchar2_1000, varchar2_4000, \"time\", timetz, \"timestamp\", timestamptz, \"interval\", tinterval, date, float4, float8) "           +
      "VALUES ('Z0000503', 1, 11.21, 1234567.90123, 'd', 'dddddd', 'ff', 'p', 'Z0000503_varchar2_data',"                                      +
      "'Z0000503_varchar2_dataZ0000503_varchar2_dataZ0000503_varchar2_dataZ0000503_varchar2_dataZ0000503_varchar2_dataZ0000503_varchar2_dataZ0000503_varchar2_dataZ0000503_varchar2_dataZ0000503_varchar2_dataZ0000503_varchar2_data', "  +
      "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
    )

  def dropSourceSchema(): String   = {

    lazy val dropSourceSchemaStatement     = "DROP SCHEMA IF EXISTS "  + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote + " CASCADE"

    dropSourceSchemaStatement
  }


  def dropTargetSchema(): String   = {

    lazy val dropTargetSchemaStatement     = "DROP SCHEMA IF EXISTS "  + sourceIdentifierQuote + targetDBSchemaName + sourceIdentifierQuote + " CASCADE"

    dropTargetSchemaStatement
  }

}


class OraclePostgresDataSetup {

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

  def createTargetSchema(): String   = {

    lazy val createTargetSchemaStatement     = "CREATE SCHEMA " + targetIdentifierQuote + targetDBSchemaName + targetIdentifierQuote

    createTargetSchemaStatement
  }

  lazy val table01Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"number_1_0\" NUMBER(1,0),"                                                                                     +
      "\"number_4_2\" NUMBER(4,2),"                                                                                     +
      "\"number_12_5\" NUMBER(12,5),"                                                                                   +
      "\"char_1\" CHAR(1),"                                                                                             +
      "\"char_1000\" CHAR(1000),"                                                                                       +
      "\"char_2000\" CHAR(2000),"                                                                                       +
      "\"timestamp\" TIMESTAMP (6),"                                                                                    +
      "\"timestamptimezone\" TIMESTAMP (6) WITH TIME ZONE,"                                                             +
      "\"timestamplocaltimezone\" TIMESTAMP (6) WITH LOCAL TIME ZONE,"                                                  +
      "\"varchar2_1\" VARCHAR2(1),"                                                                                     +
      "\"varchar2_1000\" VARCHAR2(1000),"                                                                               +
      "\"varchar2_4000\" VARCHAR2(4000),"                                                                               +
      "\"binaryfloat\" BINARY_FLOAT,"                                                                                   +
      "\"binarydouble\" BINARY_DOUBLE,"                                                                                 +
      "\"float_1\" FLOAT(1),"                                                                                           +
      "\"float_25\" FLOAT(25),"                                                                                         +
      "\"float_53\" FLOAT(53),"                                                                                         +
      "\"float_100\" FLOAT(100),"                                                                                       +
      "\"float_126\" FLOAT(126),"                                                                                       +
      "\"date\"      DATE"                                                                                              +
      ")"

  lazy val table02Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_02" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"long_data\" LONG"                                                                                              +
      ")"

  lazy val table03Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_03" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"longraw_data\" LONG RAW"                                                                                       +
      ")"

  lazy val table04Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_04" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"blob\" BLOB,"                                                                                                  +
      "\"clob\" CLOB"                                                                                                   +
      ")"

  lazy val table05Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_05" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"double_1\" DOUBLE PRECISION"                                                                                   +
      ")"

  lazy val createSourceTablesStatements      =
    Map  (
        "table_01" -> table01Statement,
        "table_02" -> table02Statement,
        "table_03" -> table03Statement,
        "table_04" -> table04Statement,
        "table_05" -> table05Statement
    )


  lazy val sourceDataInsertsList: List[String]           =
    List(
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))"""
    )

  def dropSourceSchema(sourceDBSchemaName:String): String   = {

    lazy val dropSourceSchemaStatement     = "DROP USER  "  + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote + " CASCADE"

    dropSourceSchemaStatement
  }


  def dropTargetSchema(targetDBSchemaName:String): String   = {

    lazy val dropTargetSchemaStatement     = "DROP SCHEMA IF EXISTS "  + sourceIdentifierQuote + targetDBSchemaName + sourceIdentifierQuote + " CASCADE"

    dropTargetSchemaStatement
  }

}


class OracleOracleDataSetup {

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

  lazy val table01Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"number_1_0\" NUMBER(1,0),"                                                                                     +
      "\"number_4_2\" NUMBER(4,2),"                                                                                     +
      "\"number_12_5\" NUMBER(12,5),"                                                                                   +
      "\"char_1\" CHAR(1),"                                                                                             +
      "\"char_1000\" CHAR(1000),"                                                                                       +
      "\"char_2000\" CHAR(2000),"                                                                                       +
      "\"timestamp\" TIMESTAMP (6),"                                                                                    +
      "\"timestamptimezone\" TIMESTAMP (6) WITH TIME ZONE,"                                                             +
      "\"timestamplocaltimezone\" TIMESTAMP (6) WITH LOCAL TIME ZONE,"                                                  +
      "\"varchar2_1\" VARCHAR2(1),"                                                                                     +
      "\"varchar2_1000\" VARCHAR2(1000),"                                                                               +
      "\"varchar2_4000\" VARCHAR2(4000),"                                                                               +
      "\"binaryfloat\" BINARY_FLOAT,"                                                                                   +
      "\"binarydouble\" BINARY_DOUBLE,"                                                                                 +
      "\"float_1\" FLOAT(1),"                                                                                           +
      "\"float_25\" FLOAT(25),"                                                                                         +
      "\"float_53\" FLOAT(53),"                                                                                         +
      "\"float_100\" FLOAT(100),"                                                                                       +
      "\"float_126\" FLOAT(126),"                                                                                       +
      "\"date\"      DATE"                                                                                              +
      ")"

  lazy val table02Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_02" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"long_data\" LONG"                                                                                              +
      ")"

  lazy val table03Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_03" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"longraw_data\" LONG RAW"                                                                                       +
      ")"

  lazy val table04Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_04" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"blob\" BLOB,"                                                                                                  +
      "\"clob\" CLOB"                                                                                                   +
      ")"

  lazy val table05Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_05" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"double_1\" DOUBLE PRECISION"                                                                                   +
      ")"

  lazy val createSourceTablesStatements      =
    Map  (
      "table_01" -> table01Statement,
      "table_02" -> table02Statement,
      "table_03" -> table03Statement,
      "table_04" -> table04Statement,
      "table_05" -> table05Statement
    )


  lazy val sourceDataInsertsList: List[String]           =
    List(
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))"""
    )

  def dropSourceSchema(sourceDBSchemaName:String): String   = {

    lazy val dropSourceSchemaStatement     = "DROP USER  "  + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote + " CASCADE"

    dropSourceSchemaStatement
  }

  //  Will Not be Used in this test suite
  def dropTargetSchema(targetDBSchemaName:String): String   = {

    lazy val dropTargetSchemaStatement     = "DROP USER  "  + targetIdentifierQuote + targetDBSchemaName + targetIdentifierQuote + " CASCADE"

    dropTargetSchemaStatement
  }

}


class OracleMySqlDataSetup {

  var sourceDbUserName                  = ""

  var targetDbUserName                  = ""

  var sourceIdentifierQuote             = ""

  var targetIdentifierQuote             = ""

  var sourceDBSchemaName                = ""

  var targetDBSchemaName                = ""

  var  sourceTableNames: Seq[String]    =   Seq("cars_deapi_10")

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

  def createTargetSchema(): String   = {

    lazy val createTargetSchemaStatement     = "CREATE SCHEMA " + targetIdentifierQuote + targetDBSchemaName + targetIdentifierQuote

    createTargetSchemaStatement
  }

  lazy val table01Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + sourceTableNames(0) + sourceIdentifierQuote + " "                                         +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"number_1_0\" NUMBER(1,0),"                                                                                     +
      "\"number_4_2\" NUMBER(4,2),"                                                                                     +
      "\"number_12_5\" NUMBER(12,5),"                                                                                   +
      "\"char_1\" CHAR(1),"                                                                                             +
      "\"char_1000\" CHAR(1000),"                                                                                       +
      "\"char_2000\" CHAR(2000),"                                                                                       +
      "\"timestamp\" TIMESTAMP (6),"                                                                                    +
      "\"timestamptimezone\" TIMESTAMP (6) WITH TIME ZONE,"                                                             +
      "\"timestamplocaltimezone\" TIMESTAMP (6) WITH LOCAL TIME ZONE,"                                                  +
      "\"varchar2_1\" VARCHAR2(1),"                                                                                     +
      "\"varchar2_1000\" VARCHAR2(1000),"                                                                               +
      "\"varchar2_4000\" VARCHAR2(4000),"                                                                               +
      "\"binaryfloat\" BINARY_FLOAT,"                                                                                   +
      "\"binarydouble\" BINARY_DOUBLE,"                                                                                 +
      "\"float_1\" FLOAT(1),"                                                                                           +
      "\"float_25\" FLOAT(25),"                                                                                         +
      "\"float_53\" FLOAT(53),"                                                                                         +
      "\"float_100\" FLOAT(100),"                                                                                       +
      "\"float_126\" FLOAT(126),"                                                                                       +
      "\"date\"      DATE"                                                                                              +
      ")"

  lazy val table02Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_02" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"long_data\" LONG"                                                                                              +
      ")"

  lazy val table03Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_03" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"longraw_data\" LONG RAW"                                                                                       +
      ")"

  lazy val table04Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_04" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"blob\" BLOB,"                                                                                                  +
      "\"clob\" CLOB"                                                                                                   +
      ")"

  lazy val table05Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_05" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"double_1\" DOUBLE PRECISION"                                                                                   +
      ")"

  lazy val createSourceTablesStatements      =
    Map  (
      "table_01" -> table01Statement,
      "table_02" -> table02Statement,
      "table_03" -> table03Statement,
      "table_04" -> table04Statement,
      "table_05" -> table05Statement
    )


  lazy val sourceDataInsertsList: List[String]           =
    List(
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + sourceTableNames(0) + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + sourceTableNames(0) + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + sourceTableNames(0) + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + sourceTableNames(0) + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + sourceTableNames(0) + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))"""
    )

  def dropSourceSchema(sourceDBSchemaName:String): String   = {

    lazy val dropSourceSchemaStatement     = "DROP USER  "  + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote + " CASCADE"

    dropSourceSchemaStatement
  }


  def dropTargetSchema(targetDBSchemaName:String): String   = {

    lazy val dropTargetSchemaStatement     = "DROP SCHEMA IF EXISTS "  + targetIdentifierQuote + targetDBSchemaName + targetIdentifierQuote

    dropTargetSchemaStatement
  }

}


class OracleSqlServerDataSetup {

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


  def createTargetSchema()    = {

    lazy  val  createTargetSchemaStatement      =   "CREATE SCHEMA " + targetIdentifierQuote + targetDBSchemaName + targetIdentifierQuote

    //lazy  val  goStatement                      =   "GO"





    lazy  val createTargetSchemaStatementMap   =
      Map (
        "createSourceSchemaStatement" ->  createTargetSchemaStatement
        //"goStatement"                 ->  goStatement

      )

    createTargetSchemaStatementMap
  }

  lazy val table01Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"number_1_0\" NUMBER(1,0),"                                                                                     +
      "\"number_4_2\" NUMBER(4,2),"                                                                                     +
      "\"number_12_5\" NUMBER(12,5),"                                                                                   +
      "\"char_1\" CHAR(1),"                                                                                             +
      "\"char_1000\" CHAR(1000),"                                                                                       +
      "\"char_2000\" CHAR(2000),"                                                                                       +
      "\"timestamp\" TIMESTAMP (6),"                                                                                    +
      "\"timestamptimezone\" TIMESTAMP (6) WITH TIME ZONE,"                                                             +
      "\"timestamplocaltimezone\" TIMESTAMP (6) WITH LOCAL TIME ZONE,"                                                  +
      "\"varchar2_1\" VARCHAR2(1),"                                                                                     +
      "\"varchar2_1000\" VARCHAR2(1000),"                                                                               +
      "\"varchar2_4000\" VARCHAR2(4000),"                                                                               +
      "\"binaryfloat\" BINARY_FLOAT,"                                                                                   +
      "\"binarydouble\" BINARY_DOUBLE,"                                                                                 +
      "\"float_1\" FLOAT(1),"                                                                                           +
      "\"float_25\" FLOAT(25),"                                                                                         +
      "\"float_53\" FLOAT(53),"                                                                                         +
      "\"float_100\" FLOAT(100),"                                                                                       +
      "\"float_126\" FLOAT(126),"                                                                                       +
      "\"date\"      DATE"                                                                                              +
      ")"

  lazy val table02Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_02" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"long_data\" LONG"                                                                                              +
      ")"

  lazy val table03Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_03" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"longraw_data\" LONG RAW"                                                                                       +
      ")"

  lazy val table04Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_04" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"blob\" BLOB,"                                                                                                  +
      "\"clob\" CLOB"                                                                                                   +
      ")"

  lazy val table05Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                +
      "."                                                                                                               +
      sourceIdentifierQuote + "cars_deapi_05" + sourceIdentifierQuote + " "                                             +
      "(	"                                                                                                             +
      "\"carid\" VARCHAR2(20),"                                                                                         +
      "\"double_1\" DOUBLE PRECISION"                                                                                   +
      ")"

  lazy val createSourceTablesStatements      =
    Map  (
      "table_01" -> table01Statement,
      "table_02" -> table02Statement,
      "table_03" -> table03Statement,
      "table_04" -> table04Statement,
      "table_05" -> table05Statement
    )


  lazy val sourceDataInsertsList: List[String]           =
    List(
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))"""
    )

  def dropSourceSchema(): String   = {

    lazy val dropSourceSchemaStatement     = "DROP USER  "  + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote + " CASCADE"

    dropSourceSchemaStatement
  }


  def dropTargetSchema()    = {

    //Not sure why SQL server is not taking the double quotes in these drop statements
    //So it is being overridden with blank here
    val targetIdentifierQuote = ""

    //"select 'drop table ' + 'qe10c01' + '.' + name from sys.tables where SCHEMA_NAME(schema_id) =" + targetIdentifierQuote + targetDBSchemaName + targetIdentifierQuote

    lazy val dropTargetSchemaStatement     = "DROP SCHEMA  "  + targetIdentifierQuote + targetDBSchemaName + targetIdentifierQuote

    lazy val dropTableStatement01 =    "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "cars_deapi_char_1" +  targetIdentifierQuote

    lazy val dropTableStatement02 =    "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "cars_deapi_char_1000" +  targetIdentifierQuote

    lazy val dropTableStatement03 =    "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "cars_deapi_char_2000" +  targetIdentifierQuote

    lazy val dropTableStatement04 =   "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "cars_deapi_number_1_0" +  targetIdentifierQuote

    lazy val dropTableStatement05 =   "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "cars_deapi_number_4_2" +  targetIdentifierQuote

    lazy val dropTableStatement06 =   "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "cars_deapi_number_12_5" +  targetIdentifierQuote

    lazy val dropTableStatement07 =   "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "cars_deapi_float_1" +  targetIdentifierQuote

    lazy val dropTableStatement08 =   "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "cars_deapi_float_100" +  targetIdentifierQuote

    lazy val dropTableStatement09 =   "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "deapi_date" +  targetIdentifierQuote

    lazy val dropTableStatement10 =   "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "cars_deapi_varchar2_1" +  targetIdentifierQuote

    lazy val dropTableStatement11 =   "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "cars_deapi_varchar2_1000" +  targetIdentifierQuote

    lazy val dropTableStatement12 =   "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "cars_deapi_varchar2_4000" +  targetIdentifierQuote

    lazy val dropTableStatement13 =   "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "cars_deapi_float_126" +  targetIdentifierQuote

    lazy val dropTableStatement14 =   "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "cars_deapi_double_1" +  targetIdentifierQuote

    lazy val dropTableStatement15 =   "drop table " + targetIdentifierQuote + targetDBSchemaName +  targetIdentifierQuote + "." +
                                        targetIdentifierQuote + "cars_deapi_timestamp" +  targetIdentifierQuote


    lazy  val dropTargetSchemaStatements   =

      Seq ( dropTableStatement01,     dropTableStatement02,
            dropTableStatement03,     dropTableStatement04,
            dropTableStatement05,     dropTableStatement06,
            dropTableStatement07,     dropTableStatement08,
            /*dropTableStatement09,*/ dropTableStatement10,
            dropTableStatement11,     dropTableStatement12,
            dropTableStatement13,     dropTableStatement14,
            dropTableStatement15,     dropTargetSchemaStatement
      )



    dropTargetSchemaStatements
  }

}


class MySqlPostgresDataSetup {

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

  lazy val table01Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                           +
      "."                                                                                                          +
      sourceIdentifierQuote + "cars_deapi_10" + sourceIdentifierQuote + " "                                        +
      "("                                                                                                          +
      sourceIdentifierQuote + "carid" + sourceIdentifierQuote + " VARCHAR(20),"                                     +
      sourceIdentifierQuote + "decimal_1_0" +  sourceIdentifierQuote + " DECIMAL(1,0),"                             +
      sourceIdentifierQuote + "decimal_4_2" +  sourceIdentifierQuote + " DECIMAL(4,2),"                             +
      sourceIdentifierQuote + "decimal_12_5" +  sourceIdentifierQuote + " DECIMAL(12,5),"                           +
      sourceIdentifierQuote + "char_1" +  sourceIdentifierQuote + " CHAR(1),"                                       +
      sourceIdentifierQuote + "char_150" +  sourceIdentifierQuote + " CHAR(150),"                                   +
      sourceIdentifierQuote + "char_255" +  sourceIdentifierQuote + " CHAR(255),"                                   +
      sourceIdentifierQuote + "timestamp" +  sourceIdentifierQuote + " TIMESTAMP (6),"                              +
      sourceIdentifierQuote + "date" +  sourceIdentifierQuote + " DATE,"                                            +
      sourceIdentifierQuote + "datetime" +  sourceIdentifierQuote + " DATETIME,"                                    +
      sourceIdentifierQuote + "varchar_1" +  sourceIdentifierQuote + " VARCHAR(1),"                                 +
      sourceIdentifierQuote + "varchar_1000" +  sourceIdentifierQuote + " VARCHAR(1000),"                           +
      sourceIdentifierQuote + "varchar_4000" +  sourceIdentifierQuote + " VARCHAR(4000),"                           +
      sourceIdentifierQuote + "float_1" +  sourceIdentifierQuote + " FLOAT(1,0),"                                   +
      sourceIdentifierQuote + "float_25" +  sourceIdentifierQuote + " FLOAT(25,0),"                                 +
      sourceIdentifierQuote + "float_53" +  sourceIdentifierQuote + " FLOAT(53,0),"                                 +
      sourceIdentifierQuote + "float_100" +  sourceIdentifierQuote + " FLOAT(100,0),"                               +
      sourceIdentifierQuote + "float_126" +  sourceIdentifierQuote + " FLOAT(126,0),"                               +
      sourceIdentifierQuote + "integer" +  sourceIdentifierQuote + " INTEGER"                                       +
      ")"

  lazy val table02Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                             +
      "."                                                                                                            +
      sourceIdentifierQuote + "cars_deapi_02" + sourceIdentifierQuote + " "                                          +
      "(	"                                                                                                          +
      sourceIdentifierQuote + "carid" + sourceIdentifierQuote + " VARCHAR(20),"                                      +
      sourceIdentifierQuote + "long_text" + sourceIdentifierQuote + " LONGTEXT"                                      +
      ")"

  lazy val table03Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                             +
      "."                                                                                                            +
      sourceIdentifierQuote + "cars_deapi_03" + sourceIdentifierQuote + " "                                          +
      "(	"                                                                                                          +
      sourceIdentifierQuote + "carid" + sourceIdentifierQuote + " VARCHAR(20),"                                      +
      sourceIdentifierQuote + "long_blob" + sourceIdentifierQuote + " LONGBLOB"                                      +
      ")"

  lazy val table04Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                             +
      "."                                                                                                            +
      sourceIdentifierQuote + "cars_deapi_04" + sourceIdentifierQuote + " "                                          +
      "(	"                                                                                                          +
      sourceIdentifierQuote + "carid" + sourceIdentifierQuote + " VARCHAR(20),"                                     +
      sourceIdentifierQuote + "binary" + sourceIdentifierQuote + " BINARY,"                                          +
      sourceIdentifierQuote + "bit" + sourceIdentifierQuote + " BIT"                                                 +
      ")"

  lazy val createSourceTablesStatements      =
    Map  (
      "table_01" -> table01Statement,
      "table_02" -> table02Statement,
      "table_03" -> table03Statement,
      "table_04" -> table04Statement
    )


  lazy val sourceDataInsertsList: List[String]           =
    List(
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))"""
    )

  def dropSourceSchema(sourceDBSchemaName:String): String   = {

    lazy val dropSourceSchemaStatement     = "DROP SCHEMA IF EXISTS "  + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote

    dropSourceSchemaStatement
  }


  def dropTargetSchema(targetDBSchemaName:String): String   = {

    lazy val dropTargetSchemaStatement     = "DROP SCHEMA IF EXISTS "  + targetIdentifierQuote + targetDBSchemaName + targetIdentifierQuote + " CASCADE"

    dropTargetSchemaStatement
  }

}


class MySqlOracleDataSetup {

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

  def createTargetSchema()    = {

    //lazy  val  createSourceSchemaStatement     =   "CREATE USER qe10 IDENTIFIED BY qe10 DEFAULT TABLESPACE APEX_00000001"

    lazy  val  createSourceSchemaStatement      =   "CREATE USER " + sourceDbUserName  + " IDENTIFIED BY " + sourceDbUserName

    lazy  val  setQuotaStatement                =   "ALTER USER " + sourceDbUserName  + " QUOTA 100M ON SYSTEM"

    lazy  val  grantCreateSessionStatement      =   "grant create session to " + sourceDbUserName

    lazy  val  grantCreateTableStatement        =   "grant create table to " + sourceDbUserName

    lazy  val  grantAllPrivilegesStatement      =   "grant all on " + sourceDBSchemaName  + " to " + sourceDbUserName


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

  lazy val table01Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                           +
      "."                                                                                                          +
      sourceIdentifierQuote + "cars_deapi_10" + sourceIdentifierQuote + " "                                        +
      "("                                                                                                          +
      sourceIdentifierQuote + "carid" + sourceIdentifierQuote + " VARCHAR(20),"                                     +
      sourceIdentifierQuote + "decimal_1_0" +  sourceIdentifierQuote + " DECIMAL(1,0),"                             +
      sourceIdentifierQuote + "decimal_4_2" +  sourceIdentifierQuote + " DECIMAL(4,2),"                             +
      sourceIdentifierQuote + "decimal_12_5" +  sourceIdentifierQuote + " DECIMAL(12,5),"                           +
      sourceIdentifierQuote + "char_1" +  sourceIdentifierQuote + " CHAR(1),"                                       +
      sourceIdentifierQuote + "char_150" +  sourceIdentifierQuote + " CHAR(150),"                                   +
      sourceIdentifierQuote + "char_255" +  sourceIdentifierQuote + " CHAR(255),"                                   +
      sourceIdentifierQuote + "timestamp" +  sourceIdentifierQuote + " TIMESTAMP (6),"                              +
      sourceIdentifierQuote + "date" +  sourceIdentifierQuote + " DATE,"                                            +
      sourceIdentifierQuote + "datetime" +  sourceIdentifierQuote + " DATETIME,"                                    +
      sourceIdentifierQuote + "varchar_1" +  sourceIdentifierQuote + " VARCHAR(1),"                                 +
      sourceIdentifierQuote + "varchar_1000" +  sourceIdentifierQuote + " VARCHAR(1000),"                           +
      sourceIdentifierQuote + "varchar_4000" +  sourceIdentifierQuote + " VARCHAR(4000),"                           +
      sourceIdentifierQuote + "float_1" +  sourceIdentifierQuote + " FLOAT(1,0),"                                   +
      sourceIdentifierQuote + "float_25" +  sourceIdentifierQuote + " FLOAT(25,0),"                                 +
      sourceIdentifierQuote + "float_53" +  sourceIdentifierQuote + " FLOAT(53,0),"                                 +
      sourceIdentifierQuote + "float_100" +  sourceIdentifierQuote + " FLOAT(100,0),"                               +
      sourceIdentifierQuote + "float_126" +  sourceIdentifierQuote + " FLOAT(126,0),"                               +
      sourceIdentifierQuote + "integer" +  sourceIdentifierQuote + " INTEGER"                                       +
      ")"

  lazy val table02Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                             +
      "."                                                                                                            +
      sourceIdentifierQuote + "cars_deapi_02" + sourceIdentifierQuote + " "                                          +
      "(	"                                                                                                          +
      sourceIdentifierQuote + "carid" + sourceIdentifierQuote + " VARCHAR(20),"                                      +
      sourceIdentifierQuote + "long_text" + sourceIdentifierQuote + " LONGTEXT"                                      +
      ")"

  lazy val table03Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                             +
      "."                                                                                                            +
      sourceIdentifierQuote + "cars_deapi_03" + sourceIdentifierQuote + " "                                          +
      "(	"                                                                                                          +
      sourceIdentifierQuote + "carid" + sourceIdentifierQuote + " VARCHAR(20),"                                      +
      sourceIdentifierQuote + "long_blob" + sourceIdentifierQuote + " LONGBLOB"                                      +
      ")"

  lazy val table04Statement =
    "CREATE TABLE " + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                             +
      "."                                                                                                            +
      sourceIdentifierQuote + "cars_deapi_04" + sourceIdentifierQuote + " "                                          +
      "(	"                                                                                                          +
      sourceIdentifierQuote + "carid" + sourceIdentifierQuote + " VARCHAR(20),"                                     +
      sourceIdentifierQuote + "binary" + sourceIdentifierQuote + " BINARY,"                                          +
      sourceIdentifierQuote + "bit" + sourceIdentifierQuote + " BIT"                                                 +
      ")"

  lazy val createSourceTablesStatements      =
    Map  (
      "table_01" -> table01Statement,
      "table_02" -> table02Statement,
      "table_03" -> table03Statement,
      "table_04" -> table04Statement
    )


  lazy val sourceDataInsertsList: List[String]           =
    List(
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))""",
      "INSERT INTO "                                                                                                                            +
        sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote                                                                      +
        "."                                                                                                                                     +
        sourceIdentifierQuote + "cars_deapi_01" + sourceIdentifierQuote + " "                                                                   +
        """("carid","number_1_0","number_4_2","number_12_5","char_1","char_1000","char_2000","timestamp","timestamptimezone",
        "timestamplocaltimezone","varchar2_1","varchar2_1000","varchar2_4000","binaryfloat","binarydouble","float_1",
        "float_25","float_53","float_100","float_126","date")
        VALUES
        ('K0000500', -9, -19.21, -1234567.90123, 'a', 'aaaaaaa', 'ff',
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        to_timestamp_tz('07-FEB-12 01.28.26.000000000 PM -05:00','DD-MON-RR HH.MI.SS.FF AM TZR'),
        to_timestamp('07-FEB-12 01.28.26.000000000 PM','DD-MON-RR HH.MI.SS.FF AM'),
        'p','Z0000504_varchar2_data','Z0000504_varchar2_dataZ0000504',null,null,10,90000000,90000000,90000000,90000000,
        to_date('2007-02-12','YYYY-MM-DD'))"""
    )

  def dropSourceSchema(sourceDBSchemaName:String): String   = {

    lazy val dropSourceSchemaStatement     = "DROP SCHEMA IF EXISTS "  + sourceIdentifierQuote + sourceDBSchemaName + sourceIdentifierQuote

    dropSourceSchemaStatement
  }


  def dropTargetSchema(targetDBSchemaName:String): String   = {

    lazy val dropTargetSchemaStatement     = "DROP USER  "  + targetIdentifierQuote + targetDBSchemaName + targetIdentifierQuote + " CASCADE"

    dropTargetSchemaStatement
  }

}






