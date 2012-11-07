package edu.chop.cbmi.dataExpress.test.tutorial

import org.scalatest.junit.JUnitRunner
import org.scalatest.FeatureSpec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.GivenWhenThen
import java.util.{UUID, Properties}
import java.io.File
import edu.chop.cbmi.dataExpress.backends._
import edu.chop.cbmi.dataExpress.dataModels._
import edu.chop.cbmi.dataExpress.dataModels.sql._
import edu.chop.cbmi.dataExpress.dsl._
import edu.chop.cbmi.dataExpress.dsl.stores.SqlDb

/* This test is meant to allow easy updating for ensuring the quickstart tutorial
 * on the DataExpress website can be tested easily with any code changes
 */


class TutorialFeatureSpec extends FeatureSpec with GivenWhenThen with ShouldMatchers {
  
  
  
  def withBlankDatabase(testCode: SqlDb => Any) {
    val dbName = "jdbc:sqlite:/:memory:"
    val dbProps = new Properties()
    dbProps.setProperty("jdbcUri", dbName)
    val backend = SqlBackendFactory(dbProps)
    val db = SqlDb(backend)
    try {
      testCode(db)
    }
    finally db.close
    
  }
  
  def withDatabase(testCode: SqlDb => Any) {
    val dbName = "jdbc:sqlite:/:memory:" //UUID.randomUUID.toString
    val dbProps = new Properties()
    dbProps.setProperty("jdbcUri",dbName)
    val backend = SqlBackendFactory(dbProps)
    
    try {
      dbSetup(backend)
      val db = SqlDb(backend)
      testCode(db) // "loan" the fixture to the test
    }
    finally removeDb(backend) // clean up the fixture
  }
  
  def dbSetup(db: SqlBackend) {
    db.connect()
    val columnNames = List("id", "name", "party", "dob", "dod", "term_start", "term_end")
    val tableName = "presidents"
    db.createTable(tableName = tableName, 
                   columnNames = columnNames,
                   dataTypes = List(IntegerDataType(),
                                   CharacterDataType(100,false),
                                  CharacterDataType(100,false),
                                  DateDataType(),
                                  DateDataType(),
                                  DateDataType(),
                                  DateDataType()))
                                  
   val df = new java.text.SimpleDateFormat("yyyy-MM-dd")

    val allData = List(
      List(16, "Abraham Lincoln", "Republican Party", df.parse("1809-02-12"), df.parse("1865-04-15"), df.parse("1861-03-04"), df.parse("1865-04-15")),
      List(7, "Andrew Jackson", "Democratic-Republican Party", df.parse("1767-03-15"), df.parse("1845-06-08"), df.parse("1829-03-04"), df.parse("1837-03-04")),
      List(17, "Andrew Johnson", "Democratic Party", df.parse("1808-12-29"), df.parse("1875-07-31"), df.parse("1865-04-15"), df.parse("1869-03-04")),
      List(42, "Bill Clinton", "Democratic Party", df.parse("1946-08-19"), null, df.parse("1993-01-20"), df.parse("2001-01-20")),
      List(21, "Chester A. Arthur", "Republican Party", df.parse("1829-10-05"), df.parse("1886-11-18"), df.parse("1881-09-19"), df.parse("1885-03-04")),
      List(30, "Calvin Coolidge", "Republican Party", df.parse("1872-07-04"), df.parse("1933-01-05"), df.parse("1923-08-02"), df.parse("1929-03-04")),
      List(34, "Dwight D. Eisenhower", "Republican Party", df.parse("1890-10-14"), df.parse("1969-03-28"), df.parse("1953-01-20"), df.parse("1961-01-20")),
      List(32, "Franklin D. Roosevelt", "Democratic Party", df.parse("1882-01-30"), df.parse("1945-04-12"), df.parse("1933-03-04"), df.parse("1945-04-12")),
      List(14, "Franklin Pierce", "Democratic Party", df.parse("1804-11-23"), df.parse("1869-10-08"), df.parse("1853-03-04"), df.parse("1857-03-04")),
      List(41, "George H. W. Bush", "Republican Party", df.parse("1924-06-12"), null, df.parse("1989-01-20"), df.parse("1993-01-20")),
      List(1, "George Washington", "Independent", df.parse("1732-02-22"), df.parse("1799-12-14"), df.parse("1789-04-30"), df.parse("1797-03-04")),
      List(22, "Grover Cleveland", "Democratic Party", df.parse("1837-03-18"), df.parse("1908-06-24"), df.parse("1885-03-04"), df.parse("1889-03-04")),
      List(24, "Grover Cleveland", "Democratic Party", df.parse("1837-03-18"), df.parse("1908-06-24"), df.parse("1893-03-04"), df.parse("1897-03-04")),
      List(31, "Herbert Hoover", "Republican Party", df.parse("1874-08-10"), df.parse("1964-10-20"), df.parse("1929-03-04"), df.parse("1933-03-04")),
      List(2, "John Adams", "Federalist Party", df.parse("1735-10-30"), df.parse("1826-07-04"), df.parse("1797-03-04"), df.parse("1801-03-04")),
      List(6, "John Quincy Adams", "Federalist Party", df.parse("1767-07-11"), df.parse("1848-02-23"), df.parse("1825-03-04"), df.parse("1829-03-04")),
      List(4, "James Madison", "Democratic-Republican Party", df.parse("1751-03-16"), df.parse("1836-06-28"), df.parse("1809-03-04"), df.parse("1817-03-04")),
      List(5, "James Monroe", "Democratic-Republican Party", df.parse("1758-04-28"), df.parse("1831-07-04"), df.parse("1817-03-04"), df.parse("1825-03-04")),
      List(10, "John Tyler", "Whig Party", df.parse("1790-03-29"), df.parse("1862-01-18"), df.parse("1841-04-04"), df.parse("1845-03-04")),
      List(11, "James K. Polk", "Democratic Party", df.parse("1795-11-02"), df.parse("1849-06-15"), df.parse("1845-03-04"), df.parse("1849-03-04")),
      List(15, "James Buchanan", "Democratic Party", df.parse("1791-04-23"), df.parse("1868-06-01"), df.parse("1857-03-04"), df.parse("1861-03-04")),
      List(39, "Jimmy Carter", "Democratic Party", df.parse("1924-10-01"), null, df.parse("1977-01-20"), df.parse("1981-01-20")),
      List(8, "Martin Van Buren", "Democratic-Republican Party", df.parse("1782-12-05"), df.parse("1862-07-24"), df.parse("1837-03-04"), df.parse("1841-03-04")),
      List(13, "Millard Fillmore", "Whig Party", df.parse("1800-01-07"), df.parse("1874-03-08"), df.parse("1850-07-09"), df.parse("1853-03-04")),
      List(40, "Ronald Reagan", "Republican Party", df.parse("1911-02-06"), df.parse("2004-06-05"), df.parse("1981-01-20"), df.parse("1989-01-20")),
      List(37, "Richard Nixon", "Republican Party", df.parse("1913-01-09"), df.parse("1994-04-22"), df.parse("1969-01-20"), df.parse("1974-08-09")),
      List(19, "Rutherford B. Hayes", "Republican Party", df.parse("1822-10-04"), df.parse("1893-01-17"), df.parse("1877-03-04"), df.parse("1881-03-04")),
      List(3, "Thomas Jefferson", "Democratic-Republican Party", df.parse("1743-04-13"), df.parse("1826-07-04"), df.parse("1801-03-04"), df.parse("1809-03-04")),
      List(26, "Theodore Roosevelt", "Republican Party", df.parse("1858-10-27"), df.parse("1919-01-06"), df.parse("1901-09-14"), df.parse("1909-03-04")),
      List(18, "Ulysses S. Grant", "Republican Party", df.parse("1822-04-27"), df.parse("1885-07-23"), df.parse("1869-03-04"), df.parse("1877-03-04")),
      List(29, "Warren G. Harding", "Republican Party", df.parse("1865-11-02"), df.parse("1923-08-02"), df.parse("1921-03-04"), df.parse("1923-08-02")),
      List(9, "William Henry Harrison", "Whig Party", df.parse("1773-02-09"), df.parse("1841-04-04"), df.parse("1841-03-04"), df.parse("1841-04-04")),
      List(25, "William McKinley", "Republican Party", df.parse("1843-01-29"), df.parse("1901-09-14"), df.parse("1897-03-04"), df.parse("1901-09-14")),
      List(27, "William Howard Taft", "Republican Party", df.parse("1857-09-15"), df.parse("1930-03-08"), df.parse("1909-03-04"), df.parse("1913-03-04")),
      List(28, "Woodrow Wilson", "Democratic Party", df.parse("1856-12-28"), df.parse("1924-02-03"), df.parse("1913-03-04"), df.parse("1921-03-04")),
      List(12, "Zachary Taylor", "Whig Party", df.parse("1784-11-24"), df.parse("1850-07-09"), df.parse("1849-03-04"), df.parse("1850-07-09")),
      List(20, "James Garfield", "Republican Party", df.parse("1831-11-19"), df.parse("1881-09-19"), df.parse("1881-03-05"), df.parse("1881-09-19")),
      List(36, "Lyndon B. Johnson", "Democratic Party", df.parse("1908-08-27"), df.parse("1973-01-22"), df.parse("1963-11-22"), df.parse("1969-01-20")),
      List(43, "George W. Bush", "Republican Party", df.parse("1946-07-06"), null, df.parse("2001-01-20"), df.parse("2009-01-20")),
      List(33, "Harry S. Truman", "Democratic Party", df.parse("1884-05-08"), df.parse("1972-12-26"), df.parse("1945-04-12"), df.parse("1953-01-20")),
      List(38, "Gerald Ford", "Republican Party", df.parse("1913-07-14"), df.parse("2006-12-26"), df.parse("1974-08-09"), df.parse("1977-01-20")),
      List(35, "John F. Kennedy", "Democratic Party", df.parse("1917-05-29"), df.parse("1963-11-22"), df.parse("1961-01-20"), df.parse("1963-11-22")),
      List(23, "Benjamin Harrison", "Republican Party", df.parse("1833-08-20"), df.parse("1901-03-13"), df.parse("1889-03-04"), df.parse("1893-03-04")),
      List(44, "Barack Obama", "Democratic Party", df.parse("1961-08-04"), null, df.parse("2009-01-20"), null))
      
      allData foreach {president => 
        val row = DataRow(columnNames)(president.map{Some(_)})
        db.insertRow(tableName, row)
      }
    
     if (db.sqlDialect == SqLiteDialect) {
    	 val sql = """UPDATE presidents set dob = date(dob/1000, 'unixepoch'),
                                       dod = date(dod/1000, 'unixepoch'),
                                term_start = date(term_start/1000, 'unixepoch'),
                                term_end   = date(term_end/1000, 'unixepoch')""" 
    	 db.execute(sql)
     }
     
     db.commit()
  }
  
  def removeDb(db:SqlBackend) = {
    try {
    	db.close()
    }
    //if using files, remove the file
    //finally (new File(dbName)).delete
  }

  
}