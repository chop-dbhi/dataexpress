package edu.chop.cbmi.dataExpress.test.backends.file

import org.scalatest.{BeforeAndAfter, GivenWhenThen, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import edu.chop.cbmi.dataExpress.dataModels.{DataRow}
import java.io.File
import edu.chop.cbmi.dataExpress.dataModels.RichOption._
import edu.chop.cbmi.dataExpress.backends.file._
import edu.chop.cbmi.dataExpress.backends.file.HeaderRowColumnNames
import edu.chop.cbmi.dataExpress.backends.file.TextFileBackend
import edu.chop.cbmi.dataExpress.dataModels.SeqColumnNames
import edu.chop.cbmi.dataExpress.dataModels.sql._
import edu.chop.cbmi.dataExpress.backends.file.DelimiterMarshaller
import edu.chop.cbmi.dataExpress.dataModels.sql.FloatDataType
import scala.Some
import edu.chop.cbmi.dataExpress.backends.file.MagicMarshaller
import edu.chop.cbmi.dataExpress.backends.file.HeaderRowColumnNames
import edu.chop.cbmi.dataExpress.backends.file.TextFileBackend
import edu.chop.cbmi.dataExpress.dataModels.SeqColumnNames
import java.util.Calendar
import java.sql.Date

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/17/13
 * Time: 8:57 AM
 */
class MagicMarshallerSpecs extends FunSpec with GivenWhenThen with ShouldMatchers with BeforeAndAfter{

  lazy val file = new File("./output/MagicMarshallerSpec.dat")

  before{
    if(file.exists())file.delete()
  }

  after{
    if(file.exists())file.delete()
  }

  def buildFile(content:List[DataRow[_]], colNames: Seq[String], withHeader: Boolean = true) = {
    f.backend.delete()
    f.backend.makeNewFile()
    if(withHeader)f.backend.writeHeader(DataRow(colNames.zip(colNames): _*))
    f.backend.write(content.iterator, Append)
  }

  class TestMagicOptions1 extends MagicOptions{
    override val floatTypePrecisionPadding = 0
    override val treat01AsBoolean = false
  }

  class TestMagicOptions2 extends MagicOptions{
    override val floatTypePrecisionPadding = 0
    override val maxDepth = f.content2.length
  }

  object TestMagicOptions1{
    val expectedFloatPrecision = 2
  }

  def fixture() = {
    new {
      val id = "id"
      val name = "name"
      val dbl = "dbl"
      val bool1 = "bool1"
      val bool2 = "bool2"
      val d1 = "d1"
      val d2 = "d2"
      val colNames = Seq(id, name, dbl, bool1, bool2, d1, d2)

      val content1 = List(DataRow(id->"1",    name->"Bob",    dbl->"1.12",    bool1->"true",  bool2->"false", d1->"2005/10/13", d2->"10-15-1900"))

      val content2 = List(DataRow(id->"-1",   name->"Bob",    dbl->"+.12",    bool1->"true",  bool2->"0", d1->"2005/10/12", d2->"10-11-1900"),
                          DataRow(id->"-20",  name->"Jim",    dbl->"-.4",     bool1->"false", bool2->"1", d1->"1805/02/20", d2->"01-08-1900"),
                          DataRow(id->"1",    name->"Billie", dbl->"+1.",     bool1->"True",  bool2->"0", d1->"1997/01/12", d2->"10-15-1900"),
                          DataRow(id->"0",    name->"Kim",    dbl->"-1.",     bool1->"False", bool2->"1", d1->"2000/02/05", d2->"10-15-1900"),
                          DataRow(id->"34",   name->"Billie", dbl->"+1.3",    bool1->"T",     bool2->"0", d1->"1974/04/02", d2->"10-15-1900"),
                          DataRow(id->"-34",  name->"Billie", dbl->"-34.3",   bool1->"F",     bool2->"",  d1->"1776/07/04", d2->"10-15-1900"),
                          DataRow(id->"34",   name->"Joey",   dbl->"156.45",  bool1->"0",     bool2->"",  d1->"1941/12/07", d2->"10-15-1900"),
                          DataRow(id->"34",   name->"Billie", dbl->".4",      bool1->"1",     bool2->"",  d1->"1969/06/02", d2->"10-15-1900"),
                          DataRow(id->"34",   name->"Billie", dbl->"4",       bool1->"true",  bool2->"",  d1->"2005/10/12", d2->"10-15-1900"),
                          DataRow(id->"34",   name->"Billie", dbl->"-4",      bool1->"false", bool2->"",  d1->"2005/10/12", d2->"10-15-1900"),
                          DataRow(id->"",     name->"1",      dbl->"",        bool1->"",      bool2->"",  d1->"",           d2->""),
                          DataRow(id->"",     name->"F",      dbl->"",        bool1->"",      bool2->"",  d1->"",           d2->""),
                          DataRow(id->"",     name->"1.25",      dbl->"",        bool1->"",      bool2->"",  d1->"",           d2->""),
                          DataRow(id->"",     name->"2005/05/20",      dbl->"",        bool1->"",      bool2->"",  d1->"",           d2->""),
                          DataRow(id->"586",  name->"",       dbl->"",        bool1->"",      bool2->"",  d1->"",           d2->""),
                          DataRow(id->"",     name->"ZoE",    dbl->"",        bool1->"",      bool2->"",  d1->"",           d2->""),
                          DataRow(id->"",     name->"",       dbl->"1.",      bool1->"",      bool2->"",  d1->"2005/10/12", d2->""))

      val cng = HeaderRowColumnNames(file,",")
      val dm = DelimiterMarshaller(",", SeqColumnNames(colNames))
      val backend = TextFileBackend(file, dm, 1)

      val expectedTypes = Seq(IntegerDataType,
                              CharacterDataType(100, false),
                              FloatDataType(TestMagicOptions1.expectedFloatPrecision),
                              BooleanDataType,
                              BooleanDataType,
                              DateDataType,
                              DateDataType)
    }
  }

  def rowToLine(dr:DataRow[_]) : String = {
    val id = dr.id.getOrElse("")
    val name = dr.name.getOrElse("")
    val dbl = dr.dbl.getOrElse("").toString.replace("+","")
    val b = dr.bool1.getOrElse("").toString
    val b2 = dr.bool2.getOrElse("").toString
    val d1 = dr.d1.getOrElse("").toString
    val d2 = dr.d2.getOrElse("").toString
    s"$id,$name,$dbl,$b,$b2,$d1,$d2"
  }

  def compareRows(stringRow: DataRow[_], valueRow: DataRow[_]) = {
    val sid = {
      val t = stringRow.id.getOrElse("").toString
      if(t.length==0)Int.MaxValue
      else t.toInt
    }
    val vid = valueRow.id.as[Int].getOrElse(Int.MaxValue)
    sid should equal(vid)

    val sn = stringRow.name.getOrElse("")
    val vn = valueRow.name.as[String].getOrElse("")
    sn should equal(vn)

    val sdbl = {
      val t = stringRow.dbl.getOrElse("").toString.replace("+","")
      if(t.length==0)Double.MaxValue
      else t.toDouble
    }
    val vdbl = valueRow.dbl.as[Double].getOrElse(Double.MaxValue)
    sdbl should equal(vdbl)

    val sbool = stringRow.bool1 match{
      case Some(s) => {
        val ts = s.toString.trim.toLowerCase()
        if(ts=="0" || ts == "f" || ts=="false" || ts.length==0) false
        else true
      }
      case _ => false
    }
    val vbool = valueRow.bool1.as[Boolean].getOrElse(false)
    sbool should equal(vbool)

    val cal = Calendar.getInstance()
    val sdate = stringRow.d1 match {
      case Some(s) => {
        val ts = s.toString.trim.toLowerCase()
        if(ts.length>0){
          val Array(y,m,d) = ts.split("/")
          cal.set(y.toInt, m.toInt, d.toInt)
          cal.getTime
        }else{
          cal.set(1900,1,1)
          cal.getTime
        }
      }
      case _ => {
        cal.set(1900,1,1)
        cal.getTime
      }
    }
    cal.set(1900,1,1)
    val vdate = valueRow.d1.as[Date].getOrElse(cal.getTime)
    sdate.toString should equal(vdate.toString)

    val sdate2 = stringRow.d2 match {
      case Some(s) => {
        val ts = s.toString.trim.toLowerCase()
        if(ts.length>0){
          val Array(m,d,y) = ts.split("-")
          cal.set(y.toInt, m.toInt, d.toInt)
          cal.getTime
        }else{
          cal.set(1900,1,1)
          cal.getTime
        }
      }
      case _ => {
        cal.set(1900,1,1)
        cal.getTime
      }
    }
    cal.set(1900,1,1)
    val vdate2 = valueRow.d2.as[Date].getOrElse(cal.getTime)
    sdate2.toString should equal(vdate2.toString)

  }

  def testMarshalling(stringRow: DataRow[_], m: MagicMarshaller) = {
    val line = rowToLine(stringRow)
    val valueRow = m.unmarshall(line)
    compareRows(stringRow, valueRow)
  }

  val f = fixture()

  describe("The Magic Marshaller"){
     it("should infer the proper data types from file content and do proper marshalling"){
       //******************************************
       //READ THIS BEFORE MAKING CHANGES
       //NEW MARSHALLERS ARE REQUIRED AT EACH GIVEN BECAUSE
       //MAGIC MARSHALLER ONLY EVALUATES types ONCE
       //WHICH MAKES SENSE FOR NORMAL USAGE WHERE YOU DON'T EXPECT TO KEEP CHANGING THE FILE CONTENT
       //******************************************

       Given("a file with a single row, no header")
       buildFile(f.content1, f.colNames, false)
       val m1 = MagicMarshaller(file, Some(SeqColumnNames(f.colNames)), new TestMagicOptions1())
       m1.dataTypes() should equal(f.expectedTypes)
       f.content1.foreach{row => testMarshalling(row, m1)}

       Given("a file with a single row and a header")
       buildFile(f.content1, f.colNames, true)
       val m2 = MagicMarshaller(file, Some(f.cng), new TestMagicOptions1())
       m2.dataTypes() should equal(f.expectedTypes)
       f.content1.foreach{row => testMarshalling(row, m2)}

       Given("a file with multiple rows whose columns all have the same format")
       buildFile(f.content2.take(3), f.colNames, true)
       val m3 = MagicMarshaller(file, Some(f.cng), new TestMagicOptions2())
       m3.dataTypes() should equal(f.expectedTypes)
       f.content2.take(3).foreach{row => testMarshalling(row, m3)}

       Given("a file with multiple rows whose columns all have the same format but with missing values")
       buildFile(f.content2, f.colNames, true)
       val m4 = MagicMarshaller(file, Some(f.cng), new TestMagicOptions2())
       m4.dataTypes() should equal(f.expectedTypes)
       f.content2.foreach{row => testMarshalling(row, m4)}
     }
  }

}
