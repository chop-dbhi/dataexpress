package edu.chop.cbmi.dataExpress.test.dsl

import edu.chop.cbmi.dataExpress.test.util.presidents.{KNOWN_SQL_BACKEND, PresidentsSpecWithSourceTarget}
import java.io.File
import edu.chop.cbmi.dataExpress.backends.file._
import edu.chop.cbmi.dataExpress.test.util.Functions.sqlDateFrom
import edu.chop.cbmi.dataExpress.dataModels.{DataRow}
import edu.chop.cbmi.dataExpress.test.util.presidents.MYSQL
import scala.Some
import edu.chop.cbmi.dataExpress.dataModels.sql._
import edu.chop.cbmi.dataExpress.dsl.ETL
import edu.chop.cbmi.dataExpress.dsl.ETL._
import edu.chop.cbmi.dataExpress.dsl.stores.{SqlDb, FileStore}
import java.sql.Date
import edu.chop.cbmi.dataExpress.dataModels.RichOption._

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/14/13
 * Time: 11:20 AM
 */

object PresMarshaller {
  def apply() = new PresMarshaller(Seq("id","first_name", "last_name","num_terms", "dob"))
}

class PresMarshaller(columns: => Seq[String]) extends Marshaller(columns){
  val delim = ","
  def unmarshall(line:String) : DataRow[_] = {
    val split = line.split(delim)
    val items = split.zip(Range(0,split.length)).map{tpl =>
      val idx = tpl._2
      val ts = tpl._1.trim
      if(ts.length==0)null
      else idx match{
        case 0 => ts.toInt
        case 3 => ts.toInt
        case 4 => sqlDateFrom(ts)
        case _ => ts
      }
    }
    lazy val paddedItems = if(items.length==columnNames.length-1)(items.toList.:+(null)).toArray else items
    lazy val rowEntries = columnNames.zip(paddedItems)
    DataRow(rowEntries: _*)
  }

  def marshall(row: DataRow[_]) = {
    (""/:row){(s,o) =>
      o match{
        case Some(v) =>{
          val fv= v match{
            case d:java.util.Date => d.toString.replace("-","")
            case _ => v.toString
          }
          s"$s${fv}$delim"
        }
        case _ => s"$s$delim"
      }
    }.dropRight(1)
  }

  lazy private val colTypes = List(CharacterDataType(20,false), CharacterDataType(50,false), IntegerDataType, DateDataType)
  override def dataTypes = colTypes
}
class CopyFileSpec extends PresidentsSpecWithSourceTarget{
  //override val backend_test_type : KNOWN_SQL_BACKEND = POSTGRES()
  override val backend_test_type : KNOWN_SQL_BACKEND = MYSQL()

  lazy val fileOne = new File("./output/CopyFileSpec1.dat")
  lazy val fileTwo = new File("./output/CopyFileSpec2.dat")

  //will be creating some extra tables
  val VICE_PRESIDENTS = add_known_table("vice_presidents")
  val PRESIDENTS_COPY = add_known_table("pres_copy")

  override def before_func(){
    super.before_func()
    if(fileOne.exists())fileOne.delete()
    if(fileTwo.exists())fileTwo.delete()
    //create a file to read
  }

  override def after_func(){
    super.after_func()
    if(fileOne.exists())fileOne.delete()
    if(fileTwo.exists())fileTwo.delete()
  }

  def fixture() = {
    new {
      val columnNames = Seq("id","first_name", "last_name","num_terms", "dob")
      //val cngH = HeaderRowColumnNames(fileTwo)
      val fileBackendOne = TextFileBackend(fileOne, PresMarshaller())
      val fileBackendTwo = TextFileBackend(fileTwo, PresMarshaller(), 1)
    }
  }

  val f = fixture()

  describe("CopyFrom using a File Backend") {
    it("should enable copying from a source db or file to a file"){
      val source = "source"
      val fs1 = "fileStore1"
      val fs2 = "fileStore2"
      val fs3 = "fileStore3"

      ETL.execute(true,true){
        register store FileStore(f.fileBackendOne, false) as fs1
        register store FileStore(f.fileBackendTwo) as fs2
        register store SqlDb(prop_file_source, schema) as source

        When("using create from a db, a file is created")
        copy table PRESIDENTS from source to fs1 create

        fileOne.exists should equal(true)

        And("it's content is that of db table")
        (get from fs1).length should equal(default_president_count)

        When("copying from a file to a db")
        //test magic marhsaller and copying to db
        register store FileStore(TextFileBackend(fileOne, Some(f.columnNames))) as "fileStoreMagic"
        copy from "fileStoreMagic" to source create PRESIDENTS_COPY
        (get table PRESIDENTS_COPY from source).length should equal(default_president_count)

        When("using create from another file, a file is again created")
        copy from fs1 to fs2 create

        fileTwo.exists() should equal(true)

        And("and it's content is that of file")
        (get from fs2).length should equal(default_president_count)

        f.fileBackendTwo.delete()

        And("it should allow transforms when copying from a file")
        val sf = 100
        copy from fs1 transform{ row =>
          val new_id = row.id.asu[Int] * sf
          new_row(
            "id" -> new_id,
            "first_name" -> row.first_name.asu[String].toUpperCase,
            "last_name" -> row.last_name.asu[String].toUpperCase,
            "num_terms" -> row.num_terms.asu[Int],
            "dob" -> row.dob.as[Date].getOrElse(null)
          )
        } to fs2 create

        val idsum = (0/:default_president_ids){_+_}
        (0 /: (get from fs2)){(s,dr)=>
          s + dr.id.asu[Int]
        } should equal(sf*idsum)

        And("it should allow filtering of rows and appending")
        f.fileBackendTwo.delete()
        copy from fs1 filter{row => row.num_terms.asu[Int] == 2} to fs2 create

        (get from fs2).length should equal(default_ttp_count)

        copy from fs1 to fs2 append

        (get from fs2).length should equal(default_president_count + default_ttp_count)

        And("it should allow changing of column names")
        f.fileBackendTwo.delete()

        copy from fs1 change_column_names("id"->"IDNUM", "last_name"->"LN") to fs2 create

        def headerColumnNames = TextFileBackend.getHeaderRowColumnNames(fileTwo)
        register store FileStore(TextFileBackend(fileTwo, new PresMarshaller(headerColumnNames), 1)) as fs3

        (0 /: (get from fs3)){(s,dr)=>
          s + dr.IDNUM.asu[Int]
        } should equal(idsum)

        And("it should allow altering the table")
        f.fileBackendTwo.delete()

        copy from fs1 alter{table =>
          table set_data_types(IntegerDataType, CharacterDataType(20, false))
          table set_column_names("id", "name")
          table set_row_values{row =>
            val collapsed_name = row.last_name.asu[String] + ", " + row.first_name.asu[String]
            new_row(
              "id" -> row.id.asu[Int],
              "name" -> collapsed_name
            )
          }
        } to fs2 create

        val dt = get from fs3

        List("first_name", "last_name", "num_terms", "dob") foreach{cn=> dt.hasColumn(cn) should equal(false)}
        dt.hasColumn("id") should equal(true)
        dt.hasColumn("name") should equal(true)
        dt.length should equal(default_president_count)



        true
      } should equal(Left(true))

    }
  }
}
