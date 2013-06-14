package edu.chop.cbmi.dataExpress.test.dataModels

import org.scalatest.{BeforeAndAfter, GivenWhenThen, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import edu.chop.cbmi.dataExpress.backends.file._
import edu.chop.cbmi.dataExpress.dataModels.{DataRow, DataTable}
import java.io.File
import edu.chop.cbmi.dataExpress.dataModels.file.FileTable
import edu.chop.cbmi.dataExpress.backends.file.TextFileBackend
import edu.chop.cbmi.dataExpress.dataModels.SeqColumnNames
import edu.chop.cbmi.dataExpress.backends.file.DelimiterMarshaller
import edu.chop.cbmi.dataExpress.backends.file.HeaderRowColumnNames
import edu.chop.cbmi.dataExpress.dataModels.RichOption._


/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/10/13
 * Time: 10:16 AM
 */
class FileTableSpec extends FunSpec with GivenWhenThen with ShouldMatchers with BeforeAndAfter{

  lazy val file = new File("./output/FileTableSpec.dat")

  before{
    if(file.exists())file.delete()
    //create a file to read
    f.backend.makeNewFile()
    f.backend.write(DataRow(f.colNames.zip(f.colNames): _*), Overwrite)
    f.backend.write(f.rows.iterator, Append)
  }

  after{
    if(file.exists())file.delete()
  }

  def fixture() = {
    new {
      val colNames = Seq("Name","ID","Known")
      val content = List("Bob,249,true","Jane Doe,3430,false","Mike R.,,false","Steve,83839,")
      val cng = HeaderRowColumnNames(file,",")
      val marshaller = DelimiterCustomMarshaller(",", cng, (a:Array[Option[String]])=>{
        val name = a(0) match{
          case Some(s) => s.toString
          case _ => null
        }
        val id = a(1) match{
          case Some(s) => s.toInt
          case _ => null
        }
        val known = a(2) match{
          case Some(s) => s.toBoolean
          case _ => null
        }
        Array(name, id, known)
      })
      val backend = TextFileBackend(file, marshaller, 1)
      val rows = {
        val cg = SeqColumnNames(colNames)
        val mars = DelimiterMarshaller(",",cg)
        content.map{line =>  mars.unmarshall(line)}
      }
    }
  }

  val f = fixture()

  describe("A File Table"){
     it("should result from a call to DataTable"){
       Given("A file backend and a columnanme generator")
       val ft:FileTable = DataTable(f.backend, f.cng)

       And("the table should have column names from the header")
         ft.column_names should equal(f.colNames)

       And("the table should be an iterator over DataRow that match the content")
       (0 /: ft){(idx, row) =>
         val lineVals = f.content(idx).split(",")
         row.Name.as[String] match{
           case Some(s) => lineVals(0).trim should equal(s)
           case _ => lineVals(0).trim should equal("")
         }
         row.ID.as[Int] match{
           case Some(i) => lineVals(1).trim.toInt should equal(i)
           case _ => lineVals(1).trim should equal("")
         }
         row.Known.as[Boolean] match{
           case Some(b) => lineVals(2).toBoolean should equal(b)
           case _ => lineVals.length should be <(3)
         }
         idx + 1
       }

       And("it should allow column access")
       When("accessed using the col it will return a FileColumn[Option[_]]")
       (0/: ft.col("ID")){(idx,id) =>
         val lineVals = f.content(idx).split(",")
         id.as[Int] match{
          case Some(ids) => lineVals(1).trim.toInt should equal(ids)
          case _ => lineVals(1).trim should equal("")
         }
         idx + 1
       }

       When("accessed using the col_as[G] return a FileColumn[Option[G]] IF THE MARSHALLER SUPPORTS IT")
       (0/: ft.col_as[Int]("ID")){(idx,id) =>
         val lineVals = f.content(idx).split(",")
         id match{
           case Some(v) => lineVals(1).trim.toInt should equal(v)
           case _ => lineVals(1).trim should equal("")
         }
         idx + 1
       }

       When("accessed using the col_asu[G] return a FileColumn[G] IF THE MARSHALLER SUPPORTS IT AND THE COLUMN HAS NO EMPTY ROWS")
       (0/: ft.col_asu[String]("Name")){(idx,name) =>
         val lineVals = f.content(idx).split(",")
         name should equal(lineVals(0))
         idx + 1
       }

     }


   }
}
