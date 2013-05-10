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
import edu.chop.cbmi.dataExpress.backends.file.HeadearRowColumnNames
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
      val colNames = Seq("Name","ID","Gender")
      val content = List("Bob,249,M","Jane Doe,3430,F","Mike R.,,M","Steve,83839,")
      val cng = HeadearRowColumnNames(file,",")
      val marshaller = DelimiterMarshaller(",",cng)
      val backend = TextFileBackend(file, cng, marshaller, 1)
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
         row.Name match{
           case Some(s) => lineVals(0) should equal(s)
           case _ => lineVals(0).trim should equal("")
         }
         row.ID.as[String] match{
           case Some(i) => lineVals(1).toInt should equal(i.toInt)
           case _ => lineVals(1).trim should equal("")
         }
         idx + 1
       }

     }


   }
}
