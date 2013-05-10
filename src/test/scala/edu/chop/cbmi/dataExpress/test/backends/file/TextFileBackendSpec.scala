package edu.chop.cbmi.dataExpress.test.backends.file

import org.scalatest.{BeforeAndAfter, GivenWhenThen, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import edu.chop.cbmi.dataExpress.backends.file._
import java.io.File
import edu.chop.cbmi.dataExpress.dataModels.{SeqColumnNames, DataRow}
import edu.chop.cbmi.dataExpress.backends.file.DelimiterMarshaller
import edu.chop.cbmi.dataExpress.backends.file.HeadearRowColumnNames
import edu.chop.cbmi.dataExpress.backends.file.TextFileBackend

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/9/13
 * Time: 12:04 PM
 */
class TextFileBackendSpec extends FunSpec with GivenWhenThen with ShouldMatchers with BeforeAndAfter{

  lazy val file = new File("./output/TextFileBackendSpec.dat")

  before{
     if(file.exists())file.delete()
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
      val backend = TextFileBackend(file, cng, marshaller)
      val rows = {
        val cg = SeqColumnNames(colNames)
        val mars = DelimiterMarshaller(",",cg)
        content.map{line =>  mars.unmarshall(line)}
      }
    }
  }

  val f = fixture()

  describe("The TextFileBackend"){
    val be = f.backend
    it("can create the file if it doesn't exist"){
      be.makeNewFile() should equal(true)
      file.exists() should equal(true)

      Given("DataRows[] it can write them to the file in overwrite mode")
      be.write(f.rows.iterator, Overwrite)

      And("it can read rows from the file")
      val data = be.read()
      f.content.zip(data.toList).foreach{pair =>
        pair._1 should equal(f.marshaller.marshall(pair._2))
      }

      And("it can append a single row to a file")
      val nl = "Jimmie V,98734,M"
      val nr = f.colNames.zip(nl.split(",").map{_.trim})
      be.write(DataRow(nr: _*), Append)

      And("then read it back in")
      val apData: Iterator[DataRow[_]] = be.read()
      (f.content.+:(nl)).zip(apData.toList).foreach{pair =>
        pair._1 should equal(f.marshaller.marshall(pair._2))
      }

      And("finall it can delete the file, if you really want to")
      be.delete()
      file.exists() should equal(false)
    }
  }
}
