package edu.chop.cbmi.dataExpress.test.backends.file

import org.scalatest.{BeforeAndAfter, GivenWhenThen, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import edu.chop.cbmi.dataExpress.backends.file._
import java.io.File
import edu.chop.cbmi.dataExpress.dataModels.{SeqColumnNames, DataRow}
import edu.chop.cbmi.dataExpress.backends.file.DelimiterMarshaller
import edu.chop.cbmi.dataExpress.backends.file.TextFileBackend

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/9/13
 * Time: 12:04 PM
 */
class TextFileBackendSpec extends FunSpec with GivenWhenThen with ShouldMatchers with BeforeAndAfter{

  lazy val file1 = new File("./output/TextFileBackendSpec.dat")
  lazy val file2 = new File("./output/TextFileBackendHeaderSpec.dat")

  before{
     if(file1.exists())file1.delete()
    if(file2.exists())file2.delete()
  }

  after{
    if(file1.exists())file1.delete()
    if(file2.exists())file2.delete()
  }

  def fixture() = {
    new {
      val colNames = Seq("Name","ID","Gender")
      val content = List("Bob,249,M","Jane Doe,3430,F","Mike R.,,M","Steve,83839,")
      val cng = SeqColumnNames(colNames)
      val marshaller = DelimiterMarshaller(",",cng)
      val backend = TextFileBackend(file1, marshaller)
      val backendWithHeader = TextFileBackend(file2, marshaller,1)
      val rows = {
        val cg = SeqColumnNames(colNames)
        val mars = DelimiterMarshaller(",",cg)
        content.map{line =>  mars.unmarshall(line)}
      }
    }
  }

  val f = fixture()

  describe("The TextFileBackend"){
    val be1 = f.backend
    val be2 = f.backendWithHeader
    it("can create the file if it doesn't exist"){
      be1.makeNewFile() should equal(true)
      be2.makeNewFile() should equal(true)
      file1.exists() should equal(true)
      file2.exists() should equal(true)

      Given("DataRows[] it can write them to the file in overwrite mode")
      be1.write(f.rows.iterator, Overwrite)
      //be2.write(DataRow(f.colNames.map{cn=>(cn,cn)}: _*), Overwrite)
      be2.writeHeader(DataRow(f.colNames.map{cn=>(cn,cn)}: _*))
      be2.write(f.rows.iterator, Append)

      And("it can read rows from the file")
      f.content.zip(be1.read().toList).foreach{pair =>
        pair._1 should equal(f.marshaller.marshall(pair._2))
      }
      f.content.zip(be2.read().toList).foreach{pair =>
        pair._1 should equal(f.marshaller.marshall(pair._2))
      }

      And("it can append a single row to a file")
      val nl = "Jimmie V,98734,M"
      val nr = f.colNames.zip(nl.split(",").map{_.trim})
      be1.write(DataRow(nr: _*), Append)
      be2.write(DataRow(nr: _*), Append)

      And("then read it back in")
      (f.content.:+(nl)).zip(be1.read().toList).foreach{pair =>
        pair._1 should equal(f.marshaller.marshall(pair._2))
      }
      (f.content.:+(nl)).zip(be2.read().toList).foreach{pair =>
        pair._1 should equal(f.marshaller.marshall(pair._2))
      }

      And("it can delete the file, if you really want to")
      be1.delete()
      file1.exists() should equal(false)
      be2.delete()
      file2.exists() should equal(false)
    }
  }
}
