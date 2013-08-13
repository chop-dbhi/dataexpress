package edu.chop.cbmi.dataExpress.test.dsl

import java.io.File
import edu.chop.cbmi.dataExpress.dataModels.{DataRow}
import edu.chop.cbmi.dataExpress.backends.file._
import edu.chop.cbmi.dataExpress.backends.file.TextFileBackend
import edu.chop.cbmi.dataExpress.backends.file.DelimiterCustomMarshaller
import org.scalatest.{BeforeAndAfter, GivenWhenThen, FunSpec}
import org.scalatest.matchers.ShouldMatchers
import edu.chop.cbmi.dataExpress.dsl.ETL
import edu.chop.cbmi.dataExpress.dsl.ETL._
import edu.chop.cbmi.dataExpress.dataModels.RichOption._
import edu.chop.cbmi.dataExpress.dsl.stores.FileStore

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 5/10/13
 * Time: 1:29 PM
 */
class GetFromFileSpec extends FunSpec with GivenWhenThen with ShouldMatchers with BeforeAndAfter{

  lazy val file = new File("./output/GetFromFileSpec.dat")

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
      def headerColumnNames = TextFileBackend.getHeaderRowColumnNames(file,",")
      val marshaller = DelimiterCustomMarshaller(",", headerColumnNames, (a:Array[Option[String]])=>{
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
        //val cg = SeqColumnNames(colNames)
        val mars = DelimiterMarshaller(",",colNames)
        content.map{line =>  mars.unmarshall(line)}
      }
    }
  }

  val f = fixture()

  describe("The DSL get statement") {
    it("should allow the user to retrieve data from a text file") {
      val source = "source"
      register store FileStore(f.backend) as source
      val table = get from source

      And("the table should be an iterator over DataRow that match the content")
      (0 /: table) {
        (idx, row) =>
          val lineVals = f.content(idx).split(",")
          row.Name.as[String] match {
            case Some(s) => lineVals(0).trim should equal(s)
            case _ => lineVals(0).trim should equal("")
          }
          row.ID.as[Int] match {
            case Some(i) => lineVals(1).trim.toInt should equal(i)
            case _ => lineVals(1).trim should equal("")
          }
          row.Known.as[Boolean] match {
            case Some(b) => lineVals(2).toBoolean should equal(b)
            case _ => lineVals.length should be < (3)
          }
          idx + 1
      }
    }
  }
}
