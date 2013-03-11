Considerably more information is available at the DataExpress website:
http://dataexpress.research.chop.edu/

Compiling from source: The source code includes an SBT build.sbt configuration allowing either Maven or SBT to be used for building. 

All  commands assume you are working in the dataexpress root directory, here ~/dataexpress) 

1. Download the source to a local project directory, here assume it is ~/dataexpress

2. Ensure SBT is installed see http://www.scala-sbt.org/

3. To compile current source code:
    $sbt compile

4. To test current source code:
    $sbt test

5. To package WITHOUT dependencies:
    $sbt package

6. To package WITH dependencies (does not include Scala library, to do so in ~/dataexpress/build.sbt set assembleArtifact in packageScala := true):
    $sbt assembly

7. To create scaladocs
    $sbt doc

8. To start a Scala console with DataExpress on the class path
    $sbt console

-Note this will also automatically perform the following imports:
    import edu.chop.cbmi.dataExpress.dsl.ETL._
    import edu.chop.cbmi.dataExpress.dsl.ETL
    import edu.chop.cbmi.dataExpress.dsl.stores.SqlDb
    import edu.chop.cbmi.dataExpress.dataModels.RichOption._
