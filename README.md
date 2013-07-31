__We now have [a google group for DataExpress](https://groups.google.com/forum/#!forum/dataexpress-etl)! Please come join the discussion and help us build a community__

# Getting Started

The official [DataExpress](http://dataexpress.research.chop.edu/) website contains a tutorial and other basic information for new users

## Downloading Binaries

The [releases page](https://github.com/cbmi/dataexpress/releases) contains the jars and tutorial starter files for DataExpress

## Compiling from Source

The source code includes an SBT build.sbt configuration allowing for building.

All commands assume you are working in the dataexpress root directory, here ~/dataexpress) 

1. Clone the source to a local project directory, here assume it is ~/dataexpress

2. Ensure both Scala SBT is installed see http://www.scala-sbt.org/

3. To compile current source code:
    $sbt compile

4. To test current source code:
    $sbt test

5. To package WITHOUT dependencies:
    $sbt package

6. To package a "fat jar" containing dependencies (e.g. database drivers)
    $sbt assembly
    *Note: this excludes the Scala library; to include that as well, edit the line in build.sbt to read assembleArtifact in packageScala := true*   

7. To create scaladocs
    $sbt doc

8. To start a Scala console with DataExpress on the class path
    $sbt console

-Note this will also automatically perform the following imports:
    import edu.chop.cbmi.dataExpress.dsl.ETL._
    import edu.chop.cbmi.dataExpress.dsl.ETL
    import edu.chop.cbmi.dataExpress.dsl.stores.SqlDb
    import edu.chop.cbmi.dataExpress.dataModels.RichOption._

# Getting Involved and Contributing
See [CONTRIBUTORS.md](https://github.com/cbmi/dataexpress/blob/master/CONTRIBUTORS.md#getting-involved-and-contributing) in the repo for all the details. 

tl;dr: we would love your contributions!
