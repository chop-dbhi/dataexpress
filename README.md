__We now have [a google group for DataExpress](https://groups.google.com/forum/#!forum/dataexpress-etl)! Please come join the discussion and help us build a community__

# Getting Started

The official [DataExpress](http://dataexpress.research.chop.edu/) website contains a tutorial and other basic information for new users

## Downloading Binaries

The [releases page](https://github.com/cbmi/dataexpress/releases) contains the jars and tutorial starter files for DataExpress. You can use these if you're not familiar with Scala or the various JVM build processes

## Using from SBT, Maven, etc...

DataExpress is published to the Sonatype OSS snapshot and release repositories. These repositories are pushed to Maven Central every 2 hours. This means that you can use releases without adding custom resolvers to your sbt build definition or custom repositories to your Maven POM. The group id (organization) is `edu.chop.research` and the artifact id (name) is `dataexpress` 

To add a dependency to the latest **release** version of DataExpress to your sbt (0.12.x) build definition:

    libraryDependencies += "edu.chop.research" %% "dataexpress" % "0.9.1.3"

If you would like to live on the (often bleeding-) edge, a **snapshot** version of DataExpress can go in your sbt (0.12.x) build definition:

    resolvers += Opts.resolver.sonatypeSnapshots

    libraryDependencies += "edu.chop.research" %% "dataexpress" % "0.9.1.4-SNAPSHOT"

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
