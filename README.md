__We now have (a google group for DataExpress)[https://groups.google.com/forum/#!forum/dataexpress-etl]! Please come join the discussion and help us build a community__

# Getting Started

The official (DataExpress)[http://dataexpress.research.chop.edu/] website contains a tutorial and other basic information for new users

## Downloading the binaries

The (releases page)[https://github.com/cbmi/dataexpress/releases] contains the jars and tutorial starter files for DataExpress

## Compiling from source

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

## Getting Involved and Contributing
DataExpress has humble beginnings but grand ambitions. We want to engage with those of you who, like us, tired of hacking together the same kinds of one-off scritps, tired of using expensive BI tools and annoyed GUI's that force you to draw hundreds of lines to generate XML by the terabyte just to copy some tables. We can't do this without a community of like-minded people. We need people who know data, specific database implementations, and Scala. We want people who value simplicity instead of over-engineering. Help us simplify ETL for everyone.

* Visit the (dataexpress-etl)[https://groups.google.com/forum/#!forum/dataexpress-etl] google group and see what ideas people are kicking around (this is new so there might not be any yet!)
* Take a look at the (open tickets)[https://github.com/cbmi/dataexpress/issues?state=open] we have against the project
* All long-term development is done against the develop branch or a feature branch off of develop.
* We're still adjusting to this whole community development model, please check with us before embarking on a major coding endeavor in the event that oen of us is already contemplating it. Use the google group or github issues so everyone can see the exchage.
* Tests are a bit problematic for a project like this, primarily because it's hard to use things like Travis-ci to test the builds against proprietary databases (I'm looking at you Oracle and SQLServer). We want to make this easier, so if you've got some way to help with that, that would make the project better for everyone.




