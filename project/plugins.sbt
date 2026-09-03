resolvers += Resolver.sbtPluginRepo("releases")
resolvers += Resolver.mavenCentral

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")
//addSbtPlugin("com.github.sbt" % "sbt-sonatype" % "3.12.2")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")
