resolvers += Resolver.url("artifactory",                                                        
new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns)



addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.5")

//Scct for code coverage
resolvers += Classpaths.typesafeResolver

resolvers += "scct-github-repository" at "http://mtkopone.github.com/scct/maven-repo"

addSbtPlugin("reaktor" % "sbt-scct" % "0.2-SNAPSHOT")
