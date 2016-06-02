logLevel := Level.Warn

resolvers += "repo1_maven" at "https://repo1.maven.org/maven2/"
resolvers += "jboss" at "http://repository.jboss.org/nexus/content/groups/public"
resolvers += "ibiblio" at "http://mirrors.ibiblio.org/pub/mirrors/maven2"
resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.5.3")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")