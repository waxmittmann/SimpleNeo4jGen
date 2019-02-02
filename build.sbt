name := "Templatey"

version := "0.1"

scalaVersion := "2.12.6"

val argonautVersion = "6.2" // todo: update
val catsVersion = "1.1.0"
val catsEffectVersion = "0.10.1"
val circeVersion = "0.9.3"
val doobieVersion = "0.4.4" // todo: update
val http4sVersion = "0.17.5" // todo: update
val monocleVersionScalaz = "1.4.0" // todo: update
val monocleVersionCats = "1.5.0-cats-M1" // todo: update
val scalazVersion = "7.2.16" // todo: update
val scalacheckVersion = "1.13.4" // todo: update
val specsVersion = "4.0.0" // todo: update
val neo4jBase = "3.3.3"
val neo4jDriver = "1.5.1"

val neo4jVersions = Map[String, String](
  "ogm" -> "3.1.0"
)

// Todo: Factor out dependencies
libraryDependencies ++= Seq(
  // Scalaz
//  "org.scalaz" %% "scalaz-core" % scalazVersion,
//  "org.scalaz" %% "scalaz-effect" % scalazVersion,
//  "org.scalaz" %% "scalaz-concurrent" % scalazVersion,

  // Cats
  "org.typelevel" %% "cats-core" % catsVersion,
//  "org.typelevel" %% "cats-effect" % catsEffectVersion,
//  "org.typelevel" %% "cats-mtl" % catsVersion,
//  "org.typelevel" %% "dogs-core" % "0.6.9",

  // Eff
//  "org.atnos" %% "eff" % "4.5.0",

  // Monocle
//  "com.github.julien-truffaut" %%  "monocle-core"  % monocleVersionCats,
//  "com.github.julien-truffaut" %%  "monocle-macro" % monocleVersionCats,
//  "com.github.julien-truffaut" %%  "monocle-law"   % monocleVersionCats % "test",

  // Http4s
//  "org.http4s" %% "http4s-core" % http4sVersion,
//  "org.http4s" %% "http4s-dsl" % http4sVersion,

  // Circe
//  "io.circe" %% "circe-core" % circeVersion,
//  "io.circe" %% "circe-generic" % circeVersion,
//  "io.circe" %% "circe-parser" % circeVersion,

  // Argonaut
//  "io.argonaut" %% "argonaut" % argonautVersion,
//  "io.argonaut" %% "argonaut-monocle" % argonautVersion,
//  "io.argonaut" %% "argonaut-cats" % argonautVersion, // Cats
//  "io.argonaut" %% "argonaut-scalaz" % argonautVersion,  // Scalaz

  // Doobie scalaz
//  "org.tpolecat" %% "doobie-core" % doobieVersion, // scalaz
//  "org.tpolecat" %% "doobie-h2" % doobieVersion, // scalaz
//  "org.tpolecat" %% "doobie-hikari" % doobieVersion, // scalaz
//  "org.tpolecat" %% "doobie-postgres" % doobieVersion, // scalaz
//  "org.tpolecat" %% "doobie-specs2" % doobieVersion, // scalaz

  // Doobie cats
//  "org.tpolecat" %% "doobie-core-cats" % doobieVersion, // cats
//  "org.tpolecat" %% "doobie-h2-cats" % doobieVersion, // scalaz
//  "org.tpolecat" %% "doobie-hikari-cats" % doobieVersion, // scalaz
//  "org.tpolecat" %% "doobie-postgres-cats" % doobieVersion, // scalaz
//  "org.tpolecat" %% "doobie-specs2-cats" % doobieVersion, // scalaz

  // Neo4j
//  "org.neo4j" % "neo4j" % neo4jBase,
//  "org.neo4j" % "neo4j-common" % neo4jBase,
//  "org.neo4j" % "neo4j-cypher" % neo4jBase,
//  "org.neo4j" % "neo4j-bolt" % neo4jBase,
//  "org.neo4j.driver" % "neo4j-java-driver" % neo4jDriver,


  // PureConfig
  //  "com.github.pureconfig" %% "pureconfig" % "0.9.1"

  // Shapeless
  //"com.chuusai" %% "shapeless" % "2.3.3",

  // Test
  "org.specs2" %% "specs2-core" % specsVersion % "test",
  "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
)

// For macro annotations
addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.0" cross CrossVersion.full)