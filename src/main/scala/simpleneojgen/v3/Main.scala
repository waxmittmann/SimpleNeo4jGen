package simpleneojgen.v3

import java.util.UUID
import scala.collection.immutable.TreeSet

import cats.implicits._

import simpleneojgen.utils.{Neo4s, WrappedDriver}
import simpleneojgen.v3.ParametersGeneration._
import simpleneojgen.v3.StatementGeneration.Implicits._
import simpleneojgen.v3.StatementGeneration._

object Main {

  val graph = new WrappedDriver("bolt://127.0.0.1:7687", "neo4j", "test")

  def createDefinition(): Either[String, UUID] = {
    val defn =
      FullVertex("definition", TreeSet("WORKFLOW_DEFINITION"), Map("uid" -> VariablePath("$definitionUid")))

//    val scriptParts =
//      List(
//        Create(defn),
//        Return(List(defn))
//      )


    val scriptProgram = for {
      _ <- Dsl.create(defn)
      _ <- Dsl.returns(List(defn))
    } yield ()

    val scriptParts = scriptProgram.run(EmptyGenData).left.map(println).right.get._1.statementsInOrder

    val script = StatementRenderer.render(scriptParts)

    val params =
      AttributeMap(Map(
        "definitionUid" -> AttributeUid(UUID.randomUUID())
      ))

    println(script + "\n\n" + renderParams(params))

    val result = graph.write(script, renderMapParam(params)).left.map(println).right.get
    if (result.hasNext()) {

      val result2 =
        for {
          r <- Neo4s.parse(result.next()).flatMap(m => Neo4s.asNode(m, "definition"))

          uid <- Neo4s.asUUID(r, "uid")
        } yield uid

      result2
    } else
      Left("Failed: Produced no result")
  }


  def main(args: Array[String]): Unit = {

    val r =
      for {
        defnUid <- createDefinition()
      } yield defnUid

    println(r)

//    val (uid1, uid2) = createArtifacts().left.map(println).right.get
//    println(uid1 + ", " + uid2)
//    createInstance(uid1, uid2)//.left.map(println).right.get

    graph.close()
  }

}
