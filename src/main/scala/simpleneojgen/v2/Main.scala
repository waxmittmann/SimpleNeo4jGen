package simpleneojgen.v2

import java.util.UUID
import scala.collection.immutable.TreeSet

import simpleneojgen.utils.{Neo4s, WrappedDriver}
import simpleneojgen.v2.ParametersGeneration._
import simpleneojgen.v2.StatementGeneration.Implicits._
import simpleneojgen.v2.StatementGeneration._

object Main {

  val graph = new WrappedDriver("bolt://127.0.0.1:7687", "neo4j", "test")

  def createDefinition(): Either[String, UUID] = {
    val defn =
      FullVertex("definition", TreeSet("WORKFLOW_DEFINITION"), Map("uid" -> VariablePath("$definitionUid")))

    val scriptParts =
      List(
        Create(defn),
        Return(List(defn))
      )

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

  def createArtifacts(): Either[String, (UUID, UUID)] = {
    val arti =
      FullVertex("artifact", TreeSet("ARTIFACT"), Map("uid" -> VariablePath("artifactData.uid")))

    val scriptCommands =
      List(
        Unwind(VariablePath("$artifacts"), "artifactData"),
        Create(arti),
        Return(List(WithCollect(arti, "artifacts")))
      )

    val script = StatementRenderer.render(scriptCommands)

    val params =
      AttributeMap(Map(
        "artifacts" -> AttributeList(List(
          AttributeMap(Map("uid" -> AttributeUid(UUID.randomUUID()))),
          AttributeMap(Map("uid" -> AttributeUid(UUID.randomUUID())))
        ))
      ))

    println(script + "\n\n" + renderParams(params))

    val result = graph.write(script, renderMapParam(params)).left.map(println).right.get
    if (result.hasNext()) {

      val result2 =
        for {
          r <- Neo4s.parse(result.next()).flatMap(m => Neo4s.asList(m, "artifacts"))

          artifacts <- Neo4s.asList(r)

          artifact1 <- Neo4s.asNode(artifacts.vals(0))
          arti1Uid <- Neo4s.asUUID(artifact1, "uid")
          artifact2 <- Neo4s.asNode(artifacts.vals(1))
          arti2Uid <- Neo4s.asUUID(artifact2, "uid")
        } yield (arti1Uid, arti2Uid)

      result2
    } else
      Left("Failed: Produced no result")
  }

  def createInstance(defnUid: UUID, uidA: UUID, uidB: UUID): Unit = {
    val workflowDefinition =
      FullVertex("workflowDefinition", TreeSet("WORKFLOW_DEFINITION"), Map("uid" -> VariablePath("$definitionUid")))

    val workflowInstance =
      FullVertex("workflowInstance", TreeSet("WORKFLOW_INSTANCE"), Map("uid" -> VariablePath("$instanceUid")))

    val inArti =
      FullVertex("inArt", TreeSet("ARTIFACT"), Map("uid" -> VariablePath("inputArtifact.uid")))

    val outArti =
      FullVertex("outArt", TreeSet("ARTIFACT"), Map("uid" -> VariablePath("outputArtifact.uid")))

    val scriptParts =
      List(
        Match(workflowDefinition),
        With(workflowDefinition),
        Create(Path(workflowInstance, List(("DEFINED_BY", workflowDefinition.asRef)))),
        With(List(workflowDefinition, workflowInstance)),

        Unwind(VariablePath("$inputs"), "inputArtifact"),
        Match(inArti),
        With(List(workflowDefinition, workflowInstance, inArti)),
        Create(Path(workflowInstance.asRef, List(("HAS_INPUT", inArti.asRef)))),
        With(List(WithVertex(workflowDefinition), WithVertex(workflowInstance), WithCollect(inArti, "inputs"))),

        Unwind(VariablePath("$outputs"), "outputArtifact"),
        Create(Path(workflowInstance.asRef, List(("PRODUCES_OUTPUT", outArti)))),
        Return(List(WithVertex(workflowDefinition), WithVertex(workflowInstance), WithName("inputs"), WithCollect(outArti, "outputs")))

        //renderReturn(List(WithVertex(workflowDefinition), WithVertex(workflowInstance), WithName("inputs"), WithName("outputs")))
      )

    val script = StatementRenderer.render(scriptParts)

    val params =
      AttributeMap(Map(
        "definitionUid" -> AttributeUid(defnUid),
        "instanceUid" -> AttributeUid(UUID.randomUUID()),
        "inputs" -> AttributeList(List(
          AttributeMap(Map("uid" -> AttributeUid(uidA))),
          AttributeMap(Map("uid" -> AttributeUid(uidB)))
        )),
        "outputs" -> AttributeList(List(
          AttributeMap(Map("uid" -> AttributeUid(UUID.randomUUID()))),
          AttributeMap(Map("uid" -> AttributeUid(UUID.randomUUID())))
        ))
      ))

    println(script + "\n\n" + renderParams(params))

    val result = graph.write(script, renderMapParam(params)).right.get
    if (result.hasNext())
      println(result.next().asMap())
    else
      println("Failed: Produced no result")
  }

  def main(args: Array[String]): Unit = {

    val r =
      for {
        artiUids <- createArtifacts()
        (uid1, uid2) = artiUids

        defnUid <- createDefinition()
      } yield createInstance(defnUid, uid1, uid2)

    println(r)

//    val (uid1, uid2) = createArtifacts().left.map(println).right.get
//    println(uid1 + ", " + uid2)
//    createInstance(uid1, uid2)//.left.map(println).right.get

    graph.close()
  }

}
