package simpleneojgen.v3

import java.util.UUID
import scala.collection.immutable.TreeSet

import cats.data.IndexedStateT
import cats.implicits._
import simpleneojgen.utils.{Neo4s, WrappedDriver}
import simpleneojgen.v3.ParametersGeneration._
import simpleneojgen.v3.StatementGeneration.Implicits._
import simpleneojgen.v3.StatementGeneration._

object ComplexMain {

//  val graph = new WrappedDriver("bolt://127.0.0.1:7687", "neo4j", "test")

//  def createDefinition(): Either[String, UUID] = {
  def createDefinition(): Unit = {

    val fullDefinition = FullVertex("definition", TreeSet("WORKFLOW_DEFINITION"), Map("uid" -> VariablePath("$definitionUid")))
    val fullUser = FullVertex("user", TreeSet("USER"), Map("uid" -> VariablePath("$userUid")))

    // Match definition
    val definitionPart: Result[Unit] = for {
      _ <- Dsl.matchs(List(fullDefinition, fullUser))
      _ <- Dsl.withs(List(fullDefinition, fullUser))
    } yield ()

    val fullInstance = FullVertex("instance", TreeSet("WORKFLOW_INSTANCE"),
      Map(
        "enqueuedAt" -> VariablePath("workflowInstanceData.enqueuedAt"),
        "createdAt" -> VariablePath("workflowInstanceData.createdAt"),
        "createdBy" -> VariablePath("workflowInstanceData.createdBy"),
        "uid" -> VariablePath("workflowInstanceData.uid"),
        "isMega" -> VariablePath("workflowInstanceData.isMega"),
      )
    )

    // Create instance
    val instancePart: Result[Unit] = for {
      _ <- Dsl.unwind(VariablePath("$workflowInstanceDatas"), "workflowInstanceData")
      _ <- Dsl.create(List(
        Path(fullInstance, List(("DEFINED_BY", fullDefinition))),
        Path(fullInstance.asRef, List(("CREATED_BY", fullDefinition)))
      ))
      _ <- Dsl.withs(List(fullDefinition, fullUser, fullInstance, WithName("workflowInstanceData")))
    } yield ()

    val fullInputDefn = FullVertex("inputDefn", TreeSet("WORKFLOW_ARTIFACT_DEFINITION"), Map("label" -> VariablePath("iad.label")))

    val fullInstanceArtifact = FullVertex("instanceArtifact", TreeSet("WORKFLOW_ARTIFACT_INSTANCE"),
      Map(
        "uid" -> VariablePath("iad.uid"),
        "createdBy" -> VariablePath("iad.createdBy"),
        "createdAt" -> VariablePath("iad.createdAt")
      ))

    val fullInputArtifact = FullVertex("inputArtifact", TreeSet("ARTIFACT"),
      Map(
        "uid" -> VariablePath("uidAndIndex.uid")
      ))

    // Create input artifacts
    val inputPart: Result[Unit] = for {
      _ <- Dsl.unwind(VariablePath("workflowInstanceData.inputArtifactDatas"), "iad")
      _ <- Dsl.matchs(List(Path(fullDefinition.asRef, List(("TAKES_INPUT", fullInputDefn)))))
      _ <- Dsl.withs(List(fullDefinition, fullUser, fullInstance, fullInputDefn, WithName("workflowInstanceData"), WithName("iad")))

      _ <- Dsl.create(List(
        Path(fullInstance.asRef, List(("INPUT", fullInstanceArtifact), ("DEFINED_BY", fullInputDefn.asRef)))
      ))
      _ <- Dsl.withs(List(fullDefinition, fullUser, fullInstance, fullInputDefn, WithName("workflowInstanceData"), WithName("iad"), fullInstanceArtifact))

      _ <- Dsl.unwind(VariablePath("iad.artifactUidsAndIndices"), "uidAndIndex")

      _ <- Dsl.matchs(fullInputArtifact)

      _ <- Dsl.withs(List(fullDefinition, fullUser, fullInstance, fullInputDefn, WithName("workflowInstanceData"),
        WithName("iad"), fullInstanceArtifact, WithName("uidAndIndex"), fullInputArtifact))

      // Todo: Path attributes
      _ <- Dsl.create(List(Path(fullInstanceArtifact, List(("DEFINES", fullInputArtifact)))))

      _ <- Dsl.withs(List(fullDefinition, fullUser, fullInstance, fullInputDefn, WithName("workflowInstanceData"),
          WithCollect(fullInputArtifact, "inputArtifactList")))

      _ <- Dsl.withs(List(fullDefinition, fullUser, fullInstance, WithName("workflowInstanceData"),
        WithCollect(WithMap(Map(
          "artifact" -> fullInstanceArtifact,
          "inputDefn" -> fullInputDefn
        )), "inputArtifacts")))


      _ <- Dsl.returns(List(fullDefinition, fullUser, fullInstance, WithName("workflowInstanceData"), WithName("inputArtifacts")))
    } yield ()


    val scriptProgram: Result[Unit] =
      for {
        _ <- definitionPart
        _ <- instancePart
        _ <- inputPart
      } yield ()

//    val scriptParts = scriptProgram.run(EmptyGenData).left.map(println).right.get._1.statementsInOrder
//    val script = StatementRenderer.render(scriptParts)
//    println("Result:\n" + script)

    {
      val scriptParts = definitionPart.run(EmptyGenData).left.map(println).right.get._1.statementsInOrder
      val script = StatementRenderer.render(scriptParts)
      println("ResultA:\n" + script)
    }
    {
      val scriptParts = instancePart.run(EmptyGenData).left.map(println).right.get._1.statementsInOrder
      val script = StatementRenderer.render(scriptParts)
      println("ResultB:\n" + script)
    }
    {
      val scriptParts = inputPart.run(EmptyGenData).left.map(println).right.get._1.statementsInOrder
      val script = StatementRenderer.render(scriptParts)
      println("ResultC:\n" + script)
    }
    {
      val scriptParts = scriptProgram.run(EmptyGenData).left.map(println).right.get._1.statementsInOrder
      val script = StatementRenderer.render(scriptParts)
      println("ResultD:\n" + script)
    }

    /*
UNWIND workflowInstanceData.inputArtifactDatas AS iad
    MATCH
      (workflowDefinition)
        -[:TAKES_INPUT]-> (inputDefn :WORKFLOW_ARTIFACT_DEFINITION :LAKE :V_1 { label: iad.label })
    WITH workflowInstanceData, workflowInstance, workflowDefinition, inputDefn, iad, user

    CREATE
      (workflowInstance) -[:INPUT]->  (instanceArtifact :WORKFLOW_ARTIFACT_INSTANCE :LAKE :V_1 {
        uid: iad.uid, createdBy: iad.createdBy, createdAt: iad.createdAt
      }) -[:DEFINED_BY]-> (inputDefn)
    WITH workflowInstanceData, workflowInstance, instanceArtifact, workflowDefinition, inputDefn, iad, user

    UNWIND iad.artifactUidsAndIndices AS uidAndIndex
      MATCH (inputArtifact :ARTIFACT :LAKE :V_1 :EXISTING { uid: uidAndIndex.uid })
      WITH workflowInstanceData, workflowInstance, instanceArtifact, workflowDefinition, inputDefn, iad, inputArtifact, uidAndIndex, user

      CREATE (instanceArtifact) -[:DEFINES { index: uidAndIndex.index }]-> (inputArtifact)

    WITH workflowInstanceData, workflowInstance, instanceArtifact, workflowDefinition, inputDefn, iad, user,
      collect(inputArtifact) AS inputArtifactList

WITH workflowInstanceData, workflowInstance, workflowDefinition, user,
      collect([instanceArtifact, inputArtifactList, inputDefn]) AS inputArtifacts


UNWIND workflowInstanceData.outputArtifactDatas AS iad
    MATCH
      (workflowDefinition)
        -[:PRODUCES_OUTPUT]-> (outputDefn :WORKFLOW_ARTIFACT_DEFINITION :LAKE :V_1 { label: iad.label })
        -[:RECEIVES_CLASS]-> (artiClass :LAKE_ARTIFACT_CLASS :LAKE :V_1)
    WITH workflowInstanceData, workflowInstance, workflowDefinition, inputArtifacts, outputDefn, iad, artiClass, user

    CREATE
      (workflowInstance) -[:OUTPUT]->  (instanceArtifact :WORKFLOW_ARTIFACT_INSTANCE :LAKE :V_1 {
        uid: iad.uid, createdBy: iad.createdBy, createdAt: iad.createdAt
      }) -[:DEFINED_BY]-> (outputDefn)
    WITH workflowInstanceData, workflowInstance, workflowDefinition, inputArtifacts, outputDefn, iad, artiClass, user,
      instanceArtifact

    UNWIND iad.artifactDatas AS artifactData
      CREATE
        (instanceArtifact)
          -[:OUTPUT_ARTIFACT { index: artifactData.index }]-> (outputArtifact :ARTIFACT :LAKE :V_1 :ARTIFACT :LAKE :V_1 :EXISTING :ARTIFACT :LAKE :V_1 { isTransient: artifactData.isTransient, blobDate: artifactData.blobDate, blobPath: artifactData.blobPath, createdAt: artifactData.createdAt, createdBy: artifactData.createdBy, uid: artifactData.uid, blobUid: artifactData.blobUid })
          -[:HAS_CLASS]-> (artiClass),
        (outputArtifact) -[:CREATED_BY]-> (user)

    WITH workflowInstanceData, workflowInstance, workflowDefinition, inputArtifacts, outputDefn, iad, artiClass, user,
      instanceArtifact, collect(outputArtifact) AS outputArtifactList


WITH workflowInstanceData, workflowInstance, workflowDefinition, inputArtifacts, user,
  collect([instanceArtifact, outputArtifactList, outputDefn]) AS outputArtifacts


RETURN
  workflowInstance, inputArtifacts, outputArtifacts, workflowInstance.uid AS workflowInstanceUid

     */

  }


  def main(args: Array[String]): Unit = {
    createDefinition()
  }

}
