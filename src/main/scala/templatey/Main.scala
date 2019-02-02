package templatey

import java.util
import java.util.UUID
import scala.collection.immutable.TreeSet

object Main {



  val graph = new WrappedDriver("bolt://127.0.0.1:7687", "neo4j", "test")

//  sealed trait Primitive
//  case class PInt(i: Int) extends Primitive
//  case class PBoolean(b: Boolean) extends Primitive
//  case class PString(s: String) extends Primitive




  sealed trait Attribute
  case class AttributeInt(i: Int) extends Attribute
  case class AttributeString(s: String) extends Attribute
  case class AttributeUid(uid: UUID) extends Attribute
  case class AttributeMap(m: Map[String, Attribute]) extends Attribute
  case class AttributeList(l: List[Attribute]) extends Attribute


  case class VariablePath(key: String)

  sealed trait Vertex { val name: String }
  case class VertexRef(name: String) extends Vertex
  case class FullVertex(name: String, labels: TreeSet[String], attributes: Map[String, VariablePath]) extends Vertex {
    def asRef: VertexRef = VertexRef(name)
  }

  case class Path(firstVertex: Vertex, edgesAndVertices: List[(String, Vertex)])

//  case class Create()


  sealed trait WithPart
  case class WithVertex(vertex: Vertex, as: Option[String] = None) extends WithPart
  case class WithMap(vertices: Map[String, Vertex]) extends WithPart
  case class WithCollect(toCollect: WithPart, as: String) extends WithPart
  case class WithName(name: String) extends WithPart

  object Implicits {

    implicit def toWith(vertex: Vertex): WithVertex = WithVertex(vertex, None)

    implicit def toWith(vertices: List[Vertex]): List[WithVertex] = vertices.map(vertex => WithVertex(vertex, None))

    implicit def toPath(vertex: Vertex): Path = Path(vertex, List.empty)

  }

  import Implicits._



  /*


      MATCH
        (workflowDefinition :WORKFLOW_DEFINITION { uid: wdUid })
      WITH workflowDefinition
      CREATE
        (workflowInstance :WORKFLOW_INSTANCE { uid: data.uid } ) -[DEFINED_BY]-> (workflowDefinition)
      WITH workflowDefinition, workflowInstance
      UNWIND data.inputs AS inputArtifact
        MATCH
          (inArti :ARTIFACT { uid: inputArtifact.uid })
        WITH workflowDefinition, workflowInstance, inArti
        CREATE
          (workflowInstance) -[:HAS_INPUT]-> (inArti)
      WITH workflowDefinition, workflowInstance, COLLECT(inArti) AS inputArtifacts
      UNWIND data.outputs AS outputArtifact
        CREATE
          (workflowInstance) -[:PRODUCES_OUTPUT]-> (outArti :ARTIFACT { uid: outputArtifact.uid })
      WITH workflowDefinition, workflowInstance, inputArtifacts, COLLECT(outArti) AS outputArtifacts
      RETURN workflowDefinition, workflowInstance, inputArtifacts, outputArtifacts
   */


//  def renderParams(params: AttributeMap): util.HashMap[String, Object] = {
//    params.m.mapValues()
//  }

  import scala.collection.JavaConverters._

  def renderMapParam(m: AttributeMap): util.Map[String, Object] =
    m.m.mapValues(renderParams).asJava

  def renderListParam(l: List[Attribute]): util.List[Object] =
    l.map(renderParams).asJava

  def renderParams(attr: Attribute): Object = attr match {
    case AttributeInt(i) => i.asInstanceOf[Object]
    case AttributeString(s) => s.asInstanceOf[Object]
    case AttributeUid(uid) => uid.toString.asInstanceOf[Object]
    case am: AttributeMap => renderMapParam(am)
    case AttributeList(l) => renderListParam(l)
  }

  def test2(): Unit = {
    val workflowDefinition =
      FullVertex("workflowDefinition", TreeSet("WORKFLOW_DEFINITION"), Map("uid" -> VariablePath("$definitionUid")))

    val workflowInstance =
      FullVertex("workflowInstance", TreeSet("WORKFLOW_INSTANCE"), Map("uid" -> VariablePath("$instanceUid")))

//    val workflowDefinitionRef = fullToReferenceVertex(workflowDefinitionFull)
//    val workflowInstanceRef = fullToReferenceVertex(workflowInstanceFull)


    val inArti =
      FullVertex("inArt", TreeSet("ARTIFACT"), Map("uid" -> VariablePath("inputArtifact.uid")))

    val outArti =
      FullVertex("outArt", TreeSet("ARTIFACT"), Map("uid" -> VariablePath("outputArtifact.uid")))

    val script =
      List(
        renderMatch(workflowDefinition),
        renderWith(workflowDefinition),
        renderCreate(Path(workflowInstance, List(("DEFINED_BY", workflowDefinition.asRef)))),
        renderWith(List(workflowDefinition, workflowInstance)),

        renderUnwind(VariablePath("$inputs"), "inputArtifact"),
        renderMatch(inArti),
        renderWith(List(workflowDefinition, workflowInstance, inArti)),
        renderCreate(Path(workflowInstance.asRef, List(("HAS_INPUT", inArti.asRef)))),
        renderWith(List(WithVertex(workflowDefinition), WithVertex(workflowInstance), WithCollect(inArti, "inputs"))),

        renderUnwind(VariablePath("$outputs"), "outputArtifact"),
        renderCreate(Path(workflowInstance.asRef, List(("PRODUCES_OUTPUT", outArti)))),
        renderReturn(List(WithVertex(workflowDefinition), WithVertex(workflowInstance), WithName("inputs"), WithCollect(outArti, "outputs")))

        //renderReturn(List(WithVertex(workflowDefinition), WithVertex(workflowInstance), WithName("inputs"), WithName("outputs")))
      ).mkString("\n")

    val params =
      AttributeMap(Map(
        "definitionUid" -> AttributeUid(UUID.randomUUID()),
        "instanceUid" -> AttributeUid(UUID.randomUUID()),
        "inputs" -> AttributeList(List(
          AttributeMap(Map("uid" -> AttributeUid(UUID.randomUUID()))),
          AttributeMap(Map("uid" -> AttributeUid(UUID.randomUUID())))
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

//    val session = graph.driver.session()
//    try {
//      session.writeTransaction {
//        tx =>
//          val result = tx.run(script, renderMapParam(params))
//          tx.success()
//
//          if (result.hasNext())
//            println(result.next().asMap())
//          else
//            println("Failed: Produced no result")
//      }
//    } catch {
//      case e => println(e)
//    } finally {
//      session.close()
//    }
    graph.close()
  }

  def main(args: Array[String]): Unit = {
    test2()
  }

  def test1(): Unit = {
    val v1 = VertexRef("a")
    val v2 = FullVertex("b", TreeSet("B", "B2"), Map("bInt" -> VariablePath("data.bInt")))
    val v3 = FullVertex("c", TreeSet("C", "C2"), Map("cBool" -> VariablePath("data.cBool")))

    val p1 = Path(
      v1,
      List(("EDGE_TO_B", v2))
    )

    val p2 = Path(
      v1,
      List(("EDGE_TO_C", v3))
    )

    println(renderCreate(List(p1, p2)))
    println(renderWith(List(v1, v2, v3)))
    println(renderReturn(List(v1, v2, v3)))
  }


  def fullToReferenceVertex(full: FullVertex): VertexRef = VertexRef(full.name)

  def renderLabels(labels: Set[String]): String = labels.map(l => s":$l").mkString(" ")


  def renderAttribute(primitive: VariablePath): String = primitive.key
//  {
//    primitive match {
//      case PInt(i)      => i.toString
//      case PBoolean(b)  => b.toString
//      case PString(s)   => s
//    }
//  }

  def renderAttributes(attributes: Map[String, VariablePath]): String = {
    val attrString = attributes.map { case (k, v) => s"$k: ${renderAttribute(v)}" }.mkString(", ")
    s"{ $attrString }"
  }

  def renderVertex(vertex: Vertex): String =
    vertex match {
      case VertexRef(name)                      => s"($name)"
      case FullVertex(name, labels, attributes) => s"($name ${renderLabels(labels)} ${renderAttributes(attributes)})"
    }

  def renderCreate(path: Path): String = renderCreate(List(path))

  def renderCreate(paths: List[Path]): String = {
    val renderedPaths = paths.map(renderPath).mkString(",\n")
    s"""CREATE
       |${indent(renderedPaths, 3)}""".stripMargin
  }

  def renderMatch(path: Path): String = renderMatch(List(path))

  def renderMatch(paths: List[Path]): String = {
    val renderedPaths = paths.map(renderPath).mkString(",\n")
    s"""MATCH
       |${indent(renderedPaths, 3)}""".stripMargin
  }

  def renderPath(path: Path): String =
    path.edgesAndVertices.foldLeft(List(renderVertex(path.firstVertex))) { case (statement, (edgeLabel, vertex)) =>
      s"-[:$edgeLabel]-> ${renderVertex(vertex)}" :: statement
    }.reverse.mkString


  def renderWith(part: WithPart): String = renderWith(List(part))

  def renderWith(parts: List[WithPart]): String = {
    val body = parts.foldLeft(List[String]()) { case (soFar, part) => renderWithPart(part) :: soFar }.reverse.mkString(", ")
    s"WITH $body"
  }

  def renderReturn(parts: List[WithPart]): String = {
    val body = parts.foldLeft(List[String]()) { case (soFar, part) => renderWithPart(part) :: soFar }.reverse.mkString(", ")
    s"RETURN $body"
  }

  def renderWithPart(part: WithPart): String = part match {
    case WithVertex(vertex, as) => as.map(as => s"${vertex.name} AS $as").getOrElse(vertex.name)

    case WithCollect(toCollect, as) => s"COLLECT(${renderWithPart(toCollect)}) AS $as"

    case WithName(name) => name

    case WithMap(vertices: Map[String, Vertex]) => {
      val renderedMap = vertices.map { case (key, vertex) => s"$key: ${vertex.name}" }
      s"{ $renderedMap }"
    }

  }

  def renderUnwind(primitive: VariablePath, as: String): String =
    s"UNWIND (${primitive.key}) AS $as"

  def indent(str: String, by: Int = 3): String = {
    str.split("\n").map(line => s"${" " * by}$line").mkString("\n")
  }



}
