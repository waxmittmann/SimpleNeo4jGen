package templatey

import scala.collection.immutable.TreeSet

object Main {


//  sealed trait Primitive
//  case class PInt(i: Int) extends Primitive
//  case class PBoolean(b: Boolean) extends Primitive
//  case class PString(s: String) extends Primitive

  case class Primitive(key: String)

  sealed trait Ele

  sealed trait Vertex extends Ele { val name: String }
  case class VertexRef(name: String) extends Vertex
  case class FullVertex(name: String, labels: TreeSet[String], attributes: Map[String, Primitive]) extends Vertex

  case class Path(firstVertex: Vertex, edgesAndVertices: List[(String, Vertex)])

//  case class Create()


  sealed trait WithPart
  case class WithVertex(vertex: Vertex, as: Option[String]) extends WithPart
  case class WithMap(vertices: Map[String, Vertex]) extends WithPart
  case class WithCollect(toCollect: WithPart, as: Option[String]) extends WithPart

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


  def test2(): Unit = {
    val workflowDefinitionFull =
      FullVertex("workflowDefinition", TreeSet("WORKFLOW_DEFINITION"), Map("uid" -> Primitive("$definitionUid")))

    val workflowInstanceFull =
      FullVertex("workflowInstance", TreeSet("WORKFLOW_INSTANCE"), Map("uid" -> Primitive("$instanceUid")))

    val workflowDefinitionRef = fullToReferenceVertex(workflowDefinitionFull)
    val workflowInstanceRef = fullToReferenceVertex(workflowInstanceFull)


    val inArtiFull =
      FullVertex("inArt", TreeSet("ARTIFACT"), Map("uid" -> Primitive("inputArtifact.uid")))

    renderMatch(workflowDefinitionFull)
    renderWith(workflowDefinitionFull)
    renderCreate(Path(workflowInstanceFull, List(("DEFINED_BY", workflowDefinitionRef))))
    renderWith(List(workflowDefinitionFull, workflowInstanceFull))
    renderUnwind(Primitive("$inputs"), "inputArtifact")
    renderMatch(inArtiFull)
    renderWith(List(workflowDefinitionFull, workflowInstanceFull, inArtiFull))
    renderCreate(Path(workflowInstanceRef, List(("HAS_INPUT", workflowDefinitionRef))))



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

  def main(args: Array[String]): Unit = {

  }

  def test1(): Unit = {
    val v1 = VertexRef("a")
    val v2 = FullVertex("b", TreeSet("B", "B2"), Map("bInt" -> Primitive("data.bInt")))
    val v3 = FullVertex("c", TreeSet("C", "C2"), Map("cBool" -> Primitive("data.cBool")))

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


  def renderAttribute(primitive: Primitive): String = primitive.key
//  {
//    primitive match {
//      case PInt(i)      => i.toString
//      case PBoolean(b)  => b.toString
//      case PString(s)   => s
//    }
//  }

  def renderAttributes(attributes: Map[String, Primitive]): String = {
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
      s"-[$edgeLabel]-> ${renderVertex(vertex)}" :: statement
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

    case WithCollect(toCollect, as) =>
      as.map(as => s"COLLECT(${renderWithPart(toCollect)}) AS $as").getOrElse(s"COLLECT(${renderWithPart(toCollect)})")

    case WithMap(vertices: Map[String, Vertex]) => {
      val renderedMap = vertices.map { case (key, vertex) => s"$key: ${vertex.name}" }
      s"{ $renderedMap }"
    }

  }

  def renderUnwind(primitive: Primitive, as: String): String =
    s"UNWIND ($primitive) AS $as"

  def indent(str: String, by: Int = 3): String = {
    str.split("\n").map(line => s"${" " * by}$line").mkString("\n")
  }



}
