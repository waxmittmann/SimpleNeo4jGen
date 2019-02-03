package simpleneojgen

import java.util
import java.util.UUID
import scala.collection.immutable.TreeSet

import scala.collection.JavaConverters._

object StatementGeneration {

  case class VariablePath(key: String)

  sealed trait Vertex { val name: String }
  case class VertexRef(name: String) extends Vertex
  case class FullVertex(name: String, labels: TreeSet[String], attributes: Map[String, VariablePath]) extends Vertex {
    def asRef: VertexRef = VertexRef(name)
  }

  case class Path(firstVertex: Vertex, edgesAndVertices: List[(String, Vertex)])

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

  def fullToReferenceVertex(full: FullVertex): VertexRef = VertexRef(full.name)

  def renderLabels(labels: Set[String]): String = labels.map(l => s":$l").mkString(" ")


  def renderAttribute(primitive: VariablePath): String = primitive.key

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
