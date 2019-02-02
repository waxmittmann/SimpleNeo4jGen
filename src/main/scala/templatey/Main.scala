package templatey

import scala.collection.immutable.TreeSet

object Main {


  sealed trait Primitive
  case class PInt(i: Int) extends Primitive
  case class PBoolean(b: Boolean) extends Primitive
  case class PString(s: String) extends Primitive

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

  }

  import Implicits._



  /*
      CREATE
        (n ...) - ....,
      WITH (n), ...




   */




  def main(args: Array[String]): Unit = {

    /*

    for {

      aAttrs <-
      a <- defineVertex(aAttrs)
      b <- defineVertex(...)
      c <- defineVertex(...)

      _ <- create(a, List(("EDGE_TO", b), ("EDGE_TO", c))




    } yield {

    }

    */



    val v1 = VertexRef("a")
    val v2 = FullVertex("b", TreeSet("B", "B2"), Map("bInt" -> PInt(5)))
    val v3 = FullVertex("c", TreeSet("C", "C2"), Map("cBool" -> PBoolean(false)))

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
  }


  def renderLabels(labels: Set[String]): String = labels.map(l => s":$l").mkString(" ")


  def renderAttribute(primitive: Primitive): String = {
    primitive match {
      case PInt(i)      => i.toString
      case PBoolean(b)  => b.toString
      case PString(s)   => s
    }
  }

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
       |$renderedPaths""".stripMargin
  }

  def renderPath(path: Path): String =
    path.edgesAndVertices.foldLeft(List(renderVertex(path.firstVertex))) { case (statement, (edgeLabel, vertex)) =>
      s"-[$edgeLabel]-> ${renderVertex(vertex)}" :: statement
    }.reverse.mkString


  def renderWith(parts: List[WithPart]): String = {
    val body = parts.foldLeft(List[String]()) { case (soFar, part) => renderWithPart(part) :: soFar }.reverse.mkString(", ")
    s"WITH $body"
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




}
