package simpleneojgen.v2

import scala.collection.immutable.TreeSet

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

  sealed trait Statement
  case class Match(paths: List[Path]) extends Statement
  case class With(parts: List[WithPart]) extends Statement
  case class Return(parts: List[WithPart]) extends Statement
  case class Unwind(primitive: VariablePath, as: String) extends Statement
  case class Create(paths: List[Path]) extends Statement

  object Implicits {

    implicit def toWith(vertex: Vertex): WithVertex = WithVertex(vertex, None)

    implicit def toWiths(vertex: Vertex): List[WithVertex] = List(WithVertex(vertex, None))

    implicit def toWith(vertices: List[Vertex]): List[WithVertex] = vertices.map(vertex => WithVertex(vertex, None))

    implicit def toPath(vertex: Vertex): Path = Path(vertex, List.empty)

    implicit def toMatch(path: Path): List[Path] = List(path)

//    implicit def toMatch(vertex: Vertex): Match = Match(List(Path(vertex, List.empty)))
    implicit def toPaths(vertex: Vertex): List[Path] = List(Path(vertex, List.empty))
  }


}
