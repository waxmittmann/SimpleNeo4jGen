package simpleneojgen.v3

import scala.collection.immutable.TreeSet

import cats.implicits._
import cats.data.{EitherT, State, StateT}

object StatementGeneration {

  val EmptyGenData = GenData(List.empty, List.empty, List.empty)

  case class GenData(
    statements: List[Statement],
    matchedVertices: List[FullVertex],
    createdVertices: List[FullVertex],
//    matchedVertices: Map[String, FullVertex],
//    createdVertices: Map[String, FullVertex],
    done: Boolean = false
  ) {
    lazy val matchedVerticesM: Map[String, FullVertex] = matchedVertices.map { v => (v.name, v) }.toMap
    lazy val createdVerticesM: Map[String, FullVertex] = createdVertices.map { v => (v.name, v) }.toMap

    def statementsInOrder = statements.reverse

    def findVerticesByName(vertices: List[Vertex]): List[Vertex] =
      vertices.flatMap { v => matchedVerticesM.get(v.name) } ++ vertices.flatMap { v => createdVerticesM.get(v.name) }

//    def findFullVertices(vertexes: List[FullVertex]): List[FullVertex] =
//      vertexes.flatMap { v => matchedVerticesM.get(v.name) } ++ vertexes.flatMap { v => createdVerticesM.get(v.name) }
//
//    def findRefVertices(vertexes: List[VertexRef]): List[VertexRef] =
//      vertexes.flatMap { v => matchedVerticesM.get(v.name) } ++ vertexes.flatMap { v => createdVerticesM.get(v.name) }

    def containsVertex(vertex: Vertex): Boolean =
      matchedVerticesM.get(vertex.name).isDefined || createdVerticesM.get(vertex.name).isDefined

  }

  type GenState[S] = State[GenData, S]

  case class Error(message: String, data: GenData)

//  type Result[S] = Either[Error, GenState[S]]

  type E[S] = Either[Error, S]

  type Result[S] = StateT[E, GenData, S]

  object Dsl {
    //def returns(vertexes: List[FullVertex]): EitherT[GenState, String, GenState] = ???

//    def returns(vertexes: List[FullVertex]): State[GenState, String, GenState] = ???
    def returns(parts: List[WithPart]): Result[Unit] = {
      StateT.modifyF[E, GenData] { curState =>

//        parts.map { _ match {
//          case WithVertex(vertex, as) => Map(as -> vertex)
//
//          case WithMap(vertices) => vertices
//
//          case WithCollect(toCollect, as) =>
//
//          case WithName(name) =>
//        }}

        // Todo: Check that all the parts exist

        Right(curState.copy(statements = Return(parts) :: curState.statements, done = true))
      }
    }

    def withs(parts: List[WithPart]): Result[Unit] = {
      StateT.modifyF[E, GenData] { curState =>

        //        parts.map { _ match {
        //          case WithVertex(vertex, as) => Map(as -> vertex)
        //
        //          case WithMap(vertices) => vertices
        //
        //          case WithCollect(toCollect, as) =>
        //
        //          case WithName(name) =>
        //        }}

        // Todo: Check that all the parts exist; remove (or move to 'old') all vertices that we didn't add to WITH;
        // Todo: rename those we used 'as' on

        Right(curState.copy(With(parts) :: curState.statements))
      }
    }

    def create(paths: List[Path]): Result[Unit] = {
      StateT.modifyF[E, GenData] { curState =>
        val fullVerticesInPaths = paths.flatMap(p => p.vertices.collect { case fv: FullVertex => fv })
        val fullVerticesFound = curState.findVerticesByName(fullVerticesInPaths)

        val refVerticesInPaths = paths.flatMap(p => p.vertices.collect { case fv: VertexRef => fv })
        val refVerticesFound = curState.findVerticesByName(refVerticesInPaths)

        if (fullVerticesFound.nonEmpty) {
          Left(Error("Some full vertices already exist.", curState))
        } else if (refVerticesFound.size != refVerticesInPaths.size) {
          Left(Error("Some ref vertices don't exist.", curState))
        } else {
          Right(curState.copy(
            statements = Create(paths) :: curState.statements,
            createdVertices = curState.createdVertices ++ fullVerticesInPaths
          ))
        }
      }
    }



  }

  case class VariablePath(key: String)

  sealed trait Vertex { val name: String }
  case class VertexRef(name: String) extends Vertex
  case class FullVertex(name: String, labels: TreeSet[String], attributes: Map[String, VariablePath]) extends Vertex {
    def asRef: VertexRef = VertexRef(name)
  }

  case class Path(firstVertex: Vertex, edgesAndVertices: List[(String, Vertex)]) {
    lazy val vertices: List[Vertex] = firstVertex :: edgesAndVertices.map(_._2)
  }

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
