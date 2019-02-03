package simpleneojgen.v2

import simpleneojgen.v2.StatementGeneration._

object StatementRenderer {


  def render(statements: List[Statement]): String = {
    statements.map(render).mkString("\n")
  }

  def render(statement: Statement): String = statement match {
    case s: Match   => renderMatch(s)
    case s: With    => renderWith(s)
    case s: Return  => renderReturn(s)
    case s: Unwind  => renderUnwind(s)
    case s: Create  => renderCreate(s)
  }

//  private def fullToReferenceVertex(full: FullVertex): VertexRef = VertexRef(full.name)

  private def renderLabels(labels: Set[String]): String = labels.map(l => s":$l").mkString(" ")

  private def renderAttribute(primitive: VariablePath): String = primitive.key

  private def renderAttributes(attributes: Map[String, VariablePath]): String = {
    val attrString = attributes.map { case (k, v) => s"$k: ${renderAttribute(v)}" }.mkString(", ")
    s"{ $attrString }"
  }

  private def renderVertex(vertex: Vertex): String =
    vertex match {
      case VertexRef(name)                      => s"($name)"
      case FullVertex(name, labels, attributes) => s"($name ${renderLabels(labels)} ${renderAttributes(attributes)})"
    }

  //  def renderCreate(path: Path): String = renderCreate(List(path))

  def renderCreate(create: Create ): String = {
    val renderedPaths = create.paths.map(renderPath).mkString(",\n")
    s"""CREATE
       |${indent(renderedPaths, 3)}""".stripMargin
  }

  //  def renderMatch(path: Path): String = renderMatch(List(path))

  def renderMatch(matchs: Match): String = {
    val renderedPaths = matchs.paths.map(renderPath).mkString(",\n")
    s"""MATCH
       |${indent(renderedPaths, 3)}""".stripMargin
  }

  private def renderPath(path: Path): String =
    path.edgesAndVertices.foldLeft(List(renderVertex(path.firstVertex))) { case (statement, (edgeLabel, vertex)) =>
      s"-[:$edgeLabel]-> ${renderVertex(vertex)}" :: statement
    }.reverse.mkString


  //  def renderWith(part: WithPart): String = renderWith(List(part))

  def renderWith(withs: With): String = {
    val body = withs.parts.foldLeft(List[String]()) { case (soFar, part) => renderWithPart(part) :: soFar }.reverse.mkString(", ")
    s"WITH $body"
  }

  def renderReturn(returns: Return): String = {
    val body = returns.parts.foldLeft(List[String]()) { case (soFar, part) => renderWithPart(part) :: soFar }.reverse.mkString(", ")
    s"RETURN $body"
  }

  private def renderWithPart(part: WithPart): String = part match {
    case WithVertex(vertex, as) => as.map(as => s"${vertex.name} AS $as").getOrElse(vertex.name)

    case WithCollect(toCollect, as) => s"COLLECT(${renderWithPart(toCollect)}) AS $as"

    case WithName(name) => name

    case WithMap(vertices: Map[String, Vertex]) => {
      val renderedMap = vertices.map { case (key, vertex) => s"$key: ${vertex.name}" }
      s"{ $renderedMap }"
    }

  }

  def renderUnwind(unwind: Unwind): String =
    s"UNWIND (${unwind.primitive.key}) AS ${unwind.as}"

  private def indent(str: String, by: Int = 3): String = {
    str.split("\n").map(line => s"${" " * by}$line").mkString("\n")
  }
}
