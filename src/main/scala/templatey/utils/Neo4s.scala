package templatey.utils

import java.time.{Instant, ZonedDateTime}
import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.Try

import cats.implicits._
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.v1.types.{Node, Type}
import org.neo4j.driver.v1.{Record, Value}


sealed trait Neo4s
case object Neo4sNull extends Neo4s
case class Neo4sList(vals: List[Neo4s]) extends Neo4s

sealed trait Neo4sMap extends Neo4s { val vals: Map[String, Neo4s] }
case class Neo4sMapOnly(vals: Map[String, Neo4s]) extends Neo4sMap
case class Neo4sNode(vals: Map[String, Neo4s], labels: Set[String]) extends Neo4sMap

sealed trait Neo4sAtom extends Neo4s
case class Neo4sInt(v: Long) extends Neo4sAtom
case class Neo4sBoolean(v: Boolean) extends Neo4sAtom
case class Neo4sString(v: String) extends Neo4sAtom


object Neo4s { neo4s =>
  val ts = InternalTypeSystem.TYPE_SYSTEM

  private def asFromMap[S](m: Neo4sMap, key: String)(fn: Neo4s => Either[String, S]): Either[String, S] =
    m.vals.get(key).map(fn) match {
      case Some(r) => r
      case None    => Left(s"Map did not contain $key:\n$m")
    }

  def unsafeAsUUID(neo4s: Neo4s): UUID =
    asUUID(neo4s).left.map(err => throw new Exception(err)).right.get

  def asUUID(v: Neo4s): Either[String, UUID] =
    asString(v).flatMap(s => Try(UUID.fromString(s)).toEither.left.map(_.getMessage))

  def asInstant(v: Neo4s): Either[String, Instant] =
    asLong(v).flatMap(s => Try(Instant.ofEpochMilli(s)).toEither.left.map(_.getMessage))

  def asBoolean(v: Neo4s): Either[String, Boolean] = v match {
    case Neo4sBoolean(cas) => Right(cas)
    case other => Left(s"Expected boolean, got $other")
  }

  def unsafeAsBoolean(v: Neo4s): Boolean =
    asBoolean(v).left.map(err => throw new Exception(err)).right.get

  def asString(v: Neo4s): Either[String, String] = v match {
    case Neo4sString(cas) => Right(cas)
    case other => Left(s"Expected string, got $other")
  }

  def asZonedDateTime(v: Neo4s): Either[String, ZonedDateTime] = v match {
    case Neo4sString(cas) => Try(ZonedDateTime.parse(cas)).toEither.left.map(_.getMessage)
    case other => Left(s"Expected string, got $other")
  }

  object optional {
    // Todo: Do the null bit for all optionals
    def asNode(v: Neo4sMap, key: String): Either[String, Option[Neo4sNode]] =
      v.vals.get(key) match {
        case Some(Neo4sNull) => Right(None)
        case Some(other) => neo4s.asNode(other).map(Some.apply)
        case None => Right(None)
      }

    def asZonedDateTime(v: Neo4sMap, key: String): Either[String, Option[ZonedDateTime]] =
      v.vals.get(key).map(neo4s.asZonedDateTime) match {
        case Some(r) => r.map(v => Some(v))
        case None    => Right(None)
      }

    // Todo: Also deal with Neo4sNull
    def asString(v: Neo4sMap, key: String): Either[String, Option[String]] =
      v.vals.get(key).map(neo4s.asString) match {
        case Some(r) => r.map(v => Some(v))
        case None    => Right(None)
      }

    def asBoolean(v: Neo4sMap, key: String): Either[String, Option[Boolean]] =
      v.vals.get(key).map(neo4s.asBoolean) match {
        case Some(r) => r.map(v => Some(v))
        case None    => Right(None)
      }
  }

  def unsafeAsString(v: Neo4s): String =
    asString(v).left.map(err => throw new Exception(err)).right.get

  def asLong(v: Neo4s): Either[String, Long] = v match {
    case Neo4sInt(cas) => Right(cas)
    case other => Left(s"Expected long, got $other")
  }

  def asInt(v: Neo4s): Either[String, Int] = v match {
    case Neo4sInt(cas) =>
      if (cas.isValidInt)
        Right(cas.intValue())
      else
        Left(s"Not a valid int (too long?): $cas")
    case other => Left(s"Expected long, got $other")
  }

  def unsafeAsLong(v: Neo4s): Long =
    asLong(v).left.map(err => throw new Exception(err)).right.get


  def asNode(v: Neo4s): Either[String, Neo4sNode] = v match {
    case n: Neo4sNode => Right(n)
    case other => Left(s"Expected node, got $other")
  }

  def unsafeAsNode(v: Neo4s): Neo4sNode =
    asNode(v).left.map(err => throw new Exception(err)).right.get

  def asList(v: Neo4s): Either[String, Neo4sList] = v match {
    case n: Neo4sList => Right(n)
    case other => Left(s"Expected list, got $other")
  }

  def unsafeAsList(v: Neo4s): Neo4sList =
    asList(v).left.map(err => throw new Exception(err)).right.get

  def asMap(v: Neo4s): Either[String, Neo4sMap] = v match {
    case n: Neo4sMap => Right(n)
    case other => Left(s"Expected map, got $other")
  }

  def unsafeAsMap(v: Neo4s): Neo4sMap =
    asMap(v).left.map(err => throw new Exception(err)).right.get

  def asBoolean(m: Neo4sMap, key: String): Either[String, Boolean] = asFromMap(m, key)(asBoolean)
  def asList(m: Neo4sMap, key: String): Either[String, Neo4sList] = asFromMap(m, key)(asList)
  def asString(m: Neo4sMap, key: String): Either[String, String] = asFromMap(m, key)(asString)
  def asUUID(m: Neo4sMap, key: String): Either[String, UUID] = asFromMap(m, key)(asUUID)
  def asLong(m: Neo4sMap, key: String): Either[String, Long] = asFromMap(m, key)(asLong)
  def asInt(m: Neo4sMap, key: String): Either[String, Int] = asFromMap(m, key)(asInt)
  def asNode(m: Neo4sMap, key: String): Either[String, Neo4sNode] =
    asFromMap(m, key)(asNode).left.map(err => s"When attempting to get key '$key', $err")

  def asMap(m: Neo4sMap, key: String): Either[String, Neo4sMap] = asFromMap(m, key)(asMap)
  def asInstant(m: Neo4sMap, key: String): Either[String, Instant] = asFromMap(m, key)(asInstant)

  def parseRecord(record: Record): Either[String, Neo4sMapOnly] = parse(record)

  type E[S] = Either[String, S]

  def parse(record: Record): Either[String, Neo4sMapOnly] = {
    val x: List[(String, Either[String, Neo4s])] = record.asMap(v => parse(v)).asScala.toList
    val y: List[Either[String, (String, Neo4s)]] = x.map { case (k, vResult) => vResult.map(v => (k, v)) }
    val z: Either[String, List[(String, Neo4s)]] = y.sequence[E, (String, Neo4s)]
    val zz: Either[String, Neo4sMapOnly] = z.map(m => Neo4sMapOnly(m.toMap))
    zz

//    record.asMap(v => parse(v)).asScala.toList
//      .map { case (k, vResult) => vResult.map(v => (k, v)) }
//      .sequence.map(mapAsList => Neo4sMapOnly(mapAsList.toMap))
  }

  def parse(node: Node): Either[String, Neo4sNode] = {
    node.asMap(v => parse(v)).asScala.toList
      .map { case (k, vResult) => vResult.map(v => (k, v)) }
      .sequence[E, (String, Neo4s)]
      .map(mapAsList => Neo4sNode(mapAsList.toMap, node.labels().asScala.toSet))
  }

  def parse(value: Value): Either[String, Neo4s] = {
    if (value.`type`() == ts.NULL()) {
      Right(Neo4sNull)
    } else if (value.`type`() == ts.NODE()) {
      parse(value.asNode())
    } else if (value.`type`() == ts.LIST()) {
      val r: List[Either[String, Neo4s]] = value.asList(v => parse(v)).asScala.toList
      val r2: Either[String, Neo4s] = r.sequence[E, Neo4s].map(Neo4sList)
      r2
    } else if (value.`type`() == ts.MAP()) {
      value.asMap(v => parse(v)).asScala.toList
        .map { case (k, vResult) => vResult.map(v => (k, v)) }
        .sequence[E, (String, Neo4s)].map(mapAsList => Neo4sMapOnly(mapAsList.toMap))
    } else if (isPrimitive(value.`type`())) {
      parsePrimitive(value)
    } else {
      Left(s"Cannot handle neo4j type ${value.`type`().name()}")
    }
  }

  def isPrimitive(`type`: Type): Boolean =
    if (`type` == ts.BOOLEAN()) {
      true
    } else if (`type` == ts.INTEGER()) {
      true
    } else if (`type` == ts.STRING()) {
      true
    } else {
      false
    }

  def parsePrimitive(value: Value): Either[String, Neo4s] = {
    if (value.`type`() == ts.BOOLEAN()) {
      Right(Neo4sBoolean(value.asBoolean()))
    } else if (value.`type`() == ts.INTEGER()) {
      Right(Neo4sInt(value.asLong()))
    } else if (value.`type`() == ts.STRING()) {
      Right(Neo4sString(value.asString()))
    } else {
      Left(s"Cannot handle primitive neo4j type ${value.`type`().name()}")
    }
  }
}


