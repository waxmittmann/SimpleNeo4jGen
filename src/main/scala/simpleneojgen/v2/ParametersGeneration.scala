package simpleneojgen.v2

import java.util
import java.util.UUID
import scala.collection.JavaConverters._

object ParametersGeneration {

  sealed trait Attribute
  case class AttributeInt(i: Int) extends Attribute
  case class AttributeString(s: String) extends Attribute
  case class AttributeUid(uid: UUID) extends Attribute
  case class AttributeMap(m: Map[String, Attribute]) extends Attribute
  case class AttributeList(l: List[Attribute]) extends Attribute

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

}
