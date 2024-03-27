package domain

import domain.Usertype.Usertype
import org.json4s.JsonAST.JString
import org.json4s.{CustomSerializer, DefaultFormats, Formats, JsonAST, MappingException}

case class User(
                 userid: Int,
                 name: String,
                 lastname: String,
                 usertype: Usertype.Usertype,
               )

case class UserUpdate(
                       userid: Option[Int]=None,
                       name: Option[String]=None,
                       lastname: Option[String]=None,
                       usertype: Option[Usertype]=None,
               )

object Usertype extends Enumeration {
  type Usertype = Value
  val STUDENT, TEACHER, ADMINSTRATOR = Value
}

object UserTypeSerializer extends CustomSerializer[Usertype.Value](format => (
  {
    case JString(s) => Usertype.withName(s)
    case value => throw new MappingException(s"Can't convert $value to UserType")
  },
  {
    case usertypee: Usertype.Value => JString(usertypee.toString)
  }
))
object JsonFormatsss {
  implicit val formats: Formats = DefaultFormats + UserTypeSerializer
}