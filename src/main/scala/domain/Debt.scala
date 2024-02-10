package domain
import domain.Debtstatus.Debtstatus
import org.json4s.JsonAST.JString
import org.json4s.{CustomSerializer, DefaultFormats, Formats, MappingException}

import java.text.SimpleDateFormat
import java.util.Date

case class Debt(
                    debtid: Int,
                    amount: Double,
                    userid: Int,
                    courseid:Int,
                    debtdate:Date,
                    debtstatus:Debtstatus.Debtstatus,
                  )


case class DebtUpdate(
                 debtid: Option[Int] = None,
                 amount: Option[Double] = None,
                 userid: Option[Int] = None,
                 courseid: Option[Int] = None,
                 debtdate: Option[Date] = None,
                 debtstatus: Option[Debtstatus] = None
               )

object Debtstatus extends Enumeration {
  type Debtstatus = Value

  val PAID, UNPAID = Value
}

object DebtStatusSerializer extends CustomSerializer[Debtstatus.Value](format => (
  {
    case JString(s) => Debtstatus.withName(s)
    case value => throw new MappingException(s"Can't convert $value to DebtStatus")
  },
  {
    case ddebt: Debtstatus.Value => JString(ddebt.toString)
  }
))
object JsonFormats {
  implicit val formats: Formats = DefaultFormats + DebtStatusSerializer
}