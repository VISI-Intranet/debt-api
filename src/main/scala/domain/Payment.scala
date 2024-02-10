package domain
import domain.Paymentstatus.Paymentstatus
import domain.Paymenttype.Paymenttype
import org.json4s.JsonAST.JString
import org.json4s.{CustomSerializer, DefaultFormats, Formats, JsonAST, MappingException}
import org.json4s.jackson.JsonMethods.parse

import java.util.Date
case class Payment(
                       paymentid: Int,
                       amount: Double,
                       userid: Int,
                       courseid:Int,
                       paymentdate:Date,
                       paymentstatus:Paymentstatus.Paymentstatus,
                       paymenttype: Paymenttype.Paymenttype
                     )


case class PaymentUpdate(
                          paymentid: Option[Int] = None,
                          amount: Option[Double] = None,
                          userid: Option[Int] = None,
                          courseid: Option[Int] = None,
                          paymentdate: Option[Date] = None,
                          paymentstatus: Option[Paymentstatus] = None,
                          paymenttype: Option[Paymenttype] = None
                        )

object Paymentstatus extends Enumeration {
  type Paymentstatus = Value
  val SUCCESSFUL, FAILED, INPROCESSING = Value
}

object Paymenttype extends Enumeration {
  type Paymenttype = Value
  val BANK, MOBILETRANSFER, CASH = Value
}

//object PaymenttypeSerializer extends CustomSerializer[Paymenttype.Value](format => (
//  {
//    case JString(s) => Paymenttype.withName(s)
//    case value => throw new MappingException(s"Can't convert $value to Paymenttype")
//  },
//  {
//    case pppayment: Paymenttype.Value => JString(pppayment.toString)
//  }
//))
//
//object PaymentstatusSerializer extends CustomSerializer[Paymentstatus.Value](format => (
//  {
//    case JString(s) => Paymentstatus.withName(s)
//    case value => throw new MappingException(s"Can't convert $value to Paymentstatus")
//  },
//  {
//    case ppayment: Paymentstatus.Value => JString(ppayment.toString)
//  }
//))
//object JsonFormatss {
//  implicit val formats: Formats = DefaultFormats + PaymenttypeSerializer + PaymentstatusSerializer;
//}


object EnumSerializer extends CustomSerializer[Enumeration#Value](format => (
  {
    case JString(s) =>
      // Проверяем, есть ли среди значений Paymenttype
      if (Paymenttype.values.exists(_.toString == s)) {
        Paymenttype.withName(s)
      } else if (Paymentstatus.values.exists(_.toString == s)) {
        Paymentstatus.withName(s)
      } else {
        throw new MappingException(s"Unknown enumeration value: $s")
      }
    case value =>
      throw new MappingException(s"Can't convert $value to Enumeration")
  },
  {
    case enumValue: Enumeration#Value =>
      JString(enumValue.toString)
  }
))

object JsonFormatss {
  implicit val formats: Formats = DefaultFormats + EnumSerializer
}