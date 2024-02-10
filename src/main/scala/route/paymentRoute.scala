package route

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.{CustomSerializer, DefaultFormats, MappingException, jackson}
import repo.PaymentRepository
import domain.{JsonFormatss, Payment, PaymentUpdate, Paymentstatus, Paymenttype}
import org.json4s.JsonAST.JString

import scala.util.{Failure, Success}



object PaymentRoute extends Json4sSupport {
  implicit val serialization = jackson.Serialization
  implicit val formats = JsonFormatss.formats

  val route =
    pathPrefix("payment") {
      concat(
        pathEnd {
          concat(
            get {
              onComplete(PaymentRepository.getAllPayment()) {
                case Success(payments) => complete(StatusCodes.OK, payments)
                case Failure(ex) => complete(StatusCodes.InternalServerError, s"Ошибка при получении платежей: ${ex.getMessage}")
              }
            },
            post {
              entity(as[Payment]) { payment =>
                onComplete(PaymentRepository.addPayment(payment)) {
                  case Success(newPaymentId) =>
                    complete(StatusCodes.Created, s"ID нового платежа: $newPaymentId")
                  case Failure(ex) =>
                    complete(StatusCodes.InternalServerError, s"Не удалось создать платеж: ${ex.getMessage}")
                }
              }
            }
          )
        },
        path(Segment) { paymentid =>
          concat(
            get {
              onComplete(PaymentRepository.getPaymentById(paymentid)) {
                case Success(payment) => complete(StatusCodes.OK, payment)
                case Failure(ex) => complete(StatusCodes.InternalServerError, s"Ошибка при получении платежа: ${ex.getMessage}")
              }
            },
            put {
              entity(as[PaymentUpdate]) { updatedPayment =>
                onComplete(PaymentRepository.updatePayment(paymentid, updatedPayment)) {
                  case Success(_) => complete(StatusCodes.OK, "Платеж успешно обновлен")
                  case Failure(ex) => complete(StatusCodes.InternalServerError, s"Не удалось обновить платеж: ${ex.getMessage}")
                }
              }
            },
            delete {
              onComplete(PaymentRepository.deletePayment(paymentid)) {
                case Success(_) => complete(StatusCodes.NoContent, "Платеж успешно удален")
                case Failure(ex) => complete(StatusCodes.InternalServerError, s"Не удалось удалить платеж: ${ex.getMessage}")
              }
            }
          )
        },
        get {
          parameter("param") { param =>
            onComplete(PaymentRepository.findPaymentByParams(param.toString)) {
              case Success(payments) => complete(StatusCodes.OK, payments)
              case Failure(ex) => complete(StatusCodes.InternalServerError, s"Ошибка при поиске платежей: ${ex.getMessage}")
            }
          }
        }
      )
    }
}