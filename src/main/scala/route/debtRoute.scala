package route

import Alpakka.Operations.SendMessageAndWaitForResponsAlpakka
import Alpakka.RabbitMQModel.RabbitMQModel
import RabbitMQ.RabbitMQOperation.Operations.Formatter.extractContentList
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer
import akka.stream.alpakka.amqp.{AmqpConnectionProvider, AmqpLocalConnectionProvider}
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.{DefaultFormats, Formats, jackson}
import domain.{Debt, DebtUpdate, JsonFormats}
import repo.DebtRepository

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object DebtRoute extends Json4sSupport {
  implicit val serialization = jackson.Serialization
  implicit val formats = JsonFormats.formats
  implicit lazy val system: ActorSystem = ActorSystem("web-system")
  implicit lazy val mat: Materializer = Materializer(system)
  implicit val ec:  ExecutionContext = scala.concurrent.ExecutionContext.global

  val pubStudentNameForDebtMQModel: RabbitMQModel = RabbitMQModel("DebtPublisher", "UniverSystem", "univer.debt-api.studentNameForDebtByIdGet")
  val replyStudentNameForDebtMQModel: RabbitMQModel = RabbitMQModel("DebtSubscription", "UniverSystem", "univer.student-api.studentNameForDebtByIdGet")
  val amqpConnectionProvider :AmqpConnectionProvider = AmqpLocalConnectionProvider

  val route =
    pathPrefix("debt") {
      concat(
        pathEnd {
          concat(
            get {
              onComplete(DebtRepository.getAllDebt()) {
                case Success(debts) => complete(StatusCodes.OK, debts)
                case Failure(ex) => complete(StatusCodes.InternalServerError, s"Ошибка при получении долгов: ${ex.getMessage}")
              }
            },
            post {
              entity(as[Debt]) { debt =>
                onComplete(DebtRepository.addDebt(debt)) {
                  case Success(newDebtId) =>
                    complete(StatusCodes.Created, s"ID нового долга: $newDebtId")
                  case Failure(ex) =>
                    complete(StatusCodes.InternalServerError, s"Не удалось создать долг: ${ex.getMessage}")
                }
              }
            }
          )
        },
        path(Segment) { debtid =>
          concat(
            get {
              onComplete(DebtRepository.getDebtById(debtid)) {
                case Success(debt) => complete(StatusCodes.OK, debt)
                case Failure(ex) => complete(StatusCodes.InternalServerError, s"Ошибка при получении долга: ${ex.getMessage}")
              }
            },
            put {
              entity(as[DebtUpdate]) { updatedDebt =>
                onComplete(DebtRepository.updateDebt(debtid, updatedDebt)) {
                  case Success(_) => complete(StatusCodes.OK, "Долг успешно обновлен")
                  case Failure(ex) => complete(StatusCodes.InternalServerError, s"Не удалось обновить долг: ${ex.getMessage}")
                }
              }
            },
            delete {
              onComplete(DebtRepository.deleteDebt(debtid)) {
                case Success(_) => complete(StatusCodes.NoContent, "Долг успешно удален")
                case Failure(ex) => complete(StatusCodes.InternalServerError, s"Не удалось удалить долг: ${ex.getMessage}")
              }
            }
          )
        },
        get {
          parameter("param") { param =>
            onComplete(DebtRepository.findDebtByParams(param.toString)) {
              case Success(debts) => complete(StatusCodes.OK, debts)
              case Failure(ex) => complete(StatusCodes.InternalServerError, s"Ошибка при поиске долгов: ${ex.getMessage}")
            }
          }
        }
      )
    } ~
      pathPrefix("debtStudentName") {
        path(Segment) { debtid =>
          concat(
            get {
              val debtFuture: Future[Option[Debt]] = DebtRepository.getDebtById(debtid)

              val resultFuture: Future[Option[Debt]] = debtFuture.flatMap {
                case Some(debt) =>
                  val sendResultFuture = SendMessageAndWaitForResponsAlpakka.sendMessageAndWaitForResponse(debt.userid.toString, pubStudentNameForDebtMQModel, replyStudentNameForDebtMQModel, amqpConnectionProvider)()
                  sendResultFuture.map { result =>

                    println("Результат в виде строки " + result)



                    val updatedDebt = debt.copy(userName = Option(result))

                    Some(updatedDebt)
                  }

              }
              onSuccess(resultFuture) {
                case Some(debt) => complete(debt)
              }

            },
          )
        }
      }
}
