package route

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.{DefaultFormats, jackson}
import repo.UserRepository
import domain.{JsonFormatsss, User, UserUpdate}
import scala.util.{Success, Failure}



object UserRoute extends Json4sSupport {
  implicit val serialization = jackson.Serialization
  implicit val formats = JsonFormatsss.formats

  val route =
    pathPrefix("user") {
      concat(
        pathEnd {
          concat(
            get {
              onComplete(UserRepository.getAllUsers()) {
                case Success(users) => complete(StatusCodes.OK, users)
                case Failure(ex) => complete(StatusCodes.InternalServerError, s"Ошибка при получении пользователей: ${ex.getMessage}")
              }
            },
            post {
              entity(as[User]) { user =>
                onComplete(UserRepository.addUser(user)) {
                  case Success(newUserId) =>
                    complete(StatusCodes.Created, s"ID нового пользователя: $newUserId")
                  case Failure(ex) =>
                    complete(StatusCodes.InternalServerError, s"Не удалось создать пользователя: ${ex.getMessage}")
                }
              }
            }
          )
        },
        path(Segment) { userid =>
          concat(
            get {
              onComplete(UserRepository.getUserById(userid)) {
                case Success(Some(user)) => complete(StatusCodes.OK, user)
                case Success(None) => complete(StatusCodes.NotFound, "Пользователь не найден")
                case Failure(ex) => complete(StatusCodes.InternalServerError, s"Ошибка при получении пользователя: ${ex.getMessage}")
              }
            },
            put {
              entity(as[UserUpdate]) { updatedUser =>
                onComplete(UserRepository.updateUser(userid, updatedUser)) {
                  case Success(_) => complete(StatusCodes.OK, "Пользователь успешно обновлен")
                  case Failure(ex) => complete(StatusCodes.InternalServerError, s"Не удалось обновить пользователя: ${ex.getMessage}")
                }
              }
            },
            delete {
              onComplete(UserRepository.deleteUser(userid)) {
                case Success(_) => complete(StatusCodes.NoContent, "Пользователь успешно удален")
                case Failure(ex) => complete(StatusCodes.InternalServerError, s"Не удалось удалить пользователя: ${ex.getMessage}")
              }
            }
          )
        },
        get {
          parameter("param") { param =>
            onComplete(UserRepository.findUserByParams(param)) {
              case Success(users) => complete(StatusCodes.OK, users)
              case Failure(ex) => complete(StatusCodes.InternalServerError, s"Ошибка при поиске пользователей: ${ex.getMessage}")
            }
          }
        }
      )
    }}