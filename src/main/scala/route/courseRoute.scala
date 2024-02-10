package route

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{pathEnd, _}
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.{DefaultFormats, jackson}
import repo.CourseRepository
import domain.{Course, CourseUpdate}
import domain.JsonFormats.formats

import scala.util.{Failure, Success}

object CourseRoute extends Json4sSupport {
  implicit val serialization = jackson.Serialization

  val route =
    pathPrefix("course") {
      concat(
        get {
          parameter("param") { param =>
            onComplete(CourseRepository.findCourseByParams(param)) {
              case Success(users) =>
                if (users.nonEmpty) {
                  complete(users)
                } else {
                  complete(StatusCodes.NotFound, s"Пользователь не найден")
                }
              case Failure(ex: IllegalArgumentException) =>
                complete(StatusCodes.BadRequest, s" ${ex.getMessage}")
              case Failure(_) =>
                complete(StatusCodes.InternalServerError, s"Внутренняя ошибка сервера")
            }
          }
        },
        pathEnd {
          concat(
            get {
              onComplete(CourseRepository.getAllCourses()) {
                case Success(courses) => complete(StatusCodes.OK, courses)
                case Failure(ex) => complete(StatusCodes.InternalServerError, s"Ошибка при получении курсов: ${ex.getMessage}")
              }
            },
            post {
              entity(as[Course]) { course =>
                onComplete(CourseRepository.addCourse(course)) {
                  case Success(newCourseId) =>
                    complete(StatusCodes.Created, s"ID нового курса: $newCourseId")
                  case Failure(ex) =>
                    complete(StatusCodes.InternalServerError, s"Не удалось создать курс: ${ex.getMessage}")
                }
              }
            }
          )
        },
        path(Segment) { courseid =>
          concat(
            get {
              onComplete(CourseRepository.getCourseById(courseid)) {
                case Success(course) => complete(StatusCodes.OK, course)
                case Failure(ex) => complete(StatusCodes.InternalServerError, s"Ошибка при получении курса: ${ex.getMessage}")
              }
            },
            put {
              entity(as[CourseUpdate]) { updatedCourse =>
                onComplete(CourseRepository.updateCourse(courseid, updatedCourse)) {
                  case Success(_) => complete(StatusCodes.OK, "Курс успешно обновлен")
                  case Failure(ex) => complete(StatusCodes.InternalServerError, s"Не удалось обновить курс: ${ex.getMessage}")
                }
              }
            },
            delete {
              onComplete(CourseRepository.deleteCourse(courseid)) {
                case Success(_) => complete(StatusCodes.NoContent, "Курс успешно удален")
                case Failure(ex) => complete(StatusCodes.InternalServerError, s"Не удалось удалить курс: ${ex.getMessage}")
              }
            }
          )
        }
      )
    }
}
