package scala

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.{DefaultFormats, jackson}
import akka.http.scaladsl.server.Directives._
import repo.DebtRepository

import scala.concurrent.{ExecutionContextExecutor, Future}
import route._

import scala.io.StdIn;

object Main extends Json4sSupport {

  implicit val system: ActorSystem = ActorSystem("web-service")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val serialization = jackson.Serialization
  implicit val formats = DefaultFormats

  def main(args: Array[String]): Unit = {
    val allRoutes =
      CourseRoute.route ~
      UserRoute.route ~
      DebtRoute.route ~
      PaymentRoute.route;

    val bindingFuture = Http().bindAndHandle(allRoutes, "localhost", 8080)

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine()

    bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => system.terminate())
  }

}