package Alpakka.Handlers.AddHandler

import Alpakka.Operations.SendMessageWithCorrelationIdAlpakka
import Alpakka.RabbitMQModel.RabbitMQModel
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.amqp.{AmqpConnectionProvider, AmqpLocalConnectionProvider}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object RecieveHandler {

  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  implicit lazy val system: ActorSystem = ActorSystem("web-system")
  implicit lazy val mat: Materializer = Materializer(system)

  val handler: (String, String) => Unit = (message, routingKey) => {

    routingKey match {
      case "univer.student-api.disciplinesForStudentByIdGet" =>

      case "key2" =>
        // Обработка для ключа "key2"
        println(s"Received message for key2: $message")
      case _ =>
        // Обработка для всех остальных случаев
        println(s"Received message with unknown routing key: $routingKey")
    }

  }

}
