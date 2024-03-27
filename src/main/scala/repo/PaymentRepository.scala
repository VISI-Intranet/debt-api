package repo

import MongoConnection._
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import domain.{Debt, Debtstatus, Payment, PaymentUpdate, Paymentstatus, Paymenttype}
import org.mongodb.scala.Document
import org.mongodb.scala.bson.{BsonArray, BsonDateTime, BsonDocument, BsonDouble, BsonInt32, BsonString}

import scala.concurrent.{ExecutionContext, Future}
import org.bson.types.ObjectId
import org.mongodb.scala.model.Updates.{combine, pullAll, set}

import java.text.SimpleDateFormat


object PaymentRepository {

  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global


  def findPaymentByParams(param: String): Future[List[Payment]] = {
    val keyValue = param.split("=")
    if (keyValue.length == 2) {
      val key = keyValue(0)
      val value = keyValue(1)

      val paymentDocument = if (value.forall(_.isDigit)) {
        Document(key -> value.toInt)
      } else {
        Document(key -> value)
      }

      MongoConnection.paymentCollection
        .find(paymentDocument)
        .toFuture()
        .map { docs =>
          docs.map { doc =>
            Payment(
              paymentid = doc.getInteger("paymentid"),
              amount = doc.getDouble("amount"),
              userid = doc.getInteger("userid"),
              courseid = doc.getInteger("courseid"),
              paymentdate = new SimpleDateFormat("yyyy-MM-dd").parse(doc.getString("paymentdate")),
              paymentstatus = Paymentstatus.withName(doc.getString("paymentstatus")),
              paymenttype = Paymenttype.withName(doc.getString("paymenttype"))
            )
          }.toList
        }
    } else {
      Future.failed(new IllegalArgumentException("Неверный формат параметра"))
    }
  }


  def getAllPayment(): Future[List[Payment]] = {
    val futurePayment = MongoConnection.paymentCollection.find().toFuture()
    futurePayment.map { docs =>
      Option(docs)
        .map(_.map { doc =>
          Payment(
            paymentid = doc.getInteger("paymentid"),
            amount = doc.getDouble("amount"),
            userid = doc.getInteger("userid"),
            courseid = doc.getInteger("courseid"),
            paymentdate = new SimpleDateFormat("yyyy-MM-dd").parse(doc.getString("paymentdate")),
            paymentstatus = Paymentstatus.withName(doc.getString("paymentstatus")),
            paymenttype = Paymenttype.withName(doc.getString("paymenttype"))
          )
        }.toList)
        .getOrElse(List.empty)
    }
  }

  def getPaymentById(paymentid: String): Future[Option[Payment]] = {
    val filter = Document("paymentid" -> paymentid.toInt)
    MongoConnection.paymentCollection.find(filter).headOption().map {
      case Some(doc) =>
        Some(
          Payment(
            paymentid = doc.getInteger("paymentid"),
            amount = doc.getDouble("amount"),
            userid = doc.getInteger("userid"),
            courseid = doc.getInteger("courseid"),
            paymentdate = new SimpleDateFormat("yyyy-MM-dd").parse(doc.getString("paymentdate")),
            paymentstatus = Paymentstatus.withName(doc.getString("paymentstatus")),
            paymenttype = Paymenttype.withName(doc.getString("paymenttype"))
          )
        )
      case None => None
    }
  }

  def addPayment(payment: Payment): Future[String] = {
    val paymentDocument = BsonDocument(
      "paymentid" -> BsonInt32(payment.paymentid),
      "amount" -> BsonDouble(payment.amount),
      "userid" -> BsonInt32(payment.userid),
      "courseid" -> BsonInt32(payment.courseid),
      "paymentdate" -> new SimpleDateFormat("yyyy-MM-dd").format(payment.paymentdate),
      "paymentstatus" -> BsonString(payment.paymentstatus.toString),
      "paymenttype" -> BsonString(payment.paymenttype.toString)
    )
    MongoConnection.paymentCollection.insertOne(paymentDocument).toFuture().map(_ => "Платеж успешно добавлен")
  }


  def deletePayment(paymentid: String): Future[String] = {
    val filter = Document("paymentid" -> paymentid.toInt)
    MongoConnection.paymentCollection.deleteOne(filter).toFuture().map {
      case _ => "Payment успешно удален"
    }
  }

  def updatePayment(paymentid: String, updatedPayment: PaymentUpdate): Future[String] = {
    val filter = Document("paymentid" -> paymentid.toInt)
    updatedPayment.toDocument(paymentid).flatMap{updatedBson=>
    MongoConnection.paymentCollection
      .updateOne(filter, updatedBson)
      .toFuture()
      .map { updateResult =>
        if (updateResult.wasAcknowledged() && updateResult.getModifiedCount > 0) {
          "Payment успешно обновлен"
        } else {
          "Обновление Payment не выполнено"
        }
      }
  }
  }
  implicit class RichPaymentUpdate(paymentUpdate: PaymentUpdate) {
    def toDocument(paymentid: String)(implicit ec: ExecutionContext): Future[BsonDocument] = {
      val oldPaymentFuture: Future[Option[Payment]] = getPaymentById(paymentid)
      val updatedDocumentFuture: Future[BsonDocument] = oldPaymentFuture.flatMap {
        case Some(oldPayment) =>
          val updatedDocument = oldPayment.toDocumentForUpdate(paymentUpdate)
          Future.successful(updatedDocument)
        case None =>
          Future.failed(new NoSuchElementException(s"Payment with id $paymentid not found"))
      }

      updatedDocumentFuture
    }
  }

  implicit class RichPayment(payment: Payment) {
    def toDocumentForUpdate(update: PaymentUpdate): BsonDocument = {
      val document = BsonDocument("$set" -> BsonDocument(
        "paymentid" -> BsonInt32(update.paymentid.getOrElse(payment.paymentid)),
        "amount" -> BsonDouble(update.amount.getOrElse(payment.amount)),
        "userid" -> BsonInt32(update.userid.getOrElse(payment.userid)),
        "courseid" -> BsonInt32(update.courseid.getOrElse(payment.courseid)),
        "paymentdate" -> new SimpleDateFormat("yyyy-MM-dd").format(update.paymentdate.getOrElse(payment.paymentdate)),
        "paymentstatus" -> BsonString(update.paymentstatus.map(_.toString).getOrElse(payment.paymentstatus.toString)),
        "paymenttype" -> BsonString(update.paymenttype.map(_.toString).getOrElse(payment.paymenttype.toString))
      ))
      document
    }
  }

}