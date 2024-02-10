package repo

import MongoConnection._
import domain.{Course, Debt, DebtUpdate, Debtstatus}
import org.mongodb.scala.Document
import org.mongodb.scala.bson.{BsonArray, BsonDateTime, BsonDocument, BsonDouble, BsonInt32, BsonString}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import org.json4s.{DefaultFormats, jackson}
import org.mongodb.scala.Document
import org.bson.types.ObjectId
import org.mongodb.scala.model.Updates.{combine, set}

import java.text.SimpleDateFormat

object DebtRepository {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  def findDebtByParams(param: String): Future[List[Debt]] = {
    val keyValue = param.split("=")
    if (keyValue.length == 2) {
      val key = keyValue(0)
      val value = keyValue(1)

      val debtDocument = if (value.forall(_.isDigit)) {
        Document(key -> value.toInt)
      } else {
        Document(key -> value)
      }

      MongoConnection.debtCollection
        .find(debtDocument)
        .toFuture()
        .map { docs =>
          docs.map { doc =>
            Debt(
              debtid = doc.getInteger("debtid"),
              amount=doc.getDouble("amount"),
              courseid = doc.getInteger("courseid"),
              userid = doc.getInteger("userid"),
              debtdate = new SimpleDateFormat("yyyy-MM-dd").parse(doc.getString("debtdate")),
              debtstatus = Debtstatus.withName(doc.getString("debtstatus"))
            )
          }.toList
        }
    } else {
      Future.failed(new IllegalArgumentException("Неверный формат параметра"))
    }
  }

  def getAllDebt(): Future[List[Debt]] = {
    val futureDebt= MongoConnection.debtCollection.find().toFuture()
    futureDebt.map { docs =>
      Option(docs)
        .map(_.map { doc =>
          Debt(
            debtid = doc.getInteger("debtid"),
            amount=doc.getDouble("amount"),
            userid=doc.getInteger("userid"),
            courseid = doc.getInteger("courseid"),
            debtdate = new SimpleDateFormat("yyyy-MM-dd").parse(doc.getString("debtdate")),
            debtstatus =Debtstatus.withName(doc.getString("debtstatus"))
          )
        }.toList)
        .getOrElse(List.empty)
    }
  }

  def getDebtById(debtid: String): Future[Option[Debt]] = {
    val filter = Document("debtid" -> debtid.toInt)
    MongoConnection.debtCollection.find(filter).headOption().map {
      case Some(doc) =>
        Some(
          Debt(
            debtid = doc.getInteger("debtid"),
            amount = doc.getDouble("amount"),
            userid = doc.getInteger("userid"),
            courseid = doc.getInteger("courseid"),
            debtdate = new SimpleDateFormat("yyyy-MM-dd").parse(doc.getString("debtdate")),
            debtstatus =Debtstatus.withName(doc.getString("debtstatus"))
          )
        )
      case None => None
    }
  }

  def addDebt(debt: Debt): Future[String] = {
    val debtDocument = BsonDocument(
      "debtid" -> BsonInt32(debt.debtid),
      "amount" -> BsonDouble(debt.amount),
      "userid" -> BsonInt32(debt.userid),
      "courseid" -> BsonInt32(debt.courseid),
      "debtdate"->new SimpleDateFormat("yyyy-MM-dd").format(debt.debtdate),
      "debtstatus"->BsonString(debt.debtstatus.toString)
    )
    MongoConnection.debtCollection.insertOne(debtDocument).toFuture().map(_ => "Debt успешно добавлен")
  }

  def deleteDebt(debtid: String): Future[String] = {
    val filter = Document("debtid" -> debtid.toInt)
    MongoConnection.debtCollection.deleteOne(filter).toFuture().map {
      case _ => "Debt успешно удален"
    }
  }

  def updateDebt(debtid: String, updatedDebt: DebtUpdate): Future[String] = {
    val filter = Document("debtid" -> debtid.toInt)
    updatedDebt.toDocument(debtid).flatMap { updatedBson =>

      MongoConnection.debtCollection
        .updateOne(filter, updatedBson)
        .toFuture()
        .map { updateResult =>
          if (updateResult.wasAcknowledged() && updateResult.getModifiedCount > 0) {
            "Debt успешно обновлен"
          } else {
            "Обновление Debt не выполнено"
          }
        }
    }
  }

  implicit class RichDebtUpdate(debtUpdate: DebtUpdate) {
    def toDocument(debtid: String)(implicit ec: ExecutionContext): Future[BsonDocument] = {
      val oldDebtFuture: Future[Option[Debt]] = getDebtById(debtid)
      val updatedDocumentFuture: Future[BsonDocument] = oldDebtFuture.flatMap {
        case Some(oldDebt) =>
          val updatedDocument = oldDebt.toDocumentForUpdate(debtUpdate)
          Future.successful(updatedDocument)
        case None =>
          Future.failed(new NoSuchElementException(s"Debt with id $debtid not found"))
      }

      updatedDocumentFuture
    }
  }

  implicit class RichDebt(debt: Debt) {
    def toDocumentForUpdate(update: DebtUpdate): BsonDocument = {
      val document = BsonDocument("$set" -> BsonDocument(
        "debtid" -> BsonInt32(update.debtid.getOrElse(debt.debtid)),
        "amount" -> BsonDouble(update.amount.getOrElse(debt.amount)),
        "userid" -> BsonInt32(update.userid.getOrElse(debt.userid)),
        "courseid" -> BsonInt32(update.courseid.getOrElse(debt.courseid)),
        "debtdate" -> new SimpleDateFormat("yyyy-MM-dd").format(update.debtdate.getOrElse(debt.debtdate)),
        "debtstatus" -> BsonString(update.debtstatus.map(_.toString).getOrElse(debt.debtstatus.toString))
      ))
      document
    }
  }

}

