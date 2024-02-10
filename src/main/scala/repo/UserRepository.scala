package repo

import MongoConnection._
import domain.{User, UserUpdate, Usertype}
import org.mongodb.scala.Document
import org.mongodb.scala.bson.{BsonArray, BsonDocument, BsonInt32, BsonString}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import org.json4s.{DefaultFormats, jackson}
import org.mongodb.scala.Document
import org.bson.types.ObjectId
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.Updates.{combine, set}

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.Try

object UserRepository {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  def findUserByParams(param: String): Future[List[User]] = {
    val keyValue = param.split("=")
    if (keyValue.length == 2) {
      val key = keyValue(0)
      val value = keyValue(1)

      val userDocument = if (value.forall(_.isDigit)) {
        Document(key -> value.toInt)
      } else {
        Document(key -> value)
      }

      MongoConnection.userCollection
        .find(userDocument)
        .toFuture()
        .map { docs =>
          docs.map { doc =>
            User(
              userid = doc.getInteger("userid"),
              name = doc.getString("name"),
              lastname = doc.getString("lastname"),
              usertype = Usertype.withName(doc.getString("usertype"))
            )
          }.toList
        }
    } else {
      Future.failed(new IllegalArgumentException("Неверный формат параметра"))
    }
  }
  def doesUserExist(userid: String)(implicit ec: ExecutionContext): Boolean = {
    Await.result(
    MongoConnection.userCollection.find(Document("userid" -> userid.toInt)).headOption().map(_.isDefined),
      2.seconds
    )
  }
  def getAllUsers(): Future[List[User]] = {
    val futureUsers = MongoConnection.userCollection.find().toFuture()
    futureUsers.map { docs =>
      Option(docs)
        .map(_.map { doc =>
          User(
            userid = doc.getInteger("userid"),
            name=doc.getString("name"),
            lastname=doc.getString("lastname"),
            usertype=Usertype.withName(doc.getString("usertype"))
          )
        }.toList)
        .getOrElse(List.empty)
    }
  }

  def getUserById(userid: String): Future[Option[User]] = {
    val filter = Document("userid" -> userid.toInt)
    MongoConnection.userCollection.find(filter).headOption().map {
      case Some(doc) =>
        Some(
          User(
            userid = doc.getInteger("userid"),
            name = doc.getString("name"),
            lastname = doc.getString("lastname"),
            usertype = Usertype.withName(doc.getString("usertype"))
          )
        )
      case None => None
    }
  }

  def addUser(user: User): Future[String] = {
    val userDocument = BsonDocument(
      "userid" -> BsonInt32(user.userid),
      "name" -> BsonString(user.name),
      "lastname" -> BsonString(user.lastname),
      "usertype" -> BsonString(user.usertype.toString)
    )
    MongoConnection.userCollection.insertOne(userDocument).toFuture().map(_ => "Пользователь успешно добавлен")
  }

  def deleteUser(userid: String): Future[String] = {
    val filter = Document("userid" -> userid.toInt)
    MongoConnection.userCollection.deleteOne(filter).toFuture().map {
      case _ => "Пользователь успешно удален"
    }
  }

  def updateUser(userid: String, updatedUser: UserUpdate): Future[String] = {
    val filter = Document("userid" -> userid.toInt)
    updatedUser.toDocument(userid).flatMap{updatedBson=>
      
    MongoConnection.userCollection
      .updateOne(filter, updatedBson)
      .toFuture()
      .map { updateResult =>
        if (updateResult.wasAcknowledged() && updateResult.getModifiedCount > 0) {
          "Пользователь успешно обновлен"
        } else {
          "Обновление пользователя не выполнено"
        }
      }
    }
  }


  // To convert UserUpdate to BsonDocument for update
  implicit class RichUserUpdate(userUpdate: UserUpdate) {
    def toDocument(userid: String)(implicit ec: ExecutionContext): Future[BsonDocument] = {
      val oldUserFuture: Future[Option[User]] = getUserById(userid)
      val updatedDocumentFuture: Future[BsonDocument] = oldUserFuture.flatMap {
        case Some(oldUser) =>
          val updatedDocument = oldUser.toDocumentForUpdate(userUpdate)
          Future.successful(updatedDocument)
        case None =>
          Future.failed(new NoSuchElementException(s"User with id $userid not found"))
      }

      updatedDocumentFuture
    }
  }

  // To convert User to BsonDocument for update
  implicit class RichUser(user: User) {
    def toDocumentForUpdate(update: UserUpdate): BsonDocument = {
      val document = BsonDocument("$set" -> BsonDocument(
        "userid"->BsonInt32(update.userid.getOrElse(user.userid)),
        "name" -> BsonString(update.name.getOrElse(user.name)),
        "lastname"-> BsonString(update.name.getOrElse(user.lastname)),
        "usertype" -> BsonString(update.usertype.map(_.toString).getOrElse(user.usertype.toString))
      ))
      document
    }
  }
}

