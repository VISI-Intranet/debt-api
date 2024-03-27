package repo

import MongoConnection._
import domain.{Course, CourseUpdate}
import jdk.jshell.SourceCodeAnalysis.Documentation
import org.mongodb.scala.{Document, MongoCollection}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import org.json4s.{DefaultFormats, jackson}
import org.mongodb.scala.Document
import org.bson.types.ObjectId
import org.mongodb.scala.bson.{BsonArray, BsonDocument, BsonInt32, BsonString}
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.Updates.{combine, set}

import java.text.SimpleDateFormat
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

object CourseRepository {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  def findCourseByParams(param: String): Future[List[Course]] = {
    val keyValue = param.split("=")
    if (keyValue.length == 2) {
      val key = keyValue(0)
      val value = keyValue(1)

      val userDocument = if (value.forall(_.isDigit)) {
        Document(key -> value.toInt)
      } else {
        Document(key -> value)
      }

      MongoConnection.courseCollection
        .find(userDocument)
        .toFuture()
        .map { docs =>
          docs.map { doc =>
            Course(
              courseid = doc.getInteger("courseid"),
              name = doc.getString("name"),
              userids = Option(doc.getList("userids", classOf[Integer])).map(_.asScala.map(_.toInt).toList).getOrElse(List.empty),
              enrollmentdate = new SimpleDateFormat("yyyy-MM-dd").parse(doc.getString("enrollmentdate")),
            )
          }.toList
        }
    } else {
      Future.failed(new IllegalArgumentException("Неверный формат параметра"))
    }
  }

  def getAllCourses(): Future[List[Course]] = {
    val futureCourses= MongoConnection.courseCollection.find().toFuture()
    futureCourses.map { docs =>
      Option(docs)
        .map(_.map { doc =>
          Course(
            courseid = doc.getInteger("courseid"),
            name=doc.getString("name"),
            userids=Option(doc.getList("userids", classOf[Integer])).map(_.asScala.map(_.toInt).toList).getOrElse(List.empty),
            enrollmentdate = new SimpleDateFormat("yyyy-MM-dd").parse(doc.getString("enrollmentdate"))
          )
        }.toList)
        .getOrElse(List.empty)
    }
  }

  def getCourseById(courseid: String): Future[Option[Course]] = {
    val filter = Document("courseid" -> courseid.toInt)
    MongoConnection.courseCollection.find(filter).headOption().map {
      case Some(doc) =>
        Some(
          Course(
            courseid = doc.getInteger("courseid"),
            name=doc.getString("name"),
            userids=Option(doc.getList("userids", classOf[Integer])).map(_.asScala.map(_.toInt).toList).getOrElse(List.empty),
            enrollmentdate = new SimpleDateFormat("yyyy-MM-dd").parse(doc.getString("enrollmentdate"))
          )
        )
      case None => None
    }
  }
  def addCourse(course: Course): Future[String] = {
      val courseDocument = BsonDocument(
        "courseid" -> BsonInt32(course.courseid),
        "name" -> BsonString(course.name),
        "userids" -> BsonArray(course.userids.map(BsonInt32(_))),
        "enrollmentdate" -> new SimpleDateFormat("yyyy-MM-dd").format(course.enrollmentdate)
      )
      MongoConnection.courseCollection.insertOne(courseDocument).toFuture().map(_ => "Course успешно добавлен")
    }


  def deleteCourse(courseid: String): Future[String] = {
    val filter = Document("courseid" -> courseid.toInt)
    MongoConnection.courseCollection.deleteOne(filter).toFuture().map {
      case _ => "Course успешно удален"
    }
  }

  def updateCourse(courseid: String, updatedCourse: CourseUpdate): Future[String] = {
    val filter = Document("courseid" -> courseid.toInt)
    updatedCourse.toDocument(courseid).flatMap { updatedBson =>
      MongoConnection.courseCollection
        .updateOne(filter, updatedBson)
        .toFuture()
        .map { updateResult =>
          if (updateResult.wasAcknowledged() && updateResult.getModifiedCount > 0) {
            "Course успешно обновлен"
          } else {
            "Обновление Course не выполнено"
          }
        }
    }
  }

  implicit class RichCourseUpdate(courseUpdate: CourseUpdate) {
    def toDocument(courseid: String)(implicit ec: ExecutionContext): Future[BsonDocument] = {
      val oldCourseFuture: Future[Option[Course]] = getCourseById(courseid)
      val updatedDocumentFuture: Future[BsonDocument] = oldCourseFuture.flatMap {
        case Some(oldCourse) =>
          val updatedDocument = oldCourse.toDocumentForUpdate(courseUpdate)
          Future.successful(updatedDocument)
        case None =>
          Future.failed(new NoSuchElementException(s"Course with id $courseid not found"))
      }
      updatedDocumentFuture
    }
  }

  implicit class RichCourse(course: Course) {
    def toDocumentForUpdate(update: CourseUpdate): BsonDocument = {
      val document = BsonDocument("$set" -> BsonDocument(
        "courseid" -> BsonInt32(update.courseid.getOrElse(course.courseid)),
        "name" -> BsonString(update.name.getOrElse(course.name)),
        "userids" -> BsonArray(update.userids.getOrElse(course.userids).map(BsonInt32(_))),
        "enrollmentdate" -> new SimpleDateFormat("yyyy-MM-dd").format(update.enrollmentdate.getOrElse(course.enrollmentdate))
      ))
      document
    }
  }


}

