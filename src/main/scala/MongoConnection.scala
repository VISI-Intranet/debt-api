package MongoConnection
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}

object MongoConnection {
  private val mongoClient = MongoClient("mongodb://localhost:27017")
  val database: MongoDatabase = mongoClient.getDatabase("student")
  val userCollection: MongoCollection[Document] = database.getCollection("user")
  val debtCollection: MongoCollection[Document] = database.getCollection("debt")
  val courseCollection: MongoCollection[Document] = database.getCollection("course")
  val paymentCollection: MongoCollection[Document] = database.getCollection("payment")
  val stuCourseCollection: MongoCollection[Document] = database.getCollection("stuCourse")
}
