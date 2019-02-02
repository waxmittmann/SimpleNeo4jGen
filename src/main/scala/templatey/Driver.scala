package templatey

import java.util

import org.neo4j.driver.v1._
import templatey.Main.graph

class WrappedDriver(url: String, user: String, password: String) {

  private val token: AuthToken = AuthTokens.basic(user, password)

  val driver: Driver = GraphDatabase.driver(
    url,
    token,
    Config.build.withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig
  )


  def write(script: String, params: util.Map[String, Object]): Either[String, StatementResult] = {
    val session = graph.driver.session()
    try {
      session.writeTransaction {
        tx =>
          val result = tx.run(script, params)
          tx.success()
          Right(result)

//          if (result.hasNext())
//            println(result.next().asMap())
//          else
//            println("Failed: Produced no result")
      }
    } catch {
      case e => Left(e.toString)
    } finally {
      session.close()
    }

  }

  def close(): Unit = {
    driver.close()
  }
}
