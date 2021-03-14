package fr.doba.vincent

import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.Location
import org.flywaydb.core.api.configuration.ClassicConfiguration
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.utility.DockerImageName

import java.sql.{Connection, DriverManager, Statement}

class PostgresDatabaseWithFlywayTest extends AnyFunSuite with ForAllTestContainer with BeforeAndAfterAll {

  override val container: PostgreSQLContainer = new PostgreSQLContainer(
    databaseName = Some("my_database"),
    pgUsername = Some("my_user"),
    pgPassword = Some("my_password"),
    dockerImageNameOverride = Some(DockerImageName.parse("postgres:12"))
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
    val configuration = new ClassicConfiguration()
    configuration.setDataSource(container.container.getJdbcUrl, container.container.getUsername, container.container.getPassword)
    configuration.setLocations(new Location("classpath:db/migration"))
    val flyway = new Flyway(configuration)
    flyway.migrate()
  }

  test("should load database") {

    val connection: Connection = DriverManager.getConnection(container.container.getJdbcUrl, container.container.getUsername, container.container.getPassword)
    val statement: Statement = connection.createStatement

    val result = statement.executeQuery("SELECT value FROM my_table")
    while (result.next) {
      val value: String = result.getString("value")
      System.out.println(value)
    }


  }

}
