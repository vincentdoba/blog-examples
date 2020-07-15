package fr.doba.vincent.mock_sqs_server

import java.util.UUID

import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.github.tomakehurst.wiremock.WireMockServer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.github.tomakehurst.wiremock.client.WireMock._

import scala.util.Random

class MockAmazonSqsServerTest extends AnyFunSuite with BeforeAndAfterAll {

  private val port: Int = Random.nextInt(10000) + 55534

  private val server: WireMockServer = {
    val server = new WireMockServer(wireMockConfig().port(port).extensions(AmazonSqsResponseTransformer))
    server.stubFor(post(urlEqualTo("/amazonSqsEndpoint"))
        .willReturn(aResponse().withTransformers(AmazonSqsResponseTransformer.getName))
    )
    server.start()
    server
  }

  test("should not raise error while sending message to Amazon SQS queue") {
    // Given
    System.setProperty("aws.accessKeyId", "FakeAWSAccessKeyId")
    System.setProperty("aws.secretKey", "FakeAWSSecretKey")
    val url = s"http://localhost:$port/amazonSqsEndpoint"
    val message = UUID.randomUUID().toString

    // When
    AmazonSQSClientBuilder
      .standard()
      .withRegion("eu-west-2")
      .build()
      .sendMessage(url, message)

    // Then
    // No error raised
  }

  override def afterAll(): Unit = {
    server.stop()
    super.afterAll()
  }

}
