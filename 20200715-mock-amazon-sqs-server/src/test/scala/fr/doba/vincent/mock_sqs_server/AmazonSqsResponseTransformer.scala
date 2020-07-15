package fr.doba.vincent.mock_sqs_server

import java.net.URLDecoder
import java.security.MessageDigest
import java.util.UUID

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.common.FileSource
import com.github.tomakehurst.wiremock.extension.{Parameters, ResponseDefinitionTransformer}
import com.github.tomakehurst.wiremock.http.{Request, ResponseDefinition}
import javax.xml.bind.DatatypeConverter

object AmazonSqsResponseTransformer extends ResponseDefinitionTransformer {

  private val hashEngine: MessageDigest = MessageDigest.getInstance("MD5")

  override def getName: String = "amazon-sqs-response-transformer"

  override def applyGlobally(): Boolean = false

  override def transform(request: Request, responseDefinition: ResponseDefinition, files: FileSource, parameters: Parameters): ResponseDefinition = {
    new ResponseDefinitionBuilder()
      .withHeader("Content-Type", "application/xml")
      .withStatus(200)
      .withBody(buildBody(request.getBodyAsString))
      .build()
  }

  private def buildBody(requestBody: String): String = {
    val bodyElements = extractBodyElements(requestBody)
    val hashedBody = computeMd5OfMessageBody(bodyElements("MessageBody"))

    s"""
      |<SendMessageResponse>
      |  <SendMessageResult>
      |    <MD5OfMessageBody>$hashedBody</MD5OfMessageBody>
      |    <MessageId>${UUID.randomUUID()}</MessageId>
      |  </SendMessageResult>
      |  <ResponseMetadata>
      |    <RequestId>${UUID.randomUUID()}</RequestId>
      |  </ResponseMetadata>
      |</SendMessageResponse>
      |""".stripMargin
  }

  private def extractBodyElements(requestBody: String): Map[String, String] = requestBody
    .split("&")
    .filter(_.startsWith("Message"))
    .map(splitKeyValue)
    .toMap

  private def splitKeyValue(input: String): (String, String) = {
    val split = input.split("=")
    (split(0), split(1))
  }

  private def computeMd5OfMessageBody(messageBody: String): String = {
    hashEngine.update(URLDecoder.decode(messageBody, "UTF-8").getBytes)
    val hashedBody = DatatypeConverter.printHexBinary(hashEngine.digest)
    hashEngine.reset()
    hashedBody.toLowerCase
  }

}
