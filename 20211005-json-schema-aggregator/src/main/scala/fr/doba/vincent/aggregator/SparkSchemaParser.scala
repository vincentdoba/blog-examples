package fr.doba.vincent.aggregator

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, ValueNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.JavaConverters._

object SparkSchemaParser {

  private val mapper = {
    val inner = new ObjectMapper()
    inner.registerModule(DefaultScalaModule)
    inner
  }

  def fromJson(json: String): Map[String, String] = addKeys("", mapper.readTree(json), Map.empty[String, String])

  private def addKeys(currentPath: String, jsonNode: JsonNode, accumulator: Map[String, String]): Map[String, String] = {
    if (jsonNode.isObject()) {
      val objectNode = jsonNode.asInstanceOf[ObjectNode]
      val iter = objectNode.fields().asScala.toSeq
      val pathPrefix = if (currentPath.isEmpty()) "" else currentPath + "."
      accumulator ++ iter.map(field => addKeys(pathPrefix + field.getKey, field.getValue, Map())).reduce(_ ++ _)
    } else if (jsonNode.isArray()) {
      val arrayNode = jsonNode.asInstanceOf[ArrayNode]
      if (arrayNode.size() == 0) {
        accumulator ++ Map(currentPath + "[]" -> "STRING")
      } else {
        addKeys(currentPath + "[]", arrayNode.get(0), accumulator)
      }
    } else if (jsonNode.isValueNode()) {
      val valueNode = jsonNode.asInstanceOf[ValueNode]
      accumulator ++ Map(currentPath -> getType(valueNode))
    } else if (jsonNode.isNull) {
      accumulator
    } else {
      throw new IllegalArgumentException(s"unknown node type for node $jsonNode")
    }
  }

  private def getType(valueNode: ValueNode): String = {
    if (valueNode.isInt || valueNode.isLong) {
      "BIGINT"
    } else if (valueNode.isNumber) {
      "DOUBLE"
    } else if (valueNode.isBoolean) {
      "BOOLEAN"
    } else {
      "STRING"
    }
  }

}
