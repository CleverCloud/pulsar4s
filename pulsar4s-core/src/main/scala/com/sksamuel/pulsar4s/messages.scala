package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.{MessageBuilder, Message => JMessage}
import org.apache.pulsar.client.impl.MessageIdImpl

import scala.collection.JavaConverters._
import scala.language.implicitConversions

trait SMessage {
  def data: Array[Byte]
  def key: Option[String]
  def properties: Map[String, String]
  def messageId: Option[MessageId]
  def publishTime: Long
  def eventTime: Long
}

object SMessage {

  def fromJava(message: JMessage): SMessage = {
    DefaultMessage(
      Option(message.getKey),
      message.getData,
      message.getProperties.asScala.toMap,
      Option(MessageId(message.getMessageId)),
      message.getPublishTime,
      message.getEventTime
    )
  }

  def toJava(message: SMessage): JMessage = {
    val builder = MessageBuilder.create()
      .setContent(message.data)
    message.key.foreach(builder.setKey)
    message.properties.foreach { case (k, v) => builder.setProperty(k, v) }
    builder.setEventTime(message.eventTime)
    builder.build()
  }
}

case class DefaultMessage(key: Option[String],
                          data: Array[Byte],
                          properties: Map[String, String],
                          messageId: Option[MessageId],
                          publishTime: Long,
                          eventTime: Long) extends SMessage

trait MessageWriter[T] {
  def write(t: T): SMessage
}

trait MessageReader[T] {
  def read(msg: SMessage): T
}

case class MessageId(bytes: Array[Byte])

object MessageId {

  implicit def toJava(messageId: MessageId): org.apache.pulsar.client.api.MessageId = {
    MessageIdImpl.fromByteArray(messageId.bytes)
  }

  implicit def apply(messageId: org.apache.pulsar.client.api.MessageId): MessageId = MessageId(messageId.toByteArray)
}