package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.{MessageBuilder, Message => JMessage}
import org.apache.pulsar.client.impl.MessageIdImpl

import scala.collection.JavaConverters._
import scala.language.implicitConversions

case class Message(key: Option[String],
                   data: Array[Byte],
                   properties: Map[String, String],
                   messageId: Option[MessageId],
                   publishTime: Long,
                   eventTime: Long)

object Message {

  def apply(data: Array[Byte]): Message = Message(None, data, Map.empty, None, 0, System.currentTimeMillis())

  implicit def fromJava(message: JMessage): Message = {
    Message(
      Option(message.getKey),
      message.getData,
      message.getProperties.asScala.toMap,
      Option(MessageId(message.getMessageId)),
      message.getPublishTime,
      message.getEventTime
    )
  }

  implicit def toJava(message: Message): JMessage = {
    val builder = MessageBuilder.create()
      .setContent(message.data)
    message.key.foreach(builder.setKey)
    message.properties.foreach { case (k, v) => builder.setProperty(k, v) }
    builder.setEventTime(message.eventTime)
    builder.build()
  }
}

trait MessageWriter[T] {
  def write(t: T): Either[Throwable, Message]
}

trait MessageReader[T] {
  def read(msg: Message): Either[Throwable, T]
}

case class MessageId(bytes: Array[Byte])

object MessageId {

  implicit def toJava(messageId: MessageId): org.apache.pulsar.client.api.MessageId = {
    MessageIdImpl.fromByteArray(messageId.bytes)
  }

  val earliest: MessageId = org.apache.pulsar.client.api.MessageId.earliest
  val latest: MessageId = org.apache.pulsar.client.api.MessageId.latest

  implicit def apply(messageId: org.apache.pulsar.client.api.MessageId): MessageId = MessageId(messageId.toByteArray)
}