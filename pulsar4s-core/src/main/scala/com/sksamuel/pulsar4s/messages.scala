package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api.{MessageBuilder, Schema, Message => JMessage}
import org.apache.pulsar.client.impl.MessageIdImpl

import scala.collection.JavaConverters._
import scala.language.implicitConversions

case class Message[T](key: Option[String],
                      value: T,
                      data: Array[Byte],
                      properties: Map[String, String],
                      messageId: Option[MessageId],
                      sequenceId: Long,
                      producerName: String,
                      publishTime: Long,
                      eventTime: Long)

object Message {

  implicit def fromJava[T](message: JMessage[T]): Message[T] = {
    Message(
      Option(message.getKey),
      message.getValue,
      message.getData,
      message.getProperties.asScala.toMap,
      Option(MessageId(message.getMessageId)),
      message.getSequenceId,
      message.getProducerName,
      message.getPublishTime,
      message.getEventTime
    )
  }

  implicit def toJava[T](message: Message[T])(implicit schema: Schema[T]): JMessage[T] = {
    val builder = MessageBuilder.create(schema)
      .setContent(message.data)
      .setValue(message.value)
    message.key.foreach(builder.setKey)
    message.properties.foreach { case (k, v) => builder.setProperty(k, v) }
    builder.setEventTime(message.eventTime)
    builder.build()
  }
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