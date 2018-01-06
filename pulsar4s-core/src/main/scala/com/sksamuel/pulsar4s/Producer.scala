package com.sksamuel.pulsar4s

import java.io.Closeable

import org.apache.pulsar.client.api.{MessageBuilder, Message => JMessage}
import org.apache.pulsar.client.impl.{MessageIdImpl, ProducerStats}

import scala.collection.JavaConverters._
import scala.concurrent.Future
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

case class MessageId(bytes: Array[Byte])

object MessageId {

  implicit def toJava(messageId: MessageId): org.apache.pulsar.client.api.MessageId = {
    MessageIdImpl.fromByteArray(messageId.bytes)
  }

  implicit def apply(messageId: org.apache.pulsar.client.api.MessageId): MessageId = MessageId(messageId.toByteArray)
}

case class ProducerName(name: String)

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

trait Producer extends Closeable {
  def topic: Topic
  def name: ProducerName
  def send(msg: Array[Byte]): MessageId
  def sendAsync(msg: Array[Byte]): Future[MessageId]
  def send(msg: SMessage): MessageId
  def sendAsync(msg: SMessage): Future[MessageId]
  def send[T: MessageWriter](t: T): MessageId
  def sendAsync[T: MessageWriter](t: T): Future[MessageId]
  def lastSequenceId: Long
  def stats: ProducerStats
  override def close(): Unit
  def closeAsync: Future[Unit]
}

