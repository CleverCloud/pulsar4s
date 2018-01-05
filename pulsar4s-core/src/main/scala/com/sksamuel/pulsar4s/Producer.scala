package com.sksamuel.pulsar4s

import java.io.Closeable

import org.apache.pulsar.client.impl.ProducerStats

import scala.concurrent.Future

trait SMessage {
  def data: Array[Byte]
  def key: Option[String]
  def properties: Map[String, String]
  def messageId: Option[MessageId]
  def publishTime: Long
  def eventTime: Long
}

case class MessageId(bytes: Array[Byte])
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
  def send(message: Array[Byte]): MessageId
  def sendAsync(message: Array[Byte]): Future[MessageId]
  def send(message: SMessage): MessageId
  def sendAsync(message: SMessage): Future[MessageId]
  def send[T: MessageWriter](t: T): MessageId
  def sendAsync[T: MessageWriter](t: T): Future[MessageId]
  def lastSequenceId: Long
  def stats: ProducerStats
  override def close(): Unit
  def closeAsync: Future[Unit]
}

