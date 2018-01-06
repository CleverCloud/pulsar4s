package com.sksamuel.pulsar4s

import java.io.Closeable

import org.apache.pulsar.client.api.{Message => JMessage}
import org.apache.pulsar.client.impl.ProducerStats

import scala.concurrent.Future
import scala.language.implicitConversions

case class ProducerName(name: String)

trait Producer extends Closeable {
  def topic: Topic
  def name: ProducerName
  def send(msg: Array[Byte]): MessageId
  def sendAsync(msg: Array[Byte]): Future[MessageId]
  def send(msg: String): MessageId
  def sendAsync(msg: String): Future[MessageId]
  def send(msg: Message): MessageId
  def sendAsync(msg: Message): Future[MessageId]
  def send[T: MessageWriter](t: T): MessageId
  def sendAsync[T: MessageWriter](t: T): Future[MessageId]
  def lastSequenceId: Long
  def stats: ProducerStats
  override def close(): Unit
  def closeAsync: Future[Unit]
}

