package com.sksamuel.pulsar4s

import java.io.Closeable

import org.apache.pulsar.client.api.{Message, Consumer => JConsumer}
import org.apache.pulsar.client.impl.ConsumerStats

import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait Consumer extends Closeable {

  def topic: Topic
  def subscription: Subscription

  def unsubscribe(): Unit
  def unsubscribeAsync: Future[Unit]

  def receive: SMessage
  def receiveAsync: Future[SMessage]
  def receive(duration: Duration): SMessage

  def acknowledge(message: Message): Unit
  def acknowledge(messageId: MessageId): Unit

  def acknowledgeCumulative(message: Message): Unit
  def acknowledgeCumulative(messageId: MessageId): Unit

  def acknowledgeAsync(message: Message): Future[Unit]
  def acknowledgeAsync(messageId: MessageId): Future[Unit]

  def acknowledgeCumulativeAsync(message: Message): Future[Unit]
  def acknowledgeCumulativeAsync(messageId: MessageId): Future[Unit]

  def stats: ConsumerStats

  def hasReachedEndOfTopic: Boolean
  def redeliverUnacknowledgedMessages(): Unit

  def seek(messageId: MessageId): Unit
  def seekAsync(messageId: MessageId): Future[Unit]

  def closeAsync: Future[Unit]
  override def close()
}
