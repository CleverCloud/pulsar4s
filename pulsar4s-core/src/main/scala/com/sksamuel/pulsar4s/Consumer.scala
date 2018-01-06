package com.sksamuel.pulsar4s

import java.io.Closeable

import org.apache.pulsar.client.api.{Message, Consumer => JConsumer}
import org.apache.pulsar.client.impl.ConsumerStats

import scala.compat.java8.FutureConverters
import scala.concurrent.Future
import scala.concurrent.duration.Duration

class DefaultConsumer(consumer: JConsumer, override val topic: Topic) extends Consumer2 {

  override def subscription: String = consumer.getSubscription

  override def unsubscribe(): Unit = consumer.unsubscribe()
  override def unsubscribeAsync: Future[Unit] = FutureConverters.toScala(consumer.unsubscribeAsync()).map(_ => ())

  override def receive: SMessage = {
    val msg = consumer.receive()
    SMessage.fromJava(msg)
  }

  override def receiveAsync: Future[SMessage] = {
    val f = FutureConverters.toScala(consumer.receiveAsync())
    f.map { msg => SMessage.fromJava(msg) }
  }

  override def receive(duration: Duration): Message = ???

  override def acknowledge(message: Message): Unit = ???
  override def acknowledge(messageId: MessageId): Unit = ???

  override def acknowledgeCumulative(message: Message): Unit = ???
  override def acknowledgeCumulative(messageId: MessageId): Unit = ???

  override def acknowledgeAsync(message: Message): Future[Unit] = ???
  override def acknowledgeAsync(messageId: MessageId): Future[Unit] = ???

  override def acknowledgeCumulativeAsync(message: Message): Future[Unit] = ???
  override def acknowledgeCumulativeAsync(messageId: MessageId): Future[Unit] = ???
  override def stats: ConsumerStats = ???
  override def hasReachedEndOfTopic: Boolean = ???
  override def redeliverUnacknowledgedMessages(): Unit = ???
  override def seek(messageId: MessageId): Unit = ???
  override def seekAsync(messageId: MessageId): Future[Unit] = ???

  override def close(): Unit = consumer.close()
  override def closeAsync: Future[Unit] = FutureConverters.toScala(consumer.closeAsync()).map(_ => ())
}

trait Consumer2 extends Closeable {
  def topic: Topic
  def subscription: String
  def unsubscribe(): Unit
  def unsubscribeAsync: Future[Unit]

  def receive: SMessage
  def receiveAsync: Future[SMessage]

  def receive(duration: Duration): Message
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
