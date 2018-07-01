package com.sksamuel.pulsar4s

import java.util.concurrent.TimeUnit

import com.sksamuel.exts.Logging
import org.apache.pulsar.client.api.{ConsumerStats, Consumer => JConsumer}

import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, implicitConversions}
import scala.util.Try

class Consumer[T](consumer: JConsumer[T]) extends Logging {

  def unsubscribe(): Unit = consumer.unsubscribe()
  def unsubscribeAsync[F[_] : AsyncHandler]: F[Unit] = implicitly[AsyncHandler[F]].unsubscribeAsync(consumer)

  def receive: Message[T] = {
    logger.trace("About to block until a message is received..")
    val msg = consumer.receive()
    Message.fromJava(msg)
  }

  def receive(duration: FiniteDuration): Message[T] = {
    logger.trace(s"About to block for duration $duration or until a message is received..")
    val msg = consumer.receive(duration.toNanos.toInt, TimeUnit.NANOSECONDS)
    Message.fromJava(msg)
  }

  def tryReceive: Try[Message[T]] = Try(receive)
  def tryReceive(duration: FiniteDuration): Try[Message[T]] = Try(receive(duration))

  def receiveAsync[F[_] : AsyncHandler]: F[Message[T]] = implicitly[AsyncHandler[F]].receive(consumer)

  def acknowledge(message: Message[T]): Unit = acknowledge(message.messageId.get)
  def acknowledge(messageId: MessageId): Unit = consumer.acknowledge(messageId)

  def acknowledgeCumulative(message: Message[T]): Unit = consumer.acknowledgeCumulative(message.messageId.get)
  def acknowledgeCumulative(messageId: MessageId): Unit = consumer.acknowledgeCumulative(messageId)

  def acknowledgeAsync[F[_] : AsyncHandler](message: Message[T]): F[Unit] =
    acknowledgeAsync(message.messageId.get)

  def acknowledgeAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit] =
    implicitly[AsyncHandler[F]].acknowledgeAsync(consumer, messageId)

  def acknowledgeCumulativeAsync[F[_] : AsyncHandler](message: Message[T]): F[Unit] =
    acknowledgeCumulativeAsync(message.messageId.get)

  def acknowledgeCumulativeAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit] =
    implicitly[AsyncHandler[F]].acknowledgeCumulativeAsync(consumer, messageId)

  def stats: ConsumerStats = consumer.getStats
  def subscription = Subscription(consumer.getSubscription)
  def topic = Topic(consumer.getTopic)

  def hasReachedEndOfTopic: Boolean = consumer.hasReachedEndOfTopic

  def redeliverUnacknowledgedMessages(): Unit = consumer.redeliverUnacknowledgedMessages()

  def seek(messageId: MessageId): Unit = consumer.seek(messageId)
  def seekEarliest(): Unit = seek(MessageId.earliest)
  def seekLatest(): Unit = seek(MessageId.latest)

  def seekAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit] =
    implicitly[AsyncHandler[F]].seekAsync(consumer, messageId)

  def close(): Unit = consumer.close()
  def closeAsync[F[_] : AsyncHandler]: F[Unit] = implicitly[AsyncHandler[F]].close(consumer)
}
