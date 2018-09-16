package com.sksamuel.pulsar4s

import java.io.Closeable
import java.util.concurrent.TimeUnit

import com.sksamuel.exts.Logging
import org.apache.pulsar.client.api.{ConsumerStats, Consumer => JConsumer}

import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, implicitConversions}
import scala.util.Try

trait Consumer[T] extends Closeable {

  /**
    * Receives a single message.
    * This calls blocks until a message is available.
    */
  def receive: Message[T]

  /**
    * Receive a single message waiting up to the given duration
    * if necessary. If no message is received within the duration
    * then None is returned.
    */
  def receive(duration: FiniteDuration): Option[Message[T]]

  def receiveAsync[F[_] : AsyncHandler]: F[Message[T]]

  /**
    * Receives a single message as a Success[T], or if an exception
    * is thrown by the client, returns a Failure.
    * This calls blocks until a message is available.
    */
  def tryReceive: Try[Message[T]] = Try(receive)

  /**
    * Receive a single message waiting up to the given duration
    * if necessary. If no message is received within the duration
    * then None is returned.
    *
    * If the method throws an exception then a Failure is returned.
    */
  def tryReceive(duration: FiniteDuration): Try[Option[Message[T]]] = Try(receive(duration))

  def stats: ConsumerStats
  def subscription: Subscription
  def topic: Topic

  /**
    * Return true if the topic was terminated and this consumer has already consumed all the messages in the topic.
    *
    * Please note that this does not simply mean that the consumer is caught up with the last message published by
    * producers, rather the topic needs to be explicitly "terminated".
    */
  def hasReachedEndOfTopic: Boolean

  def redeliverUnacknowledgedMessages(): Unit

  def seek(messageId: MessageId): Unit
  def seekEarliest(): Unit = seek(MessageId.earliest)
  def seekLatest(): Unit = seek(MessageId.latest)
  def seekAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit]

  def close(): Unit
  def closeAsync[F[_] : AsyncHandler]: F[Unit]

  def acknowledge(message: Message[T]): Unit = acknowledge(message.messageId.get)
  def acknowledge(messageId: MessageId): Unit

  def acknowledgeCumulative(message: Message[T]): Unit
  def acknowledgeCumulative(messageId: MessageId): Unit

  def acknowledgeAsync[F[_] : AsyncHandler](message: Message[T]): F[Unit] =
    acknowledgeAsync(message.messageId.get)

  def acknowledgeAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit]

  def acknowledgeCumulativeAsync[F[_] : AsyncHandler](message: Message[T]): F[Unit] =
    acknowledgeCumulativeAsync(message.messageId.get)

  def acknowledgeCumulativeAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit]

  def unsubscribe(): Unit
  def unsubscribeAsync[F[_] : AsyncHandler]: F[Unit]
}

class DefaultConsumer[T](consumer: JConsumer[T]) extends Consumer[T] with Logging {

  override def receive: Message[T] = {
    logger.trace("About to block until a message is received..")
    val msg = consumer.receive()
    Message.fromJava(msg)
  }

  override def receive(duration: FiniteDuration): Option[Message[T]] = {
    logger.trace(s"About to block for duration $duration or until a message is received..")
    val msg = consumer.receive(duration.toMillis.toInt, TimeUnit.MILLISECONDS)
    Option(msg).map(Message.fromJava)
  }

  def receiveAsync[F[_] : AsyncHandler]: F[Message[T]] = implicitly[AsyncHandler[F]].receive(consumer)

  def acknowledge(messageId: MessageId): Unit = consumer.acknowledge(messageId)

  def acknowledgeCumulative(message: Message[T]): Unit = consumer.acknowledgeCumulative(message.messageId.get)
  def acknowledgeCumulative(messageId: MessageId): Unit = consumer.acknowledgeCumulative(messageId)

  def acknowledgeAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit] =
    implicitly[AsyncHandler[F]].acknowledgeAsync(consumer, messageId)

  def acknowledgeCumulativeAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit] =
    implicitly[AsyncHandler[F]].acknowledgeCumulativeAsync(consumer, messageId)

  def stats: ConsumerStats = consumer.getStats
  def subscription = Subscription(consumer.getSubscription)
  def topic = Topic(consumer.getTopic)

  def hasReachedEndOfTopic: Boolean = consumer.hasReachedEndOfTopic

  def redeliverUnacknowledgedMessages(): Unit = consumer.redeliverUnacknowledgedMessages()

  def seek(messageId: MessageId): Unit = consumer.seek(messageId)

  def seekAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit] =
    implicitly[AsyncHandler[F]].seekAsync(consumer, messageId)

  def close(): Unit = consumer.close()
  def closeAsync[F[_] : AsyncHandler]: F[Unit] = implicitly[AsyncHandler[F]].close(consumer)

  override def unsubscribe(): Unit = consumer.unsubscribe()
  override def unsubscribeAsync[F[_] : AsyncHandler]: F[Unit] = implicitly[AsyncHandler[F]].unsubscribeAsync(consumer)
}
