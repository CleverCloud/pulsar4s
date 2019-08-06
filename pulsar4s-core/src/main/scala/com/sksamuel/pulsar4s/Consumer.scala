package com.sksamuel.pulsar4s

import java.io.Closeable
import java.util.concurrent.TimeUnit

import com.sksamuel.exts.Logging
import org.apache.pulsar.client.api.ConsumerStats

import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, implicitConversions}
import scala.util.Try

trait Consumer[T] extends Closeable {

  /**
    * Receives a single message.
    * This calls blocks until a message is available.
    */
  def receive: Try[ConsumerMessage[T]]

  /**
    * Receive a single message waiting up to the given duration
    * if necessary. If no message is received within the duration
    * then None is returned.
    */
  def receive(duration: FiniteDuration): Try[Option[ConsumerMessage[T]]]

  def receiveAsync[F[_] : AsyncHandler]: F[ConsumerMessage[T]]

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

  def acknowledge(message: ConsumerMessage[T]): Unit = acknowledge(message.messageId)
  def acknowledge(messageId: MessageId): Unit

  def negativeAcknowledge(message: ConsumerMessage[T]): Unit = negativeAcknowledge(message.messageId)
  def negativeAcknowledge(messageId: MessageId): Unit

  def acknowledgeCumulative(message: ConsumerMessage[T]): Unit
  def acknowledgeCumulative(messageId: MessageId): Unit

  final def acknowledgeAsync[F[_] : AsyncHandler](message: ConsumerMessage[T]): F[Unit] =
    acknowledgeAsync(message.messageId)

  def acknowledgeAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit]

  final def negativeAcknowledgeAsync[F[_] : AsyncHandler](message: ConsumerMessage[T]): F[Unit] =
    negativeAcknowledgeAsync(message.messageId)

  def negativeAcknowledgeAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit]

  final def acknowledgeCumulativeAsync[F[_] : AsyncHandler](message: ConsumerMessage[T]): F[Unit] =
    acknowledgeCumulativeAsync(message.messageId)

  def acknowledgeCumulativeAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit]

  def unsubscribe(): Unit
  def unsubscribeAsync[F[_] : AsyncHandler]: F[Unit]
}

class DefaultConsumer[T](consumer: JConsumer[T]) extends Consumer[T] with Logging {

  override def receive: Try[ConsumerMessage[T]] = Try {
    logger.trace("About to block until a message is received..")
    val msg = consumer.receive()
    ConsumerMessage.fromJava(msg)
  }

  override def receive(duration: FiniteDuration): Try[Option[ConsumerMessage[T]]] = Try {
    logger.trace(s"About to block for duration $duration or until a message is received..")
    val msg = consumer.receive(duration.toMillis.toInt, TimeUnit.MILLISECONDS)
    Option(msg).map(ConsumerMessage.fromJava)
  }

  override def receiveAsync[F[_] : AsyncHandler]: F[ConsumerMessage[T]] = implicitly[AsyncHandler[F]].receive(consumer)

  override def acknowledge(messageId: MessageId): Unit = consumer.acknowledge(messageId)
  override def acknowledgeCumulative(message: ConsumerMessage[T]): Unit = consumer.acknowledgeCumulative(message.messageId)
  override def acknowledgeCumulative(messageId: MessageId): Unit = consumer.acknowledgeCumulative(messageId)

  override def acknowledgeAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit] =
    implicitly[AsyncHandler[F]].acknowledgeAsync(consumer, messageId)
  override def acknowledgeCumulativeAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit] =
    implicitly[AsyncHandler[F]].acknowledgeCumulativeAsync(consumer, messageId)

  override def negativeAcknowledge(messageId: MessageId): Unit = consumer.negativeAcknowledge(messageId)
  override def negativeAcknowledgeAsync[F[_]: AsyncHandler](messageId: MessageId): F[Unit] =
    implicitly[AsyncHandler[F]].negativeAcknowledgeAsync(consumer, messageId)

  override def stats: ConsumerStats = consumer.getStats
  override def subscription = Subscription(consumer.getSubscription)
  override def topic = Topic(consumer.getTopic)

  override def hasReachedEndOfTopic: Boolean = consumer.hasReachedEndOfTopic

  override def redeliverUnacknowledgedMessages(): Unit = consumer.redeliverUnacknowledgedMessages()

  override def seek(messageId: MessageId): Unit = consumer.seek(messageId)

  override def seekAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit] =
    implicitly[AsyncHandler[F]].seekAsync(consumer, messageId)

  override def close(): Unit = {
    logger.info("Closing consumer")
    consumer.close()
  }

  override def closeAsync[F[_] : AsyncHandler]: F[Unit] = implicitly[AsyncHandler[F]].close(consumer)

  override def unsubscribe(): Unit = consumer.unsubscribe()
  override def unsubscribeAsync[F[_] : AsyncHandler]: F[Unit] = implicitly[AsyncHandler[F]].unsubscribeAsync(consumer)
}
