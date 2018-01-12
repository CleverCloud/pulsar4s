package com.sksamuel.pulsar4s

import java.util.concurrent.TimeUnit

import com.sksamuel.exts.Logging
import org.apache.pulsar.client.api.{Consumer => JConsumer}
import org.apache.pulsar.client.impl.ConsumerStats

import scala.concurrent.duration.FiniteDuration
import scala.language.{higherKinds, implicitConversions}
import scala.util.{Failure, Success, Try}

class Consumer(consumer: JConsumer, val topic: Topic, val subscription: Subscription) extends Logging{

  import Message._

  def unsubscribe(): Unit = consumer.unsubscribe()
  def unsubscribeAsync[F[_] : AsyncHandler]: F[Unit] = implicitly[AsyncHandler[F]].unsubscribeAsync(consumer)

  def receive: Message = {
    logger.trace("About to block until a message is received..")
    val msg = consumer.receive()
    Message.fromJava(msg)
  }

  def receive(duration: FiniteDuration): Message = {
    logger.trace(s"About to block for duration $duration or until a message is received..")
    val msg = consumer.receive(duration.toNanos.toInt, TimeUnit.NANOSECONDS)
    Message.fromJava(msg)
  }

  def tryReceive: Try[Message] = Try(receive)
  def tryReceive(duration: FiniteDuration): Try[Message] = Try(receive(duration))

  def receiveT[T: MessageReader]: T = {
    implicitly[MessageReader[T]].read(receive) match {
      case Failure(e) => throw e
      case Success(t) => t
    }
  }

  def tryReceiveT[T: MessageReader]: Try[T] = tryReceive.flatMap(implicitly[MessageReader[T]].read)
  def tryReceiveT[T](duration: FiniteDuration)(implicit reader: MessageReader[T]): Try[T] =
    tryReceive(duration).flatMap(reader.read)

  def receiveAsync[F[_] : AsyncHandler]: F[Message] = implicitly[AsyncHandler[F]].receive(consumer)
  def receiveAsyncT[T: MessageReader, F[_] : AsyncHandler]: F[T] =
    implicitly[AsyncHandler[F]].transform(receiveAsync)(implicitly[MessageReader[T]].read)

  def acknowledge(message: Message): Unit = consumer.acknowledge(message)
  def acknowledge(messageId: MessageId): Unit = consumer.acknowledge(messageId)

  def acknowledgeCumulative(message: Message): Unit = consumer.acknowledgeCumulative(message)
  def acknowledgeCumulative(messageId: MessageId): Unit = consumer.acknowledgeCumulative(messageId)

  def acknowledgeAsync[F[_] : AsyncHandler](message: Message): F[Unit] =
    implicitly[AsyncHandler[F]].acknowledgeAsync(consumer, message)

  def acknowledgeAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit] =
    implicitly[AsyncHandler[F]].acknowledgeAsync(consumer, messageId)

  def acknowledgeCumulativeAsync[F[_] : AsyncHandler](message: Message): F[Unit] =
    implicitly[AsyncHandler[F]].acknowledgeCumulativeAsync(consumer, message)

  def acknowledgeCumulativeAsync[F[_] : AsyncHandler](messageId: MessageId): F[Unit] =
    implicitly[AsyncHandler[F]].acknowledgeCumulativeAsync(consumer, messageId)

  def stats: ConsumerStats = consumer.getStats

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
