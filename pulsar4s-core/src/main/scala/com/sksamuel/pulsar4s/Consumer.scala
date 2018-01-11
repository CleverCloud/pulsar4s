package com.sksamuel.pulsar4s

import java.util.concurrent.{CompletableFuture, TimeUnit}

import org.apache.pulsar.client.api.{Consumer => JConsumer}
import org.apache.pulsar.client.impl.ConsumerStats

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

class Consumer(consumer: JConsumer, val topic: Topic, val subscription: Subscription)
              (implicit context: ExecutionContext) {

  import Message._

  implicit def completableToFuture[T](f: CompletableFuture[T]): Future[T] = FutureConverters.toScala(f)
  implicit def voidCompletableToFuture(f: CompletableFuture[Void]): Future[Unit] = f.map(_ => ())

  def unsubscribe(): Unit = consumer.unsubscribe()
  def unsubscribeAsync: Future[Unit] = consumer.unsubscribeAsync()

  def receive: Message = {
    val msg = consumer.receive()
    Message.fromJava(msg)
  }

  def receive(duration: FiniteDuration): Message = {
    val msg = consumer.receive(duration.toNanos.toInt, TimeUnit.NANOSECONDS)
    Message.fromJava(msg)
  }

  def tryReceive: Try[Message] = Try(receive)
  def tryReceive(duration: FiniteDuration): Try[Message] = Try(receive(duration))

  def receiveAsync: Future[Message] = {
    val f = consumer.receiveAsync()
    f.map(Message.fromJava)
  }

  def receiveT[T](implicit reader: MessageReader[T]): T = {
    reader.read(receive) match {
      case Failure(e) => throw e
      case Success(t) => t
    }
  }

  def tryReceiveT[T](implicit reader: MessageReader[T]): Try[T] = tryReceive.flatMap(reader.read)

  def receiveAsyncT[T](implicit reader: MessageReader[T]): Future[T] = {
    receiveAsync.map(reader.read).map {
      case Success(t) => t
      case Failure(t) => throw t
    }
  }

  def tryReceiveT[T](duration: FiniteDuration)(implicit reader: MessageReader[T]): Try[T] =
    tryReceive(duration).flatMap(reader.read)

  def acknowledge(message: Message): Unit = {
    consumer.acknowledge(message)
  }

  def acknowledge(messageId: MessageId): Unit = {
    consumer.acknowledge(messageId)
  }

  def acknowledgeCumulative(message: Message): Unit = {
    consumer.acknowledgeCumulative(message)
  }

  def acknowledgeCumulative(messageId: MessageId): Unit = {
    consumer.acknowledgeCumulative(messageId)
  }

  def acknowledgeAsync(message: Message): Future[Unit] = {
    consumer.acknowledgeAsync(message)
  }

  def acknowledgeAsync(messageId: MessageId): Future[Unit] = {
    consumer.acknowledgeAsync(messageId)
  }

  def acknowledgeCumulativeAsync(message: Message): Future[Unit] = {
    consumer.acknowledgeCumulativeAsync(message)
  }

  def acknowledgeCumulativeAsync(messageId: MessageId): Future[Unit] = {
    consumer.acknowledgeCumulativeAsync(messageId)
  }

  def stats: ConsumerStats = consumer.getStats

  def hasReachedEndOfTopic: Boolean = consumer.hasReachedEndOfTopic

  def redeliverUnacknowledgedMessages(): Unit = consumer.redeliverUnacknowledgedMessages()

  def seek(messageId: MessageId): Unit = consumer.seek(messageId)
  def seekEarliest(): Unit = seek(MessageId.earliest)
  def seekLatest(): Unit = seek(MessageId.latest)

  def seekAsync(messageId: MessageId): Future[Unit] = {
    consumer.seekAsync(messageId)
  }

  def close(): Unit = consumer.close()
  def closeAsync: Future[Unit] = consumer.closeAsync()
}
