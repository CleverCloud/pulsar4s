package com.sksamuel.pulsar4s

import java.util.concurrent.{CompletableFuture, TimeUnit}

import org.apache.pulsar.client.api.{Message, Consumer => JConsumer}
import org.apache.pulsar.client.impl.{ConsumerStats, MessageIdImpl}

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

class DefaultConsumer(consumer: JConsumer, override val topic: Topic, override val subscription: Subscription)
                     (implicit context: ExecutionContext) extends Consumer2 {

  implicit def completableToFuture[T](f: CompletableFuture[T]): Future[T] = FutureConverters.toScala(f)
  implicit def voidCompletableToFuture(f: CompletableFuture[Void]): Future[Unit] = f.map(_ => ())

  override def unsubscribe(): Unit = consumer.unsubscribe()
  override def unsubscribeAsync: Future[Unit] = consumer.unsubscribeAsync()

  override def receive: SMessage = {
    val msg = consumer.receive()
    SMessage.fromJava(msg)
  }

  override def receiveAsync: Future[SMessage] = {
    val f = consumer.receiveAsync()
    f.map { msg => SMessage.fromJava(msg) }
  }

  override def receive(duration: Duration): SMessage = {
    val msg = consumer.receive(duration.toNanos.toInt, TimeUnit.NANOSECONDS)
    SMessage.fromJava(msg)
  }

  override def acknowledge(message: Message): Unit = {
    consumer.acknowledge(message)
  }

  override def acknowledge(messageId: MessageId): Unit = {
    consumer.acknowledge(messageId)
  }

  override def acknowledgeCumulative(message: Message): Unit = {
    consumer.acknowledgeCumulative(message)
  }

  override def acknowledgeCumulative(messageId: MessageId): Unit = {
    consumer.acknowledgeCumulative(messageId)
  }

  override def acknowledgeAsync(message: Message): Future[Unit] = {
    consumer.acknowledgeAsync(message)
  }

  override def acknowledgeAsync(messageId: MessageId): Future[Unit] = {
    consumer.acknowledgeAsync(messageId)
  }

  override def acknowledgeCumulativeAsync(message: Message): Future[Unit] = {
    consumer.acknowledgeCumulativeAsync(message)
  }

  override def acknowledgeCumulativeAsync(messageId: MessageId): Future[Unit] = {
    consumer.acknowledgeCumulativeAsync(messageId)
  }

  override def stats: ConsumerStats = consumer.getStats

  override def hasReachedEndOfTopic: Boolean = consumer.hasReachedEndOfTopic

  override def redeliverUnacknowledgedMessages(): Unit = consumer.redeliverUnacknowledgedMessages()

  override def seek(messageId: MessageId): Unit = consumer.seek(messageId)

  override def seekAsync(messageId: MessageId): Future[Unit] = {
    consumer.seekAsync(MessageIdImpl.fromByteArray(messageId.bytes))
  }

  override def close(): Unit = consumer.close()
  override def closeAsync: Future[Unit] = consumer.closeAsync()
}
