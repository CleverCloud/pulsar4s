package com.sksamuel.pulsar4s

import java.util.concurrent.CompletableFuture

import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.TypedMessageBuilder

import scala.compat.java8.FutureConverters
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class FutureAsyncHandler(implicit ec: ExecutionContext) extends AsyncHandler[Future] {

  implicit class VoidCompletableFutureOps(val completableFuture: CompletableFuture[Void]) {
    def toScala: Future[Unit] = new CompletionStageOps(completableFuture).toScala.map(_ => ())
  }

  override def failed(e: Throwable): Future[Nothing] = Future.failed(e)

  override def send[T](t: T, producer: api.Producer[T]): Future[MessageId] = {
    val future = producer.sendAsync(t)
    FutureConverters.toScala(future).map(MessageId.fromJava)
  }

  override def receive[T](consumer: api.Consumer[T]): Future[ConsumerMessage[T]] = {
    val future = consumer.receiveAsync()
    FutureConverters.toScala(future).map(ConsumerMessage.fromJava)
  }

  override def unsubscribeAsync(consumer: api.Consumer[_]): Future[Unit] = consumer.unsubscribeAsync().toScala

  override def close(producer: api.Producer[_]): Future[Unit] = producer.closeAsync().toScala
  override def close(consumer: api.Consumer[_]): Future[Unit] = consumer.closeAsync().toScala

  override def seekAsync(consumer: api.Consumer[_], messageId: MessageId): Future[Unit] =
    consumer.seekAsync(messageId).toScala

  override def transform[A, B](f: Future[A])(fn: A => Try[B]): Future[B] = f.flatMap { a =>
    fn(a) match {
      case Success(b) => Future.successful(b)
      case Failure(e) => Future.failed(e)
    }
  }

  override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Future[Unit] =
    consumer.acknowledgeAsync(messageId).toScala

  override def negativeAcknowledgeAsync[T](consumer: JConsumer[T], messageId: MessageId): Future[Unit] =
    Future.successful(consumer.negativeAcknowledge(messageId))

  override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Future[Unit] =
    consumer.acknowledgeCumulativeAsync(messageId).toScala

  override def close(reader: api.Reader[_]): Future[Unit] = reader.closeAsync().toScala
  override def flush(producer: api.Producer[_]): Future[Unit] = producer.flushAsync().toScala

  override def nextAsync[T](reader: api.Reader[T]): Future[ConsumerMessage[T]] =
    reader.readNextAsync().toScala.map(ConsumerMessage.fromJava)

  override def send[T](builder: TypedMessageBuilder[T]): Future[MessageId] =
    builder.sendAsync().toScala.map(MessageId.fromJava)
}
