package com.sksamuel.pulsar4s

import java.util.concurrent.CompletableFuture

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import org.apache.pulsar.client.api

import scala.util.{Failure, Success, Try}

class FutureAsyncHandler(implicit ec: ExecutionContext) extends AsyncHandler[Future] {

  implicit def completableToFuture[U](f: CompletableFuture[U]): Future[U] = FutureConverters.toScala(f)
  implicit def voidCompletableToFuture(f: CompletableFuture[Void]): Future[Unit] = f.map(_ => ())

  override def failed(e: Throwable): Future[Nothing] = Future.failed(e)

  override def send[T](t: T, producer: api.Producer[T]): Future[MessageId] = {
    val future = producer.sendAsync(t)
    FutureConverters.toScala(future).map { id => MessageId(id) }
  }

  override def receive[T](consumer: api.Consumer[T]): Future[Message[T]] = {
    val future = consumer.receiveAsync()
    FutureConverters.toScala(future).map { msg => Message.fromJava(msg) }
  }

  def unsubscribeAsync(consumer: api.Consumer[_]): Future[Unit] = consumer.unsubscribeAsync()

  override def close(producer: api.Producer[_]): Future[Unit] = producer.closeAsync()
  override def close(consumer: api.Consumer[_]): Future[Unit] = consumer.closeAsync()

  override def seekAsync(consumer: api.Consumer[_], messageId: MessageId): Future[Unit] = consumer.seekAsync(messageId)

  override def transform[A, B](f: Future[A])(fn: A => Try[B]): Future[B] = f.flatMap { a =>
    fn(a) match {
      case Success(b) => Future.successful(b)
      case Failure(e) => Future.failed(e)
    }
  }

  override def acknowledgeAsync[T](consumer: api.Consumer[T], message: Message[T]): Future[Unit] = {
    consumer.acknowledgeAsync(message)
  }

  override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Future[Unit] = {
    consumer.acknowledgeAsync(messageId)
  }

  override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T],
                                             message: Message[T]): Future[Unit] = {
    consumer.acknowledgeCumulativeAsync(message)
  }

  override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T],
                                             messageId: MessageId): Future[Unit] =
    consumer.acknowledgeCumulativeAsync(messageId)

  override def close(reader: api.Reader[_]): Future[Unit] = reader.closeAsync()

  override def nextAsync[T](reader: api.Reader[T]): Future[Message[T]] = reader.readNextAsync().map(Message.fromJava)
}