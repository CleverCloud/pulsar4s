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
  override def send(msg: Message, producer: api.Producer): Future[MessageId] = {
    val future = producer.sendAsync(msg)
    FutureConverters.toScala(future).map { id => MessageId(id) }
  }

  override def receive(consumer: api.Consumer): Future[Message] = {
    val future = consumer.receiveAsync()
    FutureConverters.toScala(future).map { msg => Message.fromJava(msg) }
  }

  def unsubscribeAsync(consumer: api.Consumer): Future[Unit] = consumer.unsubscribeAsync()

  override def close(producer: api.Producer): Future[Unit] = producer.closeAsync()
  override def close(consumer: api.Consumer): Future[Unit] = consumer.closeAsync()

  override def seekAsync(consumer: api.Consumer, messageId: MessageId): Future[Unit] = consumer.seekAsync(messageId)

  override def transform[A, B](f: Future[A])(fn: A => Try[B]): Future[B] = f.flatMap { a =>
    fn(a) match {
      case Success(b) => Future.successful(b)
      case Failure(e) => Future.failed(e)
    }
  }

  override def acknowledgeAsync(consumer: api.Consumer, message: Message): Future[Unit] =
    consumer.acknowledgeAsync(message)

  override def acknowledgeAsync(consumer: api.Consumer, messageId: MessageId): Future[Unit] =
    consumer.acknowledgeAsync(messageId)

  override def acknowledgeCumulativeAsync(consumer: api.Consumer, message: Message): Future[Unit] =
    consumer.acknowledgeCumulativeAsync(message)

  override def acknowledgeCumulativeAsync(consumer: api.Consumer, messageId: MessageId): Future[Unit] =
    consumer.acknowledgeCumulativeAsync(messageId)

  override def close(reader: api.Reader): Future[Unit] = reader.closeAsync()

  override def nextAsync(reader: api.Reader): Future[Message] = reader.readNextAsync().map(Message.fromJava)
}