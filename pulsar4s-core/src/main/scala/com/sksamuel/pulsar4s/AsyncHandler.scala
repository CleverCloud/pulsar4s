package com.sksamuel.pulsar4s

import java.util.concurrent.CompletableFuture

import org.apache.pulsar.client.api

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}
import scala.language.{higherKinds, implicitConversions}
import scala.util.{Failure, Success, Try}

trait AsyncHandler[F[_]] {

  def transform[A, B](f: F[A])(fn: A => Try[B]): F[B]
  def failed(e: Throwable): F[Nothing]

  def send(msg: Message, producer: api.Producer): F[MessageId]
  def receive(consumer: api.Consumer): F[Message]

  def close(producer: api.Producer): F[Unit]
  def close(consumer: api.Consumer): F[Unit]

  def seekAsync(consumer: api.Consumer, messageId: MessageId): F[Unit]

  def unsubscribeAsync(consumer: api.Consumer): F[Unit]


  def acknowledgeAsync(consumer: api.Consumer, message: Message): F[Unit]
  def acknowledgeAsync(consumer: api.Consumer, messageId: MessageId): F[Unit]

  def acknowledgeCumulativeAsync(consumer: api.Consumer, message: Message): F[Unit]
  def acknowledgeCumulativeAsync(consumer: api.Consumer, messageId: MessageId): F[Unit]
}

object AsyncHandler {

  def apply[F[_] : AsyncHandler]: AsyncHandler[F] = implicitly[AsyncHandler[F]]

  implicit def ScalaFutureHandler(implicit ec: ExecutionContext): AsyncHandler[Future] = new AsyncHandler[Future] {

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
  }
}
