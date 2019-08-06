package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.TypedMessageBuilder

import scala.concurrent.{ExecutionContext, Future}
import scala.language.{higherKinds, implicitConversions}
import scala.util.Try

trait AsyncHandler[F[_]] {

  def transform[A, B](f: F[A])(fn: A => Try[B]): F[B]
  def failed(e: Throwable): F[Nothing]

  def send[T](t: T, producer: api.Producer[T]): F[MessageId]
  def send[T](builder: TypedMessageBuilder[T]): F[MessageId]
  def receive[T](consumer: api.Consumer[T]): F[ConsumerMessage[T]]

  def close(producer: api.Producer[_]): F[Unit]
  def close(consumer: api.Consumer[_]): F[Unit]
  def close(reader: api.Reader[_]): F[Unit]

  def flush(producer: api.Producer[_]): F[Unit]

  def seekAsync(consumer: api.Consumer[_], messageId: MessageId): F[Unit]
  def nextAsync[T](reader: api.Reader[T]): F[ConsumerMessage[T]]

  def unsubscribeAsync(consumer: api.Consumer[_]): F[Unit]

  def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit]
  def negativeAcknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit]
  def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit]
}

object AsyncHandler {
  def apply[F[_] : AsyncHandler]: AsyncHandler[F] = implicitly[AsyncHandler[F]]
  implicit def handler(implicit ec: ExecutionContext): AsyncHandler[Future] = new FutureAsyncHandler
}
