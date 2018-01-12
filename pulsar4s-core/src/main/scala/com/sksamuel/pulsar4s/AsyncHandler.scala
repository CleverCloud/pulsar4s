package com.sksamuel.pulsar4s

import org.apache.pulsar.client.api

import scala.concurrent.{ExecutionContext, Future}
import scala.language.{higherKinds, implicitConversions}
import scala.util.Try

trait AsyncHandler[F[_]] {

  def transform[A, B](f: F[A])(fn: A => Try[B]): F[B]
  def failed(e: Throwable): F[Nothing]

  def send(msg: Message, producer: api.Producer): F[MessageId]
  def receive(consumer: api.Consumer): F[Message]

  def close(producer: api.Producer): F[Unit]
  def close(consumer: api.Consumer): F[Unit]
  def close(reader: api.Reader): F[Unit]

  def seekAsync(consumer: api.Consumer, messageId: MessageId): F[Unit]
  def nextAsync(reader: api.Reader): F[Message]

  def unsubscribeAsync(consumer: api.Consumer): F[Unit]

  def acknowledgeAsync(consumer: api.Consumer, message: Message): F[Unit]
  def acknowledgeAsync(consumer: api.Consumer, messageId: MessageId): F[Unit]

  def acknowledgeCumulativeAsync(consumer: api.Consumer, message: Message): F[Unit]
  def acknowledgeCumulativeAsync(consumer: api.Consumer, messageId: MessageId): F[Unit]
}

object AsyncHandler {
  def apply[F[_] : AsyncHandler]: AsyncHandler[F] = implicitly[AsyncHandler[F]]
  implicit def handler(implicit ec: ExecutionContext): AsyncHandler[Future] = new FutureAsyncHandler
}
