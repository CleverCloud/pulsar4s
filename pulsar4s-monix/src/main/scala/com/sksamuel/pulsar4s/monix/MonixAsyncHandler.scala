package com.sksamuel.pulsar4s.monix

import java.util.concurrent.CompletableFuture

import com.sksamuel.pulsar4s.{AsyncHandler, Message, MessageId}
import monix.eval.Task
import org.apache.pulsar.client.api

import scala.compat.java8.FutureConverters
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

class MonixAsyncHandler extends AsyncHandler[Task] {

  implicit def completableTToFuture[T](f: CompletableFuture[T]): Future[T] =
    FutureConverters.toScala(f)

  implicit def completableVoidToTask(f: CompletableFuture[Void]): Task[Unit] =
    Task.deferFuture(FutureConverters.toScala(f)).map(_ => ())

  override def failed(e: Throwable): Task[Nothing] = Task.raiseError(e)

  override def send(msg: Message, producer: api.Producer): Task[MessageId] = {
    Task.deferFuture {
      val future = producer.sendAsync(msg)
      FutureConverters.toScala(future)
    }.map { id => MessageId(id) }
  }

  override def receive(consumer: api.Consumer): Task[Message] = {
    Task.deferFuture {
      val future = consumer.receiveAsync()
      FutureConverters.toScala(future)
    }.map { msg => Message.fromJava(msg) }
  }

  def unsubscribeAsync(consumer: api.Consumer): Task[Unit] = consumer.unsubscribeAsync()

  override def close(producer: api.Producer): Task[Unit] = producer.closeAsync()
  override def close(consumer: api.Consumer): Task[Unit] = consumer.closeAsync()

  override def seekAsync(consumer: api.Consumer, messageId: MessageId): Task[Unit] = consumer.seekAsync(messageId)

  override def transform[A, B](t: Task[A])(fn: A => Try[B]): Task[B] =
    t.flatMap { a =>
      fn(a) match {
        case Success(b) => Task.now(b)
        case Failure(e) => Task.raiseError(e)
      }
    }

  override def acknowledgeAsync(consumer: api.Consumer, message: Message): Task[Unit] =
    consumer.acknowledgeAsync(message)

  override def acknowledgeAsync(consumer: api.Consumer, messageId: MessageId): Task[Unit] =
    consumer.acknowledgeAsync(messageId)

  override def acknowledgeCumulativeAsync(consumer: api.Consumer, message: Message): Task[Unit] =
    consumer.acknowledgeCumulativeAsync(message)

  override def acknowledgeCumulativeAsync(consumer: api.Consumer, messageId: MessageId): Task[Unit] =
    consumer.acknowledgeCumulativeAsync(messageId)
}

object MonixAsyncHandler {
  implicit def handler: AsyncHandler[Task] = new MonixAsyncHandler
}
