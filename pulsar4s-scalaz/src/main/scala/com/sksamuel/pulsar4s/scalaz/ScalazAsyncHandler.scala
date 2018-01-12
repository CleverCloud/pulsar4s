package com.sksamuel.pulsar4s.scalaz

import java.util.concurrent.CompletableFuture
import java.util.function.BiConsumer

import com.sksamuel.pulsar4s.{AsyncHandler, Message, MessageId}
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.Reader

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}
import scalaz.concurrent.Task

class ScalazAsyncHandler extends AsyncHandler[Task] {

  implicit def completableVoidToTask(f: CompletableFuture[Void]): Task[Unit] = completableToTask(f).map(_ => ())
  implicit def completableToTask[T](f: CompletableFuture[T]): Task[T] = {
    Task.async[T] { k =>
      f.whenCompleteAsync(new BiConsumer[T, Throwable] {
        override def accept(t: T, e: Throwable): Unit = {
          if (e != null)
            k.apply(scalaz.\/.left(e))
          else
            k.apply(scalaz.\/.right(t))
        }
      })
    }
  }

  override def failed(e: Throwable): Task[Nothing] = Task.fail(e)

  override def send(msg: Message, producer: api.Producer): Task[MessageId] =
    completableToTask(producer.sendAsync(msg)).map(MessageId.apply)

  override def receive(consumer: api.Consumer): Task[Message] =
    completableToTask(consumer.receiveAsync).map(Message.fromJava)

  def unsubscribeAsync(consumer: api.Consumer): Task[Unit] = consumer.unsubscribeAsync()

  override def close(producer: api.Producer): Task[Unit] = producer.closeAsync()
  override def close(consumer: api.Consumer): Task[Unit] = consumer.closeAsync()

  override def seekAsync(consumer: api.Consumer, messageId: MessageId): Task[Unit] = consumer.seekAsync(messageId)

  override def transform[A, B](f: Task[A])(fn: A => Try[B]): Task[B] = f.flatMap {
    a =>
      fn(a) match {
        case Success(b) => Task.now(b)
        case Failure(e) => Task.fail(e)
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

  override def close(reader: Reader): Task[Unit] = reader.closeAsync()

  override def nextAsync(reader: Reader): Task[Message] = reader.readNextAsync().map(Message.fromJava)
}

object ScalazAsyncHandler {
  implicit def handler: AsyncHandler[Task] = new ScalazAsyncHandler
}
