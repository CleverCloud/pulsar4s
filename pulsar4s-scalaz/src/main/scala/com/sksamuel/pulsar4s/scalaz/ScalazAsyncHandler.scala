package com.sksamuel.pulsar4s.scalaz

import java.util.concurrent.CompletableFuture
import java.util.function.BiConsumer

import com.sksamuel.pulsar4s.{AsyncHandler, ConsumerMessage, MessageId}
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.{Reader, TypedMessageBuilder}
import scalaz.concurrent.Task

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

class ScalazAsyncHandler extends AsyncHandler[Task] {

  implicit def completableVoidToTask(f: => CompletableFuture[Void]): Task[Unit] =
    completableToTask(f).map(_ => ())

  implicit def completableToTask[T](f: => CompletableFuture[T]): Task[T] = {
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

  override def send[T](t: T, producer: api.Producer[T]): Task[MessageId] =
    completableToTask(producer.sendAsync(t)).map(MessageId.fromJava)

  override def receive[T](consumer: api.Consumer[T]): Task[ConsumerMessage[T]] =
    completableToTask(consumer.receiveAsync).map(ConsumerMessage.fromJava)

  override def unsubscribeAsync(consumer: api.Consumer[_]): Task[Unit] =
    consumer.unsubscribeAsync()

  override def seekAsync(consumer: api.Consumer[_], messageId: MessageId): Task[Unit] =
    consumer.seekAsync(messageId)

  override def transform[A, B](f: Task[A])(fn: A => Try[B]): Task[B] = f.flatMap { a =>
    fn(a) match {
      case Success(b) => Task.now(b)
      case Failure(e) => Task.fail(e)
    }
  }

  override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Task[Unit] =
    consumer.acknowledgeAsync(messageId)

  override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId): Task[Unit] =
    consumer.acknowledgeCumulativeAsync(messageId)

  override def negativeAcknowledgeAsync[T](consumer: Consumer[T], messageId: MessageId): Task[Unit] =
    Task { consumer.negativeAcknowledge(messageId) }

  override def close(reader: Reader[_]): Task[Unit] = reader.closeAsync()
  override def close(producer: api.Producer[_]): Task[Unit] = producer.closeAsync()
  override def close(consumer: api.Consumer[_]): Task[Unit] = consumer.closeAsync()

  override def flush(producer: api.Producer[_]): Task[Unit] = producer.flushAsync()

  override def nextAsync[T](reader: Reader[T]): Task[ConsumerMessage[T]] =
    reader.readNextAsync().map(ConsumerMessage.fromJava)

  override def send[T](builder: TypedMessageBuilder[T]): Task[MessageId] =
    builder.sendAsync().map(MessageId.fromJava)
}

object ScalazAsyncHandler {
  implicit def handler: AsyncHandler[Task] = new ScalazAsyncHandler
}
