package com.sksamuel.pulsar4s.cats

import java.util.concurrent.CompletableFuture
import java.util.function.BiConsumer

import cats.effect.IO
import com.sksamuel.pulsar4s.{AsyncHandler, Message, MessageId}
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.Reader

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

class CatsAsyncHandler extends AsyncHandler[IO] {

  implicit def completableVoidToIO(f: CompletableFuture[Void]): IO[Unit] = completableToIO(f).map(_ => ())
  implicit def completableToIO[T](f: CompletableFuture[T]): IO[T] =
    IO.async[T] { k =>
      f.whenCompleteAsync(new BiConsumer[T, Throwable] {
        override def accept(t: T, e: Throwable): Unit = {
          if (e != null) k.apply(Left(e))
          else k.apply(Right(t))
        }
      })
    }

  override def failed(e: Throwable): IO[Nothing] = IO.raiseError(e)

  override def send(msg: Message, producer: api.Producer): IO[MessageId] = producer.sendAsync(msg).map(MessageId.apply)
  override def receive(consumer: api.Consumer): IO[Message] = consumer.receiveAsync().map(Message.fromJava)

  def unsubscribeAsync(consumer: api.Consumer): IO[Unit] = consumer.unsubscribeAsync()

  override def close(producer: api.Producer): IO[Unit] = producer.closeAsync()
  override def close(consumer: api.Consumer): IO[Unit] = consumer.closeAsync()

  override def seekAsync(consumer: api.Consumer, messageId: MessageId): IO[Unit] = consumer.seekAsync(messageId)

  override def transform[A, B](t: IO[A])(fn: A => Try[B]): IO[B] =
    t.flatMap { a =>
      fn(a) match {
        case Success(b) => IO.pure(b)
        case Failure(e) => IO.raiseError(e)
      }
    }

  override def acknowledgeAsync(consumer: api.Consumer, message: Message): IO[Unit] =
    consumer.acknowledgeAsync(message)

  override def acknowledgeAsync(consumer: api.Consumer, messageId: MessageId): IO[Unit] =
    consumer.acknowledgeAsync(messageId)

  override def acknowledgeCumulativeAsync(consumer: api.Consumer, message: Message): IO[Unit] =
    consumer.acknowledgeCumulativeAsync(message)

  override def acknowledgeCumulativeAsync(consumer: api.Consumer, messageId: MessageId): IO[Unit] =
    consumer.acknowledgeCumulativeAsync(messageId)

  override def close(reader: Reader): IO[Unit] = reader.closeAsync()

  override def nextAsync(reader: Reader): IO[Message] = reader.readNextAsync().map(Message.fromJava)
}

object CatsAsyncHandler {
  implicit def handler: AsyncHandler[IO] = new CatsAsyncHandler
}
