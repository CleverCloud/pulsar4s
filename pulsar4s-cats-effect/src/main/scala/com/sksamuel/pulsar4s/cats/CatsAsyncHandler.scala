package com.sksamuel.pulsar4s.cats

import java.util.concurrent.CompletableFuture
import java.util.function.BiConsumer

import cats.implicits._
import cats.effect._
import com.sksamuel.pulsar4s.{AsyncHandler, ConsumerMessage, MessageId}
import org.apache.pulsar.client.api
import org.apache.pulsar.client.api.{Reader, TypedMessageBuilder}

import scala.language.higherKinds
import scala.util.{Failure, Success, Try}


object CatsAsyncHandler {
  object CompletableFutureConverters {
    implicit class CompletableOps[T](f: => CompletableFuture[T]) {
      def toF[F[_]: Async]: F[T] =
        Async[F].async[T] { k =>
          f.whenCompleteAsync(new BiConsumer[T, Throwable] {
            override def accept(t: T, e: Throwable): Unit = {
              if (e != null) k.apply(Left(e))
              else k.apply(Right(t))
            }
          })
        }

    }
  }

  implicit def asyncHandlerForCatsEffectAsync[F[_]: Async]: AsyncHandler[F] = new AsyncHandler[F] {
    import CompletableFutureConverters._

    override def failed(e: Throwable): F[Nothing] = Async[F].raiseError(e)

    override def send[T](t: T, producer: api.Producer[T]): F[MessageId] = producer.sendAsync(t).toF[F].map(MessageId.fromJava)
    override def receive[T](consumer: api.Consumer[T]): F[ConsumerMessage[T]] = consumer.receiveAsync().toF[F].map(ConsumerMessage.fromJava)

    override def unsubscribeAsync(consumer: api.Consumer[_]): F[Unit] = consumer.unsubscribeAsync().toF[F].void

    override def close(producer: api.Producer[_]): F[Unit] = producer.closeAsync().toF[F].void
    override def close(consumer: api.Consumer[_]): F[Unit] = consumer.closeAsync().toF[F].void

    override def seekAsync(consumer: api.Consumer[_], messageId: MessageId): F[Unit] = consumer.seekAsync(messageId).toF[F].void

    override def transform[A, B](t: F[A])(fn: A => Try[B]): F[B] =
      t.flatMap { a =>
        fn(a) match {
          case Success(b) => Async[F].pure(b)
          case Failure(e) => Async[F].raiseError(e)
        }
      }

    override def acknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit] =
      consumer.acknowledgeAsync(messageId).toF[F].void

    override def acknowledgeCumulativeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit] =
      consumer.acknowledgeCumulativeAsync(messageId).toF[F].void

    override def negativeAcknowledgeAsync[T](consumer: api.Consumer[T], messageId: MessageId): F[Unit] =
      Async[F].delay { consumer.negativeAcknowledge(messageId) }

    override def close(reader: Reader[_]): F[Unit] = reader.closeAsync().toF[F].void
    override def flush(producer: api.Producer[_]): F[Unit] = producer.flushAsync().toF[F].void

    override def nextAsync[T](reader: Reader[T]): F[ConsumerMessage[T]] =
      reader.readNextAsync().toF[F].map(ConsumerMessage.fromJava)

    override def send[T](builder: TypedMessageBuilder[T]): F[MessageId] =
      builder.sendAsync().toF[F].map(MessageId.fromJava)
  }

  object io {
    implicit val handler: AsyncHandler[IO] = asyncHandlerForCatsEffectAsync[IO]
  }
}
